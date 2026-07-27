"""
worker.py — Phase 3-10 production copier support.

Provides failure classification, retry scheduling, and reconciliation
for the MT5 worker layer. All logic here is broker-agnostic; MT5-specific
calls remain in router.py.
"""
import json
import logging
import secrets
from datetime import datetime, timedelta
from typing import Optional

import database as db

logger = logging.getLogger("worker")

# ── Failure classification ────────────────────────────────────────────────────

# MT5 TRADE_RETCODE_* values that indicate a transient condition worth retrying.
_TRANSIENT_CODES = frozenset({
    10004,  # REQUOTE
    10020,  # PRICE_CHANGED
    10024,  # TOO_MANY_REQUESTS
    10031,  # NO_CONNECTION
})

# Codes that indicate a permanent rejection — do not retry.
_PERMANENT_CODES = frozenset({
    10006,  # REJECT
    10013,  # INVALID
    10014,  # INVALID_VOLUME
    10015,  # INVALID_PRICE
    10016,  # INVALID_STOPS
    10017,  # TRADE_DISABLED
    10018,  # MARKET_CLOSED
    10019,  # NO_MONEY (insufficient funds)
    10026,  # SERVER_DISABLES_AT
    10027,  # CLIENT_DISABLES_AT
    10033,  # LIMIT_ORDERS
    10034,  # LIMIT_VOLUME
})


def classify_failure(error_code: Optional[str], error_message: Optional[str]) -> str:
    """
    Returns one of:
      'transient'  — retry with backoff (IPC glitch, network hiccup, requote)
      'terminal'   — worker must repair its MT5 login before retrying
      'permanent'  — do not retry (risk guard, market closed, invalid params)
    """
    if error_code:
        try:
            code = int(error_code)
            if code in _TRANSIENT_CODES:
                return "transient"
            if code in _PERMANENT_CODES:
                return "permanent"
        except (ValueError, TypeError):
            pass

    msg = (error_message or "").lower()
    if any(kw in msg for kw in ("ipc", "no connection", "network", "timeout", "requote")):
        return "transient"
    if any(kw in msg for kw in ("login failed", "account switch failed", "terminal unavailable", "mt5 ipc unavailable")):
        return "terminal"
    if any(kw in msg for kw in ("slippage blocked", "risk", "market closed", "trade disabled", "invalid")):
        return "permanent"

    return "transient"  # unknown errors are assumed transient


def compute_retry_after_iso(attempt_no: int, base_seconds: int = 5) -> str:
    """Exponential backoff: 5s, 10s, 20s, 40s, capped at 60s."""
    delay = min(base_seconds * (2 ** max(attempt_no - 1, 0)), 60)
    return (datetime.utcnow() + timedelta(seconds=delay)).isoformat()


# ── Safety limits ─────────────────────────────────────────────────────────────

MAX_SLAVES_PER_MASTER = 5


# ── Reconciliation ────────────────────────────────────────────────────────────

def reconcile_open_job(job_row: dict) -> dict:
    """
    Verify that a confirmed open job has a matching slave_positions record.
    Returns (expected, actual, status, notes) dict ready for record_broker_reconciliation.
    """
    account_id = job_row["account_id"]
    broker_ticket = job_row.get("broker_ticket")

    expected = {
        "job_id": job_row["job_id"],
        "account_id": account_id,
        "broker_ticket": broker_ticket,
        "status": "position_recorded",
    }

    if not broker_ticket:
        return {
            "expected": expected,
            "actual": {},
            "status": "mismatch",
            "notes": "No broker_ticket recorded for confirmed open job",
        }

    rows = db.get_slave_positions_by_ticket(account_id, broker_ticket)
    if rows:
        return {
            "expected": expected,
            "actual": {"ticket": broker_ticket, "found": True, **rows[0]},
            "status": "matched",
            "notes": None,
        }
    return {
        "expected": expected,
        "actual": {"ticket": broker_ticket, "found": False},
        "status": "mismatch",
        "notes": f"Confirmed ticket {broker_ticket} not found in slave_positions",
    }


def reconcile_close_job(job_row: dict) -> dict:
    """
    Verify that positions are cleared after a confirmed close.
    """
    account_id = job_row["account_id"]
    try:
        request = json.loads(job_row.get("request_json") or "{}")
    except Exception:
        request = {}
    source = request.get("source", request)
    magic_number = source.get("magic_number")
    slave_symbol = job_row.get("slave_symbol") or source.get("symbol")

    expected = {
        "job_id": job_row["job_id"],
        "magic_number": magic_number,
        "symbol": slave_symbol,
        "positions_remaining": 0,
    }

    if magic_number and slave_symbol:
        tickets = db.get_slave_tickets(account_id, magic_number, slave_symbol)
        count = len(tickets)
        actual = {"positions_remaining": count}
        if count == 0:
            return {"expected": expected, "actual": actual, "status": "matched", "notes": None}
        return {
            "expected": expected,
            "actual": actual,
            "status": "mismatch",
            "notes": f"{count} slave position(s) still tracked after confirmed close",
        }

    return {
        "expected": expected,
        "actual": {},
        "status": "matched",
        "notes": "Could not verify — missing magic_number or symbol",
    }


def run_reconciliation(job_row: dict) -> None:
    """Run post-execution reconciliation and persist the result."""
    job_type = job_row.get("job_type", "")
    status = job_row.get("status", "")

    if status != "confirmed":
        return
    if job_type not in ("open", "close"):
        return

    if job_type == "open":
        rec = reconcile_open_job(job_row)
    else:
        rec = reconcile_close_job(job_row)

    reconciliation_id = secrets.token_hex(8)
    try:
        db.record_broker_reconciliation(
            reconciliation_id=reconciliation_id,
            job_id=job_row["job_id"],
            account_id=job_row["account_id"],
            expected=rec["expected"],
            actual=rec["actual"],
            status=rec["status"],
            notes=rec["notes"],
        )
        if rec["status"] == "mismatch":
            logger.warning(
                f"[reconcile] MISMATCH job={job_row['job_id']} "
                f"account={job_row['account_id']}: {rec['notes']}"
            )
    except Exception as exc:
        logger.error(f"[reconcile] Failed to persist reconciliation: {exc}")
