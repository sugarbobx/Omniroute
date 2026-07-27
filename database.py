"""
database.py — OmniRoute v2.4
v2.4: bcrypt app-user hashing, DPAPI broker-password encryption, owner_user_id
      scaffolding, watcher_baseline table, first-run detection.
"""

import json
import logging
import sqlite3
import secrets
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

import crypto
from models import MasterAccount, SlaveAccount, TradeProtection

logger = logging.getLogger("database")
DB_PATH = Path("copybridge.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS masters (
                master_id       TEXT PRIMARY KEY,
                label           TEXT NOT NULL,
                login           INTEGER NOT NULL,
                password        TEXT NOT NULL,
                server          TEXT NOT NULL,
                terminal_path   TEXT NOT NULL,
                magic_number    INTEGER NOT NULL UNIQUE,
                symbol_map_json TEXT NOT NULL DEFAULT '{}',
                enabled         INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slaves (
                account_id          TEXT PRIMARY KEY,
                label               TEXT NOT NULL,
                login               INTEGER NOT NULL,
                password            TEXT NOT NULL,
                server              TEXT NOT NULL,
                terminal_path       TEXT NOT NULL,
                lot_sizing_mode     TEXT NOT NULL DEFAULT 'equity_ratio',
                fixed_lot           REAL NOT NULL DEFAULT 0.01,
                multiplier          REAL NOT NULL DEFAULT 1.0,
                max_lot             REAL NOT NULL DEFAULT 10.0,
                min_lot             REAL NOT NULL DEFAULT 0.01,
                symbol_map_json     TEXT NOT NULL DEFAULT '{}',
                max_open_trades     INTEGER NOT NULL DEFAULT 20,
                slippage_override   INTEGER,
                protection_json     TEXT NOT NULL DEFAULT '{}',
                enabled             INTEGER NOT NULL DEFAULT 1,
                created_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slave_master_links (
                account_id  TEXT NOT NULL,
                master_id   TEXT NOT NULL,
                linked_at   TEXT NOT NULL,
                PRIMARY KEY (account_id, master_id),
                FOREIGN KEY (account_id) REFERENCES slaves(account_id) ON DELETE CASCADE,
                FOREIGN KEY (master_id)  REFERENCES masters(master_id) ON DELETE CASCADE
            );

            -- Tracks slave tickets so we can find them for SL/TP modify
            CREATE TABLE IF NOT EXISTS slave_positions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id   TEXT NOT NULL,
                master_id    TEXT NOT NULL,
                magic_number INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                ticket       INTEGER NOT NULL,
                open_price   REAL NOT NULL,
                trade_type   TEXT NOT NULL,
                opened_at    TEXT NOT NULL,
                UNIQUE(account_id, ticket)
            );

            -- Log of all SL/TP modify operations
            CREATE TABLE IF NOT EXISTS modify_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id   TEXT NOT NULL,
                ticket       INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                old_sl       REAL,
                old_tp       REAL,
                new_sl       REAL,
                new_tp       REAL,
                success      INTEGER NOT NULL,
                error        TEXT,
                modified_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_intents (
                intent_id        TEXT PRIMARY KEY,
                idempotency_key   TEXT NOT NULL UNIQUE,
                intent_type       TEXT NOT NULL,
                master_id         TEXT,
                magic_number      INTEGER,
                symbol            TEXT,
                direction         TEXT,
                payload_json      TEXT NOT NULL,
                status            TEXT NOT NULL DEFAULT 'received',
                error             TEXT,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_jobs (
                job_id          TEXT PRIMARY KEY,
                intent_id       TEXT NOT NULL,
                account_id      TEXT NOT NULL,
                master_id       TEXT,
                job_type        TEXT NOT NULL,
                symbol          TEXT,
                slave_symbol    TEXT,
                request_json    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'queued',
                claimed_by      TEXT,
                claimed_at      TEXT,
                started_at      TEXT,
                finished_at     TEXT,
                broker_ticket   INTEGER,
                fill_price      REAL,
                error_code      TEXT,
                error_message   TEXT,
                attempts        INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                FOREIGN KEY(intent_id) REFERENCES trade_intents(intent_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS trade_job_attempts (
                attempt_id      TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL,
                attempt_no      INTEGER NOT NULL,
                status          TEXT NOT NULL,
                request_json    TEXT,
                response_json   TEXT,
                error_code      TEXT,
                error_message   TEXT,
                latency_ms      REAL,
                created_at      TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES trade_jobs(job_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS broker_reconciliations (
                reconciliation_id TEXT PRIMARY KEY,
                job_id            TEXT NOT NULL,
                account_id        TEXT NOT NULL,
                expected_json     TEXT NOT NULL,
                actual_json       TEXT NOT NULL,
                status            TEXT NOT NULL,
                notes             TEXT,
                created_at        TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES trade_jobs(job_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_slave_positions_lookup
                ON slave_positions(account_id, magic_number, symbol);

            CREATE INDEX IF NOT EXISTS idx_trade_intents_master
                ON trade_intents(master_id, magic_number, status);

            CREATE INDEX IF NOT EXISTS idx_trade_jobs_intent
                ON trade_jobs(intent_id, account_id, status);

            CREATE INDEX IF NOT EXISTS idx_trade_attempts_job
                ON trade_job_attempts(job_id, attempt_no);

            CREATE TABLE IF NOT EXISTS mt5_workers (
                worker_id         TEXT PRIMARY KEY,
                account_id        TEXT,
                terminal_path     TEXT NOT NULL,
                worker_role       TEXT NOT NULL DEFAULT 'slave',
                status            TEXT NOT NULL DEFAULT 'starting',
                current_job_id    TEXT,
                current_error     TEXT,
                last_heartbeat_at TEXT,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                FOREIGN KEY(account_id) REFERENCES slaves(account_id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_mt5_workers_status
                ON mt5_workers(status, worker_role);

            CREATE TABLE IF NOT EXISTS strategies (
                strategy_id     TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                mode            TEXT NOT NULL CHECK(mode IN ('visual', 'code')),
                symbol          TEXT NOT NULL,
                timeframe       TEXT NOT NULL,
                file_path       TEXT NOT NULL,
                assigned_bot_id TEXT,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(assigned_bot_id) REFERENCES masters(master_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS strategy_results (
                result_id        TEXT PRIMARY KEY,
                strategy_id      TEXT NOT NULL,
                bot_id           TEXT NOT NULL,
                signal_direction TEXT,
                executed_at      TEXT,
                entry_price      REAL,
                exit_price       REAL,
                pnl              REAL,
                mode             TEXT CHECK(mode IN ('forward_test', 'live')),
                FOREIGN KEY(strategy_id) REFERENCES strategies(strategy_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS app_users (
                user_id       TEXT PRIMARY KEY,
                username      TEXT NOT NULL UNIQUE,
                display_name  TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'admin',
                enabled       INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_sessions (
                token        TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                expires_at   TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES app_users(user_id) ON DELETE CASCADE
            );

            -- Baseline snapshot taken at watcher startup so pre-existing
            -- master positions are never emitted as new trade intents.
            CREATE TABLE IF NOT EXISTS watcher_baseline (
                master_id    TEXT NOT NULL,
                ticket       INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                PRIMARY KEY (master_id, ticket)
            );
        """)
        # SQLite has no ALTER TABLE ... ADD COLUMN IF NOT EXISTS — check pragma first
        _ensure_column(conn, "masters", "is_virtual_bot", "INTEGER DEFAULT 0")
        _ensure_column(conn, "masters", "bot_symbol", "TEXT")
        _ensure_column(conn, "masters", "bot_timeframe", "TEXT DEFAULT 'M5'")
        _ensure_column(conn, "masters", "base_volume", "REAL DEFAULT 0.1")
        _ensure_column(conn, "masters", "strategy_name", "TEXT")
        _ensure_column(conn, "masters", "forward_test", "INTEGER DEFAULT 0")
        _ensure_column(conn, "masters", "bot_mode", "TEXT DEFAULT 'standalone'")
        _ensure_column(conn, "trade_jobs", "claimed_by", "TEXT")
        _ensure_column(conn, "trade_jobs", "claimed_at", "TEXT")
        _ensure_column(conn, "trade_jobs", "started_at", "TEXT")
        _ensure_column(conn, "trade_jobs", "finished_at", "TEXT")
        _ensure_column(conn, "trade_jobs", "retry_after", "TEXT")
        _ensure_column(conn, "trade_jobs", "max_retries", "INTEGER DEFAULT 3")
        # owner scoping
        _ensure_column(conn, "masters",       "owner_user_id", "TEXT")
        _ensure_column(conn, "slaves",        "owner_user_id", "TEXT")
        _ensure_column(conn, "trade_intents", "owner_user_id", "TEXT")
        _ensure_column(conn, "trade_jobs",    "owner_user_id", "TEXT")
        # encrypted broker credentials (DPAPI envelope)
        _ensure_column(conn, "masters", "password_enc",          "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "masters", "investor_password_enc",  "TEXT NOT NULL DEFAULT ''")
        _ensure_column(conn, "slaves",  "password_enc",           "TEXT NOT NULL DEFAULT ''")
        # investor_password field on masters (plain login path kept for compat)
        _ensure_column(conn, "masters", "investor_password", "TEXT NOT NULL DEFAULT ''")
        _seed_demo_user_if_none(conn)
    logger.info(f"Database ready: {DB_PATH.resolve()}")


def _ensure_column(conn, table: str, col: str, decl: str):
    existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        logger.info(f"Migration: added {table}.{col}")


def _seed_demo_user_if_none(conn):
    """Only seed a demo user if the table is completely empty (first ever run).
    New deployments should use POST /setup/create-admin instead."""
    pass  # first-run setup handled via /setup endpoint; no hardcoded credentials


def needs_first_run_setup() -> bool:
    """Return True if no admin users exist yet."""
    with get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM app_users WHERE role='admin' AND enabled=1").fetchone()
    return (row["n"] == 0) if row else True


def create_first_admin(username: str, display_name: str, password: str) -> dict:
    """Create the first admin user. Raises if one already exists."""
    if not needs_first_run_setup():
        raise ValueError("Admin user already exists")
    user_id = secrets.token_hex(8)
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_users (user_id, username, display_name, password_hash, role, enabled, created_at)
            VALUES (?, ?, ?, ?, 'admin', 1, ?)
            """,
            (user_id, username.strip().lower(), display_name.strip(),
             crypto.hash_password(password), datetime.utcnow().isoformat()),
        )
    return {"user_id": user_id, "username": username.strip().lower()}


def verify_app_user(username: str, password: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM app_users WHERE lower(username)=lower(?) AND enabled=1",
            (username,),
        ).fetchone()
    if not row:
        return None
    if not crypto.verify_password(password, row["password_hash"]):
        return None
    # Transparent upgrade: re-hash legacy SHA-256 hashes with bcrypt on successful login
    if not crypto.is_bcrypt_hash(row["password_hash"]):
        new_hash = crypto.hash_password(password)
        with get_conn() as conn:
            conn.execute("UPDATE app_users SET password_hash=? WHERE user_id=?",
                         (new_hash, row["user_id"]))
    return {
        "user_id": row["user_id"],
        "username": row["username"],
        "display_name": row["display_name"],
        "role": row["role"],
    }


def create_app_session(user_id: str, ttl_hours: int = 24) -> dict:
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires = now.timestamp() + ttl_hours * 3600
    expires_at = datetime.utcfromtimestamp(expires).isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_sessions (token, user_id, created_at, last_seen_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (token, user_id, now.isoformat(), now.isoformat(), expires_at),
        )
    return {"token": token, "expires_at": expires_at}


def get_app_session(token: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT s.token, s.user_id, s.created_at, s.last_seen_at, s.expires_at,
                   u.username, u.display_name, u.role
            FROM app_sessions s
            JOIN app_users u ON u.user_id = s.user_id
            WHERE s.token=? AND u.enabled=1
            """,
            (token,),
        ).fetchone()
    if not row:
        return None
    try:
        if datetime.fromisoformat(row["expires_at"]) <= datetime.utcnow():
            delete_app_session(token)
            return None
    except Exception:
        delete_app_session(token)
        return None
    return {
        "token": row["token"],
        "user_id": row["user_id"],
        "username": row["username"],
        "display_name": row["display_name"],
        "role": row["role"],
        "created_at": row["created_at"],
        "last_seen_at": row["last_seen_at"],
        "expires_at": row["expires_at"],
    }


def touch_app_session(token: str):
    with get_conn() as conn:
        conn.execute(
            "UPDATE app_sessions SET last_seen_at=? WHERE token=?",
            (datetime.utcnow().isoformat(), token),
        )


def delete_app_session(token: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM app_sessions WHERE token=?", (token,))


# ── Trade execution journal ──────────────────────────────────────────────────

def save_trade_intent(
    intent_id: str,
    idempotency_key: str,
    intent_type: str,
    payload: dict,
    master_id: Optional[str] = None,
    magic_number: Optional[int] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    status: str = "received",
):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO trade_intents
              (intent_id, idempotency_key, intent_type, master_id, magic_number,
               symbol, direction, payload_json, status, error, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
            ON CONFLICT(intent_id) DO UPDATE SET
              idempotency_key=excluded.idempotency_key,
              intent_type=excluded.intent_type,
              master_id=excluded.master_id,
              magic_number=excluded.magic_number,
              symbol=excluded.symbol,
              direction=excluded.direction,
              payload_json=excluded.payload_json,
              status=excluded.status,
              updated_at=excluded.updated_at
            """,
            (
                intent_id,
                idempotency_key,
                intent_type,
                master_id,
                magic_number,
                symbol,
                direction,
                json.dumps(payload),
                status,
                now,
                now,
            ),
        )


def get_trade_intent(intent_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM trade_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
    return dict(row) if row else None


def update_trade_intent_status(intent_id: str, status: str, error: Optional[str] = None):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE trade_intents
            SET status=?, error=?, updated_at=?
            WHERE intent_id=?
            """,
            (status, error, datetime.utcnow().isoformat(), intent_id),
        )


def save_trade_job(
    job_id: str,
    intent_id: str,
    account_id: str,
    request: dict,
    master_id: Optional[str] = None,
    job_type: str = "open",
    symbol: Optional[str] = None,
    slave_symbol: Optional[str] = None,
    status: str = "queued",
):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO trade_jobs
              (job_id, intent_id, account_id, master_id, job_type, symbol, slave_symbol,
               request_json, status, broker_ticket, fill_price, error_code, error_message,
               attempts, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, 0, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
              intent_id=excluded.intent_id,
              account_id=excluded.account_id,
              master_id=excluded.master_id,
              job_type=excluded.job_type,
              symbol=excluded.symbol,
              slave_symbol=excluded.slave_symbol,
              request_json=excluded.request_json,
              status=excluded.status,
              updated_at=excluded.updated_at
            """,
            (
                job_id,
                intent_id,
                account_id,
                master_id,
                job_type,
                symbol,
                slave_symbol,
                json.dumps(request),
                status,
                now,
                now,
            ),
        )


def update_trade_job_status(
    job_id: str,
    status: str,
    broker_ticket: Optional[int] = None,
    fill_price: Optional[float] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
):
    final_states = {"confirmed", "blocked", "failed", "dead_letter", "rejected", "completed", "completed_with_errors", "no_targets"}
    finished_at = datetime.utcnow().isoformat() if status in final_states else None
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE trade_jobs
            SET status=?, broker_ticket=?, fill_price=?, error_code=?, error_message=?,
                attempts=attempts + 1, finished_at=COALESCE(?, finished_at), updated_at=?
            WHERE job_id=?
            """,
            (
                status,
                broker_ticket,
                fill_price,
                error_code,
                error_message,
                finished_at,
                datetime.utcnow().isoformat(),
                job_id,
            ),
        )


def get_trade_job(job_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM trade_jobs WHERE job_id=?",
            (job_id,),
        ).fetchone()
    return dict(row) if row else None


def claim_trade_job(job_id: str, worker_id: str) -> bool:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE trade_jobs
            SET claimed_by=?, claimed_at=?, status='dispatching', updated_at=?
            WHERE job_id=? AND status IN ('queued', 'retry_wait')
            """,
            (worker_id, now, now, job_id),
        )
        return cur.rowcount > 0


def claim_next_trade_job(worker_id: str, account_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM trade_jobs
            WHERE account_id=? AND status IN ('queued', 'retry_wait')
              AND (retry_after IS NULL OR retry_after <= ?)
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (account_id, datetime.utcnow().isoformat()),
        ).fetchone()
        if not row:
            return None
        now = datetime.utcnow().isoformat()
        cur = conn.execute(
            """
            UPDATE trade_jobs
            SET claimed_by=?, claimed_at=?, status='dispatching', updated_at=?
            WHERE job_id=? AND status IN ('queued', 'retry_wait')
            """,
            (worker_id, now, now, row["job_id"]),
        )
        if cur.rowcount <= 0:
            return None
        row = conn.execute("SELECT * FROM trade_jobs WHERE job_id=?", (row["job_id"],)).fetchone()
    return dict(row) if row else None


def mark_trade_job_started(job_id: str, worker_id: str) -> bool:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE trade_jobs
            SET status='executing', started_at=COALESCE(started_at, ?), updated_at=?
            WHERE job_id=? AND claimed_by=?
            """,
            (now, now, job_id, worker_id),
        )
        return cur.rowcount > 0


def mark_trade_job_finished(
    job_id: str,
    status: str,
    broker_ticket: Optional[int] = None,
    fill_price: Optional[float] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
):
    update_trade_job_status(
        job_id=job_id,
        status=status,
        broker_ticket=broker_ticket,
        fill_price=fill_price,
        error_code=error_code,
        error_message=error_message,
    )


def save_mt5_worker(
    worker_id: str,
    account_id: Optional[str],
    terminal_path: str,
    worker_role: str = "slave",
    status: str = "starting",
    current_job_id: Optional[str] = None,
    current_error: Optional[str] = None,
):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO mt5_workers
              (worker_id, account_id, terminal_path, worker_role, status,
               current_job_id, current_error, last_heartbeat_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(worker_id) DO UPDATE SET
              account_id=excluded.account_id,
              terminal_path=excluded.terminal_path,
              worker_role=excluded.worker_role,
              status=excluded.status,
              current_job_id=excluded.current_job_id,
              current_error=excluded.current_error,
              updated_at=excluded.updated_at
            """,
            (
                worker_id,
                account_id,
                terminal_path,
                worker_role,
                status,
                current_job_id,
                current_error,
                now,
                now,
                now,
            ),
        )


def load_mt5_workers() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM mt5_workers").fetchall()
    return [dict(r) for r in rows]


def get_mt5_worker(worker_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM mt5_workers WHERE worker_id=?",
            (worker_id,),
        ).fetchone()
    return dict(row) if row else None


def update_mt5_worker(
    worker_id: str,
    status: Optional[str] = None,
    current_job_id: Optional[str] = None,
    current_error: Optional[str] = None,
    last_heartbeat_at: bool = False,
    clear_current_job: bool = False,
    clear_current_error: bool = False,
):
    sets = []
    vals = []
    if status is not None:
        sets.append("status=?")
        vals.append(status)
    if current_job_id is not None:
        sets.append("current_job_id=?")
        vals.append(current_job_id)
    elif clear_current_job:
        sets.append("current_job_id=NULL")
    if current_error is not None:
        sets.append("current_error=?")
        vals.append(current_error)
    elif clear_current_error:
        sets.append("current_error=NULL")
    if last_heartbeat_at:
        sets.append("last_heartbeat_at=?")
        vals.append(datetime.utcnow().isoformat())
    if not sets:
        return
    sets.append("updated_at=?")
    vals.append(datetime.utcnow().isoformat())
    vals.append(worker_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE mt5_workers SET {', '.join(sets)} WHERE worker_id=?",
            vals,
        )


def claim_mt5_worker(worker_id: str) -> bool:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE mt5_workers
            SET status='idle', last_heartbeat_at=?, updated_at=?
            WHERE worker_id=? AND status IN ('starting', 'idle', 'degraded')
            """,
            (now, now, worker_id),
        )
        return cur.rowcount > 0


def delete_mt5_worker(worker_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM mt5_workers WHERE worker_id=?", (worker_id,))


def record_trade_job_attempt(
    job_id: str,
    attempt_no: int,
    status: str,
    request: Optional[dict] = None,
    response: Optional[dict] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    latency_ms: Optional[float] = None,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO trade_job_attempts
              (attempt_id, job_id, attempt_no, status, request_json, response_json,
               error_code, error_message, latency_ms, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(secrets.token_hex(8)),
                job_id,
                attempt_no,
                status,
                json.dumps(request) if request is not None else None,
                json.dumps(response) if response is not None else None,
                error_code,
                error_message,
                latency_ms,
                datetime.utcnow().isoformat(),
            ),
        )


def record_broker_reconciliation(
    reconciliation_id: str,
    job_id: str,
    account_id: str,
    expected: dict,
    actual: dict,
    status: str,
    notes: Optional[str] = None,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO broker_reconciliations
              (reconciliation_id, job_id, account_id, expected_json, actual_json,
               status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                reconciliation_id,
                job_id,
                account_id,
                json.dumps(expected),
                json.dumps(actual),
                status,
                notes,
                datetime.utcnow().isoformat(),
            ),
        )


# ── Masters ──────────────────────────────────────────────────────────────────

def save_master(m: MasterAccount, owner_user_id: Optional[str] = None):
    pwd_enc = crypto.encrypt_password(m.password) if m.password and not crypto.is_encrypted(m.password) else (m.password or "")
    inv_enc = ""
    if hasattr(m, "investor_password") and m.investor_password:
        inv_enc = crypto.encrypt_password(m.investor_password) if not crypto.is_encrypted(m.investor_password) else m.investor_password
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO masters
              (master_id,label,login,password,password_enc,investor_password,investor_password_enc,
               server,terminal_path,magic_number,symbol_map_json,enabled,created_at,owner_user_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(master_id) DO UPDATE SET
              label=excluded.label, login=excluded.login,
              password=excluded.password,
              password_enc=excluded.password_enc,
              investor_password=excluded.investor_password,
              investor_password_enc=excluded.investor_password_enc,
              server=excluded.server,
              terminal_path=excluded.terminal_path,
              magic_number=excluded.magic_number,
              symbol_map_json=excluded.symbol_map_json,
              enabled=excluded.enabled,
              owner_user_id=COALESCE(excluded.owner_user_id, owner_user_id)
        """, (
            m.master_id, m.label, m.login, m.password, pwd_enc,
            getattr(m, "investor_password", ""), inv_enc,
            m.server, m.terminal_path, m.magic_number, json.dumps(m.symbol_map),
            int(m.enabled), m.created_at.isoformat(), owner_user_id,
        ))


def load_all_masters() -> list[MasterAccount]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM masters WHERE enabled=1").fetchall()
    return [_row_to_master(r) for r in rows]


def delete_master(master_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM masters WHERE master_id=?", (master_id,))


def update_master(master_id: str, data: dict):
    col_map = {
        "label": "label", "login": "login", "password": "password",
        "server": "server", "terminal_path": "terminal_path",
        "magic_number": "magic_number", "symbol_map": "symbol_map_json",
        "enabled": "enabled",
    }
    sets, vals = [], []
    for key, col in col_map.items():
        if key in data:
            v = data[key]
            if key == "symbol_map":
                v = json.dumps(v)
            elif isinstance(v, bool):
                v = int(v)
            sets.append(f"{col}=?")
            vals.append(v)
    if not sets:
        return
    vals.append(master_id)
    with get_conn() as conn:
        conn.execute(
            f"UPDATE masters SET {', '.join(sets)} WHERE master_id=? AND (is_virtual_bot IS NULL OR is_virtual_bot=0)",
            vals,
        )


def _row_to_master(row) -> MasterAccount:
    return MasterAccount(
        master_id=row["master_id"], label=row["label"], login=row["login"],
        password=row["password"], server=row["server"],
        terminal_path=row["terminal_path"], magic_number=row["magic_number"],
        symbol_map=json.loads(row["symbol_map_json"]),
        enabled=bool(row["enabled"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


# ── Slaves ───────────────────────────────────────────────────────────────────

def save_slave(s: SlaveAccount, owner_user_id: Optional[str] = None):
    pwd_enc = crypto.encrypt_password(s.password) if s.password and not crypto.is_encrypted(s.password) else (s.password or "")
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO slaves
              (account_id,label,login,password,password_enc,server,terminal_path,lot_sizing_mode,
               fixed_lot,multiplier,max_lot,min_lot,symbol_map_json,max_open_trades,
               slippage_override,protection_json,enabled,created_at,owner_user_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
              label=excluded.label, login=excluded.login,
              password=excluded.password,
              password_enc=excluded.password_enc,
              server=excluded.server,
              terminal_path=excluded.terminal_path,
              lot_sizing_mode=excluded.lot_sizing_mode,
              fixed_lot=excluded.fixed_lot, multiplier=excluded.multiplier,
              max_lot=excluded.max_lot, min_lot=excluded.min_lot,
              symbol_map_json=excluded.symbol_map_json,
              max_open_trades=excluded.max_open_trades,
              slippage_override=excluded.slippage_override,
              protection_json=excluded.protection_json,
              enabled=excluded.enabled,
              owner_user_id=COALESCE(excluded.owner_user_id, owner_user_id)
        """, (
            s.account_id, s.label, s.login, s.password, pwd_enc,
            s.server, s.terminal_path,
            s.lot_sizing_mode.value, s.fixed_lot, s.multiplier, s.max_lot, s.min_lot,
            json.dumps(s.symbol_map), s.max_open_trades, s.slippage_override,
            s.protection.model_dump_json(), int(s.enabled), s.created_at.isoformat(),
            owner_user_id,
        ))


def update_slave_protection(account_id: str, protection: TradeProtection):
    """Targeted update — only touches the protection column."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE slaves SET protection_json=? WHERE account_id=?",
            (protection.model_dump_json(), account_id),
        )


def load_all_slaves() -> list[SlaveAccount]:
    with get_conn() as conn:
        rows  = conn.execute("SELECT * FROM slaves WHERE enabled=1").fetchall()
        links = conn.execute("SELECT account_id, master_id FROM slave_master_links").fetchall()
    slave_masters: dict[str, list[str]] = {}
    for lnk in links:
        slave_masters.setdefault(lnk["account_id"], []).append(lnk["master_id"])
    result = []
    for row in rows:
        s = _row_to_slave(row)
        s.master_ids = slave_masters.get(s.account_id, [])
        result.append(s)
    return result


def delete_slave(account_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM slaves WHERE account_id=?", (account_id,))
        conn.execute("DELETE FROM slave_positions WHERE account_id=?", (account_id,))


def update_slave(account_id: str, data: dict):
    col_map = {
        "label": "label", "login": "login", "password": "password",
        "server": "server", "lot_sizing_mode": "lot_sizing_mode",
        "fixed_lot": "fixed_lot", "multiplier": "multiplier",
        "max_lot": "max_lot", "min_lot": "min_lot",
        "max_open_trades": "max_open_trades", "symbol_map": "symbol_map_json",
        "enabled": "enabled",
    }
    sets, vals = [], []
    for key, col in col_map.items():
        if key in data:
            v = data[key]
            if key == "symbol_map":
                v = json.dumps(v)
            elif isinstance(v, bool):
                v = int(v)
            sets.append(f"{col}=?")
            vals.append(v)
    if not sets:
        return
    vals.append(account_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE slaves SET {', '.join(sets)} WHERE account_id=?", vals)


def _row_to_slave(row) -> SlaveAccount:
    prot_raw = row["protection_json"] if "protection_json" in row.keys() else "{}"
    try:
        prot = TradeProtection.model_validate_json(prot_raw)
    except Exception:
        prot = TradeProtection()
    return SlaveAccount(
        account_id=row["account_id"], label=row["label"], login=row["login"],
        password=row["password"], server=row["server"], terminal_path=row["terminal_path"],
        lot_sizing_mode=row["lot_sizing_mode"], fixed_lot=row["fixed_lot"],
        multiplier=row["multiplier"], max_lot=row["max_lot"], min_lot=row["min_lot"],
        symbol_map=json.loads(row["symbol_map_json"]),
        max_open_trades=row["max_open_trades"],
        slippage_override=row["slippage_override"],
        protection=prot,
        enabled=bool(row["enabled"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


# ── Links ────────────────────────────────────────────────────────────────────

def link_slave_to_master(account_id: str, master_id: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO slave_master_links (account_id,master_id,linked_at) VALUES (?,?,?)",
            (account_id, master_id, datetime.utcnow().isoformat()),
        )


def unlink_slave_from_master(account_id: str, master_id: str):
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM slave_master_links WHERE account_id=? AND master_id=?",
            (account_id, master_id),
        )


def get_slaves_for_master(master_id: str) -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT account_id FROM slave_master_links WHERE master_id=?", (master_id,)
        ).fetchall()
    return [r["account_id"] for r in rows]


def get_masters_for_slave(account_id: str) -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT master_id FROM slave_master_links WHERE account_id=?", (account_id,)
        ).fetchall()
    return [r["master_id"] for r in rows]


# ── Position tracking (for SL/TP modify) ────────────────────────────────────

def record_slave_position(account_id: str, master_id: str, magic_number: int,
                           symbol: str, ticket: int, open_price: float, trade_type: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO slave_positions
              (account_id,master_id,magic_number,symbol,ticket,open_price,trade_type,opened_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (account_id, master_id, magic_number, symbol, ticket, open_price, trade_type,
              datetime.utcnow().isoformat()))


def get_slave_tickets(account_id: str, magic_number: int, symbol: str) -> list[dict]:
    """Return all open slave tickets for a given magic+symbol combination."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ticket, open_price, trade_type FROM slave_positions
            WHERE account_id=? AND magic_number=? AND symbol=?
        """, (account_id, magic_number, symbol)).fetchall()
    return [{"ticket": r["ticket"], "open_price": r["open_price"], "trade_type": r["trade_type"]} for r in rows]


def remove_slave_position(account_id: str, ticket: int):
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM slave_positions WHERE account_id=? AND ticket=?",
            (account_id, ticket),
        )


def log_modify(account_id: str, ticket: int, symbol: str,
               old_sl: float, old_tp: float, new_sl: float, new_tp: float,
               success: bool, error: Optional[str] = None):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO modify_log
              (account_id,ticket,symbol,old_sl,old_tp,new_sl,new_tp,success,error,modified_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (account_id, ticket, symbol, old_sl, old_tp, new_sl, new_tp,
              int(success), error, datetime.utcnow().isoformat()))


# ── Decrypted credential helpers (internal use only — never expose to API) ───

def get_master_password(master_id: str) -> str:
    """Return decrypted trading password for a master. Never log or return over API."""
    with get_conn() as conn:
        row = conn.execute("SELECT password, password_enc FROM masters WHERE master_id=?", (master_id,)).fetchone()
    if not row:
        return ""
    enc = row["password_enc"]
    if enc:
        try:
            return crypto.decrypt_password(enc)
        except Exception:
            pass
    return row["password"] or ""


def get_master_investor_password(master_id: str) -> str:
    """Return decrypted investor (read-only) password for a master."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT investor_password, investor_password_enc FROM masters WHERE master_id=?",
            (master_id,),
        ).fetchone()
    if not row:
        return ""
    enc = row["investor_password_enc"]
    if enc:
        try:
            return crypto.decrypt_password(enc)
        except Exception:
            pass
    return row["investor_password"] or ""


def get_slave_password(account_id: str) -> str:
    """Return decrypted trading password for a slave."""
    with get_conn() as conn:
        row = conn.execute("SELECT password, password_enc FROM slaves WHERE account_id=?", (account_id,)).fetchone()
    if not row:
        return ""
    enc = row["password_enc"]
    if enc:
        try:
            return crypto.decrypt_password(enc)
        except Exception:
            pass
    return row["password"] or ""


def disable_account(account_id_or_master_id: str):
    """Disable a master or slave without deleting it (keeps audit trail)."""
    with get_conn() as conn:
        conn.execute("UPDATE masters SET enabled=0 WHERE master_id=?", (account_id_or_master_id,))
        conn.execute("UPDATE slaves  SET enabled=0 WHERE account_id=?", (account_id_or_master_id,))


# ── Watcher baseline ─────────────────────────────────────────────────────────

def save_watcher_baseline(master_id: str, tickets: dict):
    """
    Persist the set of positions/orders seen at watcher startup so they are
    not emitted as new trade intents. tickets = {ticket_int: snapshot_dict}.
    """
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("DELETE FROM watcher_baseline WHERE master_id=?", (master_id,))
        for ticket, snap in tickets.items():
            conn.execute(
                "INSERT INTO watcher_baseline (master_id, ticket, snapshot_json, created_at) VALUES (?,?,?,?)",
                (master_id, int(ticket), json.dumps(snap), now),
            )


def get_watcher_baseline(master_id: str) -> set:
    """Return set of ticket integers that are part of this master's baseline."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticket FROM watcher_baseline WHERE master_id=?", (master_id,)
        ).fetchall()
    return {r["ticket"] for r in rows}


def clear_watcher_baseline(master_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM watcher_baseline WHERE master_id=?", (master_id,))


# ── Virtual bots (rows in masters with is_virtual_bot=1) ────────────────────

_BOT_FIELDS = ("is_virtual_bot", "bot_symbol", "bot_timeframe", "base_volume",
               "strategy_name", "forward_test", "bot_mode")


def _row_to_bot(row) -> dict:
    return {
        "bot_id":        row["master_id"],
        "label":         row["label"],
        "magic_number":  row["magic_number"],
        "enabled":       bool(row["enabled"]),
        "symbol":        row["bot_symbol"],
        "timeframe":     row["bot_timeframe"] or "M5",
        "base_volume":   row["base_volume"] if row["base_volume"] is not None else 0.1,
        "strategy_name": row["strategy_name"],
        "forward_test":  bool(row["forward_test"]),
        "mode":          row["bot_mode"] or "standalone",
        "created_at":    row["created_at"],
    }


def get_all_virtual_bots() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM masters WHERE is_virtual_bot=1").fetchall()
    return [_row_to_bot(r) for r in rows]


def get_virtual_bot(bot_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM masters WHERE master_id=? AND is_virtual_bot=1", (bot_id,)
        ).fetchone()
    return _row_to_bot(row) if row else None


def save_virtual_bot(bot: dict):
    """Insert a virtual bot as a masters row. Standard master columns get
    placeholder values — a virtual bot has no real MT5 master terminal."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO masters
              (master_id,label,login,password,server,terminal_path,magic_number,
               symbol_map_json,enabled,created_at,
               is_virtual_bot,bot_symbol,bot_timeframe,base_volume,strategy_name,forward_test,bot_mode)
            VALUES (?,?,0,'','virtual','virtual',?,'{}',?,?,1,?,?,?,?,?,?)
        """, (
            bot["bot_id"], bot["label"], bot["magic_number"], int(bot.get("enabled", True)),
            datetime.utcnow().isoformat(),
            bot["symbol"], bot.get("timeframe", "M5"), bot.get("base_volume", 0.1),
            bot.get("strategy_name"), int(bot.get("forward_test", False)),
            bot.get("mode", "standalone"),
        ))


def update_virtual_bot(bot_id: str, updates: dict):
    col_map = {
        "label": "label", "magic_number": "magic_number", "enabled": "enabled",
        "symbol": "bot_symbol", "timeframe": "bot_timeframe", "base_volume": "base_volume",
        "strategy_name": "strategy_name", "forward_test": "forward_test", "mode": "bot_mode",
    }
    sets, vals = [], []
    for key, col in col_map.items():
        if key in updates:
            v = updates[key]
            if isinstance(v, bool):
                v = int(v)
            sets.append(f"{col}=?")
            vals.append(v)
    if not sets:
        return
    vals.append(bot_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE masters SET {', '.join(sets)} WHERE master_id=? AND is_virtual_bot=1", vals)


def delete_virtual_bot(bot_id: str):
    with get_conn() as conn:
        conn.execute("UPDATE strategies SET assigned_bot_id=NULL WHERE assigned_bot_id=?", (bot_id,))
        conn.execute("DELETE FROM masters WHERE master_id=? AND is_virtual_bot=1", (bot_id,))


# ── Strategies ───────────────────────────────────────────────────────────────

def get_strategy(strategy_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE strategy_id=?", (strategy_id,)).fetchone()
    return dict(row) if row else None


def get_all_strategies() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM strategies ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def save_strategy(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO strategies
              (strategy_id,name,mode,symbol,timeframe,file_path,assigned_bot_id,created_at)
            VALUES (?,?,?,?,?,?,?,COALESCE((SELECT created_at FROM strategies WHERE strategy_id=?),CURRENT_TIMESTAMP))
        """, (data["strategy_id"], data["name"], data["mode"], data["symbol"],
              data["timeframe"], data["file_path"], data.get("assigned_bot_id"),
              data["strategy_id"]))


def delete_strategy(strategy_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM strategies WHERE strategy_id=?", (strategy_id,))


def assign_strategy_to_bot(strategy_id: str, bot_id: Optional[str]):
    with get_conn() as conn:
        # one strategy per bot — unassign anything previously on this bot
        if bot_id:
            conn.execute("UPDATE strategies SET assigned_bot_id=NULL WHERE assigned_bot_id=?", (bot_id,))
        conn.execute("UPDATE strategies SET assigned_bot_id=? WHERE strategy_id=?", (bot_id, strategy_id))


def get_strategy_for_bot(bot_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE assigned_bot_id=?", (bot_id,)).fetchone()
    return dict(row) if row else None


def log_strategy_result(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO strategy_results
              (result_id,strategy_id,bot_id,signal_direction,executed_at,entry_price,exit_price,pnl,mode)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (data["result_id"], data["strategy_id"], data["bot_id"],
              data.get("signal_direction"), data.get("executed_at", datetime.utcnow().isoformat()),
              data.get("entry_price"), data.get("exit_price"), data.get("pnl"),
              data.get("mode", "forward_test")))


def get_strategy_results(bot_id: str, limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM strategy_results WHERE bot_id=?
            ORDER BY executed_at DESC LIMIT ?
        """, (bot_id, limit)).fetchall()
    return [dict(r) for r in rows]


# ── Retry and dead-letter helpers ─────────────────────────────────────────────

def schedule_job_retry(
    job_id: str,
    retry_after: str,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE trade_jobs
            SET status='retry_wait', retry_after=?, error_code=?, error_message=?,
                attempts=attempts + 1, updated_at=?
            WHERE job_id=?
            """,
            (retry_after, error_code, error_message, now, job_id),
        )


def dead_letter_job(
    job_id: str,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE trade_jobs
            SET status='dead_letter', finished_at=COALESCE(finished_at, ?),
                error_code=?, error_message=?, attempts=attempts + 1, updated_at=?
            WHERE job_id=?
            """,
            (now, error_code, error_message, now, job_id),
        )


# ── Observability queries ─────────────────────────────────────────────────────

def get_job_metrics() -> dict:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM trade_jobs GROUP BY status"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS n FROM trade_jobs").fetchone()["n"]
        avg_latency = conn.execute(
            """
            SELECT AVG(latency_ms) AS avg_ms
            FROM trade_job_attempts
            WHERE status='confirmed' AND latency_ms IS NOT NULL
            """
        ).fetchone()["avg_ms"]
        rec_counts = conn.execute(
            "SELECT status, COUNT(*) AS n FROM broker_reconciliations GROUP BY status"
        ).fetchall()
        worker_counts = conn.execute(
            "SELECT status, COUNT(*) AS n FROM mt5_workers GROUP BY status"
        ).fetchall()

    by_status = {r["status"]: r["n"] for r in rows}
    rec_by_status = {r["status"]: r["n"] for r in rec_counts}
    worker_by_status = {r["status"]: r["n"] for r in worker_counts}

    return {
        "jobs_total": total,
        "jobs_by_status": by_status,
        "jobs_confirmed": by_status.get("confirmed", 0),
        "jobs_failed": by_status.get("failed", 0),
        "jobs_dead_letter": by_status.get("dead_letter", 0),
        "jobs_queued": by_status.get("queued", 0) + by_status.get("retry_wait", 0),
        "avg_execution_latency_ms": round(avg_latency, 2) if avg_latency else None,
        "reconciliations_by_status": rec_by_status,
        "workers_by_status": worker_by_status,
    }


def get_recent_jobs(limit: int = 100, account_id: Optional[str] = None) -> list[dict]:
    with get_conn() as conn:
        if account_id:
            rows = conn.execute(
                "SELECT * FROM trade_jobs WHERE account_id=? ORDER BY created_at DESC LIMIT ?",
                (account_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM trade_jobs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [dict(r) for r in rows]


def get_job_attempts(job_id: str) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_job_attempts WHERE job_id=? ORDER BY attempt_no",
            (job_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_reconciliations(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM broker_reconciliations ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_slave_positions_by_ticket(account_id: str, ticket: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM slave_positions WHERE account_id=? AND ticket=?",
            (account_id, ticket),
        ).fetchall()
    return [dict(r) for r in rows]


def get_recent_intents(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM trade_intents ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]
