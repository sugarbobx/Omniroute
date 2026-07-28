"""
router.py — OmniRoute v2.3
Multi-terminal MT5 architecture:
  • Each slave gets a dedicated MT5 terminal copy (auto-provisioned)
  • asyncio.Lock serialises all MT5 ops (singleton IPC)
  • Terminal switching via mt5.shutdown() + mt5.initialize(path=...)
  • Fast switching: terminals already logged in to their broker servers
  • All timeouts ≥ 180 s (FundedNext needs ~70 s on cold connect)
"""

import asyncio
import json
import logging
import uuid
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Optional

MT5_BASE_PATH  = Path(r"C:\Program Files\MetaTrader 5")
MT5_SLAVES_DIR = Path(r"C:\MT5-Slaves")

import database as db
import notifier
import protection as prot_engine
import provisioning
import worker as worker_svc
from master_watcher import MasterWatcher
from models import (
    ConnectionStatus,
    LotSizingMode,
    MasterAccount,
    MasterStatus,
    ModifySignal,
    SlaveAccount,
    SlaveStatus,
    TradeLog,
    TradeProtection,
    TradeResult,
    TradeSignal,
    TradeType,
)
from config import settings

logger = logging.getLogger("router")

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None  # type: ignore
    MT5_AVAILABLE = False
    logger.warning("MetaTrader5 not found — SIMULATION mode active")


class MasterState:
    def __init__(self, account: MasterAccount):
        self.account       = account
        self.status        = ConnectionStatus.PENDING
        self.equity: float = 0.0
        self.balance: float = 0.0
        self.trades_today: int = 0
        self.error: Optional[str] = None
        self.last_ping: Optional[datetime] = None


class SlaveState:
    def __init__(self, account: SlaveAccount):
        self.account       = account
        self.status        = ConnectionStatus.PENDING
        self.equity: float = 0.0
        self.balance: float = 0.0
        self.open_tickets: set[int] = set()
        self.error: Optional[str] = None
        self.last_ping: Optional[datetime] = None


class CopyRouter:
    MAX_LOG = 1000

    def __init__(self):
        self.masters: Dict[str, MasterState] = {}
        self.slaves:  Dict[str, SlaveState]  = {}
        self._magic_index: Dict[int, str] = {}
        self.global_symbol_map: Dict[str, str] = {}
        self._log:       deque[TradeLog] = deque(maxlen=self.MAX_LOG)
        self._latencies: deque[float]    = deque(maxlen=500)
        self._copied_today  = 0
        self._failed_today  = 0
        self._blocked_today = 0
        self._today         = date.today()
        self._start_time    = time.time()
        # Created in startup() to ensure we're inside the event loop
        self._mt5_lock: Optional[asyncio.Lock] = None
        self._primary_master: Optional[MasterAccount] = None
        # Per-slave terminal management
        self._slave_processes:      Dict[str, subprocess.Popen] = {}
        self._slave_terminal_paths: Dict[str, str]              = {}
        self._provision_status:     Dict[str, dict]             = {}
        self._current_mt5_path:     Optional[str]               = None
        self._worker_tasks:         Dict[str, asyncio.Task]     = {}
        self._worker_stop_event:    Optional[asyncio.Event]     = None
        self._watchers:             Dict[str, MasterWatcher]    = {}

    # ── Boot / shutdown ──────────────────────────────────────────────────────

    async def startup(self):
        self._mt5_lock = asyncio.Lock()
        self._worker_stop_event = asyncio.Event()
        masters = db.load_all_masters()
        slaves  = db.load_all_slaves()
        for m in masters:
            self.masters[m.master_id] = MasterState(m)
            self._magic_index[m.magic_number] = m.master_id
        for s in slaves:
            state = SlaveState(s)
            self.slaves[s.account_id] = state
        for s_id, s_state in self.slaves.items():
            s_state.account.master_ids = db.get_masters_for_slave(s_id)
        logger.info(f"Loaded {len(self.masters)} masters, {len(self.slaves)} slaves")

        # Provision masters (copy image + launch terminal + start watcher) as background tasks.
        for ms in self.masters.values():
            asyncio.create_task(
                self._provision_and_watch(ms.account.master_id),
                name=f"provision_master:{ms.account.master_id}",
            )

        # Provision slaves as background tasks — UI polls /provision_status for progress.
        for ss in self.slaves.values():
            asyncio.create_task(self.provision_slave(ss))
            self._ensure_mt5_worker(ss.account)
            self._start_worker_task(ss.account.account_id)

        asyncio.create_task(self._heartbeat_monitor(), name="heartbeat_monitor")
        notifier.notify_bridge_started(len(self.masters), len(self.slaves))

    async def shutdown(self):
        for watcher in list(self._watchers.values()):
            watcher.stop()
        self._watchers.clear()
        if self._worker_stop_event:
            self._worker_stop_event.set()
        for task in list(self._worker_tasks.values()):
            task.cancel()
        self._worker_tasks.clear()
        notifier.notify_bridge_stopped()
        await notifier.close_client()
        if MT5_AVAILABLE:
            mt5.shutdown()

    # ── CRUD ─────────────────────────────────────────────────────────────────

    async def add_master(self, account: MasterAccount, owner_user_id: Optional[str] = None) -> dict:
        if account.magic_number in self._magic_index:
            return {"status": "duplicate_magic", "existing_master_id": self._magic_index[account.magic_number]}
        state = MasterState(account)
        self.masters[account.master_id] = state
        self._magic_index[account.magic_number] = account.master_id
        db.save_master(account, owner_user_id=owner_user_id)
        # Provision dedicated terminal + start watcher in background
        asyncio.create_task(
            self._provision_and_watch(account.master_id),
            name=f"provision_master:{account.master_id}",
        )
        self._log_event("INFO", f"Master added: {account.label} magic={account.magic_number}", master_id=account.master_id)
        return {"status": "provisioning", "master_id": account.master_id}

    def remove_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        self._stop_watcher(master_id)
        state = self.masters.pop(master_id)
        self._magic_index.pop(state.account.magic_number, None)
        db.delete_master(master_id)
        for s in self.slaves.values():
            s.account.master_ids = [m for m in s.account.master_ids if m != master_id]
        if self._primary_master and self._primary_master.master_id == master_id:
            self._primary_master = None
        return {"status": "removed", "master_id": master_id}

    async def add_slave(self, account: SlaveAccount, owner_user_id: Optional[str] = None) -> dict:
        if account.account_id in self.slaves:
            return {"status": "already_registered", "account_id": account.account_id}
        state = SlaveState(account)
        self.slaves[account.account_id] = state
        db.save_slave(account, owner_user_id=owner_user_id)
        asyncio.create_task(self.provision_slave(state))
        self._ensure_mt5_worker(account)
        self._start_worker_task(account.account_id)
        self._log_event("INFO", f"Slave added: {account.label}", account_id=account.account_id)
        return {"status": "provisioning", "account_id": account.account_id}

    def remove_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves.pop(account_id)
        self._stop_worker_task(account_id)
        db.delete_mt5_worker(self._worker_id_for_account(account_id))
        db.delete_slave(account_id)
        self.deprovision_slave(account_id)
        return {"status": "removed", "account_id": account_id}

    def update_protection(self, account_id: str, protection: TradeProtection) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves[account_id].account.protection = protection
        db.update_slave_protection(account_id, protection)
        self._log_event("INFO", f"Protection updated: profile={protection.risk_profile_label}", account_id=account_id)
        return {"status": "updated", "account_id": account_id, "protection": protection.model_dump()}

    async def update_master(self, master_id: str, data: dict) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        db.update_master(master_id, data)
        state = self.masters[master_id]
        acc = state.account
        for k, v in data.items():
            if k == "symbol_map" and hasattr(acc, "symbol_map"):
                acc.symbol_map = v
            elif hasattr(acc, k):
                setattr(acc, k, v)
        if "magic_number" in data:
            # Reindex magic
            old_magic = next((mn for mn, mid in self._magic_index.items() if mid == master_id), None)
            if old_magic is not None:
                self._magic_index.pop(old_magic, None)
            self._magic_index[acc.magic_number] = master_id
        await self._connect_master(state)
        return {"status": "updated", "master_id": master_id}

    async def update_slave(self, account_id: str, data: dict) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        db.update_slave(account_id, data)
        state = self.slaves[account_id]
        acc = state.account
        for k, v in data.items():
            if k == "lot_sizing_mode":
                acc.lot_sizing_mode = LotSizingMode(v)
            elif k == "symbol_map" and hasattr(acc, "symbol_map"):
                acc.symbol_map = v
            elif hasattr(acc, k):
                setattr(acc, k, v)
        await self._connect_slave(state)
        return {"status": "updated", "account_id": account_id}

    # ── Linking ───────────────────────────────────────────────────────────────

    def link(self, master_id: str, account_id: str) -> dict:
        if master_id not in self.masters:  return {"status": "master_not_found"}
        if account_id not in self.slaves:  return {"status": "slave_not_found"}
        current_slaves = self._eligible_slave_ids(master_id)
        if account_id not in current_slaves and len(current_slaves) >= worker_svc.MAX_SLAVES_PER_MASTER:
            return {
                "status": "limit_exceeded",
                "message": f"Maximum {worker_svc.MAX_SLAVES_PER_MASTER} slaves per master already linked",
                "current_count": len(current_slaves),
            }
        s = self.slaves[account_id]
        if master_id not in s.account.master_ids:
            s.account.master_ids.append(master_id)
            db.link_slave_to_master(account_id, master_id)
        return {"status": "linked", "master_id": master_id, "account_id": account_id}

    def unlink(self, master_id: str, account_id: str) -> dict:
        if account_id not in self.slaves:  return {"status": "slave_not_found"}
        s = self.slaves[account_id]
        s.account.master_ids = [m for m in s.account.master_ids if m != master_id]
        db.unlink_slave_from_master(account_id, master_id)
        return {"status": "unlinked"}

    def _build_trade_job_payload(
        self,
        job_type: str,
        master_state: MasterState,
        slave_state: SlaveState,
        source_payload: dict,
        slave_symbol: str,
    ) -> dict:
        return {
            "job_type": job_type,
            "master_id": master_state.account.master_id,
            "master_label": master_state.account.label,
            "slave_account_id": slave_state.account.account_id,
            "slave_label": slave_state.account.label,
            "slave_symbol": slave_symbol,
            "source": source_payload,
        }

    def _create_trade_job(
        self,
        intent_id: str,
        job_type: str,
        master_state: MasterState,
        slave_state: SlaveState,
        source_symbol: str,
        slave_symbol: str,
        source_payload: dict,
    ) -> str:
        job_id = str(uuid.uuid4())[:12]
        request = self._build_trade_job_payload(job_type, master_state, slave_state, source_payload, slave_symbol)
        db.save_trade_job(
            job_id=job_id,
            intent_id=intent_id,
            account_id=slave_state.account.account_id,
            request=request,
            master_id=master_state.account.master_id,
            job_type=job_type,
            symbol=source_symbol,
            slave_symbol=slave_symbol,
        )
        return job_id

    def _eligible_slave_ids(self, master_id: str) -> list[str]:
        return [
            s_id for s_id, ss in self.slaves.items()
            if master_id in ss.account.master_ids and ss.account.enabled
        ]

    def _worker_id_for_account(self, account_id: str) -> str:
        return f"worker:{account_id}"

    def _ensure_mt5_worker(self, account: SlaveAccount):
        db.save_mt5_worker(
            worker_id=self._worker_id_for_account(account.account_id),
            account_id=account.account_id,
            terminal_path=account.terminal_path,
            worker_role="slave",
            status="starting",
        )

    def _start_worker_task(self, account_id: str):
        worker_id = self._worker_id_for_account(account_id)
        if worker_id in self._worker_tasks and not self._worker_tasks[worker_id].done():
            return
        self._worker_tasks[worker_id] = asyncio.create_task(self._worker_loop(worker_id, account_id), name=worker_id)

    def _stop_worker_task(self, account_id: str):
        worker_id = self._worker_id_for_account(account_id)
        task = self._worker_tasks.pop(worker_id, None)
        if task:
            task.cancel()

    def _start_watcher(self, master_id: str):
        if master_id in self._watchers:
            return
        state = self.masters.get(master_id)
        if not state:
            return
        acc = state.account
        investor_pw = db.get_master_investor_password(master_id) or db.get_master_password(master_id)
        # Use the provisioned terminal path when available; None falls back to any running terminal
        default_path = "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
        terminal_path = acc.terminal_path if acc.terminal_path not in (default_path, "virtual", None, "") else None
        watcher = MasterWatcher(
            master_id=master_id,
            login=acc.login,
            investor_password=investor_pw,
            server=acc.server,
            terminal_path=terminal_path,
            router=self,
            mt5_lock=self._mt5_lock,   # share the router's lock — no MT5 singleton conflicts
        )
        watcher.start()
        self._watchers[master_id] = watcher
        logger.info(f"Watcher started for master {master_id} terminal={terminal_path}")

    def _stop_watcher(self, master_id: str):
        watcher = self._watchers.pop(master_id, None)
        if watcher:
            watcher.stop()

    async def _provision_and_watch(self, master_id: str):
        """
        Background task: copy golden image → launch terminal → start watcher.
        Idempotent: if the terminal folder already exists it is reused.
        """
        state = self.masters.get(master_id)
        if not state:
            return
        acc = state.account
        investor_pw = db.get_master_investor_password(master_id) or db.get_master_password(master_id)

        try:
            result = await provisioning.provision_account(
                account_id=master_id,
                login=acc.login,
                password=investor_pw,
                server=acc.server,
                role="master_watcher",
            )
            if result["status"] == "error":
                logger.error(f"[provision_master:{master_id}] {result.get('error')}")
                state.status = ConnectionStatus.ERROR
                state.error  = result.get("error")
                return

            terminal_path = result["terminal_path"]
            acc.terminal_path = terminal_path
            db.update_master(master_id, {"terminal_path": terminal_path})
            logger.info(f"[provision_master:{master_id}] terminal launched: {terminal_path}")

        except Exception as exc:
            logger.error(f"[provision_master:{master_id}] exception: {exc}")
            state.status = ConnectionStatus.ERROR
            state.error  = str(exc)
            return

        if acc.enabled:
            self._start_watcher(master_id)
        state.last_ping = datetime.utcnow()

    async def _worker_loop(self, worker_id: str, account_id: str):
        db.update_mt5_worker(worker_id, status="idle", last_heartbeat_at=True)
        while not (self._worker_stop_event and self._worker_stop_event.is_set()):
            try:
                job_row = db.claim_next_trade_job(worker_id, account_id)
                if not job_row:
                    db.update_mt5_worker(worker_id, status="idle", last_heartbeat_at=True)
                    await asyncio.sleep(1.0)
                    continue

                if not db.mark_trade_job_started(job_row["job_id"], worker_id):
                    continue

                db.update_mt5_worker(
                    worker_id,
                    status="busy",
                    current_job_id=job_row["job_id"],
                    clear_current_error=True,
                    last_heartbeat_at=True,
                )
                await self._process_claimed_trade_job(worker_id, job_row)

                # Reload the job to get final status, then apply retry or reconcile.
                finished_job = db.get_trade_job(job_row["job_id"])
                if finished_job:
                    await self._post_job_actions(finished_job)

                db.update_mt5_worker(
                    worker_id,
                    status="idle",
                    clear_current_job=True,
                    clear_current_error=True,
                    last_heartbeat_at=True,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                db.update_mt5_worker(
                    worker_id,
                    status="degraded",
                    current_error=str(exc),
                    last_heartbeat_at=True,
                )
                await asyncio.sleep(2.0)

        db.update_mt5_worker(worker_id, status="offline", current_job_id=None, last_heartbeat_at=True)

    async def _post_job_actions(self, job_row: dict):
        """Handle retry scheduling and reconciliation after a job finishes."""
        status = job_row.get("status", "")
        job_id = job_row["job_id"]

        if status == "confirmed":
            # Phase 6: reconcile confirmed jobs
            try:
                worker_svc.run_reconciliation(job_row)
            except Exception as exc:
                logger.error(f"Reconciliation error for job {job_id}: {exc}")
            # Update parent intent to completed if all sibling jobs are done
            self._update_intent_completion(job_row.get("intent_id"))
            return

        if status == "failed":
            attempts = int(job_row.get("attempts", 0))
            max_retries = int(job_row.get("max_retries") or 3)
            error_code = job_row.get("error_code")
            error_message = job_row.get("error_message")

            failure_class = worker_svc.classify_failure(error_code, error_message)

            if failure_class == "permanent" or attempts >= max_retries:
                db.dead_letter_job(job_id, error_code=error_code, error_message=error_message)
                self._log_event(
                    "ERROR",
                    f"Job {job_id} dead-lettered after {attempts} attempt(s): {error_message}",
                    account_id=job_row.get("account_id"),
                )
                self._update_intent_completion(job_row.get("intent_id"))
            else:
                retry_after = worker_svc.compute_retry_after_iso(attempts)
                db.schedule_job_retry(
                    job_id, retry_after,
                    error_code=error_code,
                    error_message=error_message,
                )
                self._log_event(
                    "WARN",
                    f"Job {job_id} scheduled for retry #{attempts + 1} "
                    f"(class={failure_class}) after {retry_after}",
                    account_id=job_row.get("account_id"),
                )

    def _update_intent_completion(self, intent_id: Optional[str]):
        if not intent_id:
            return
        with db.get_conn() as conn:
            rows = conn.execute(
                "SELECT status FROM trade_jobs WHERE intent_id=?", (intent_id,)
            ).fetchall()
        if not rows:
            return
        statuses = {r["status"] for r in rows}
        terminal = {"confirmed", "failed", "dead_letter", "blocked", "no_targets"}
        if not statuses.issubset(terminal):
            return  # still in progress
        if statuses <= {"confirmed"}:
            db.update_trade_intent_status(intent_id, "completed")
        else:
            db.update_trade_intent_status(intent_id, "completed_with_errors")

    async def _heartbeat_monitor(self):
        """Phase 5: Mark workers offline if heartbeat is stale for > 60 seconds."""
        while not (self._worker_stop_event and self._worker_stop_event.is_set()):
            try:
                await asyncio.sleep(30)
                cutoff = (datetime.utcnow() - timedelta(seconds=60)).isoformat()
                workers = db.load_mt5_workers()
                for w in workers:
                    if w["status"] in ("offline", "draining"):
                        continue
                    hb = w.get("last_heartbeat_at")
                    if hb and hb < cutoff:
                        db.update_mt5_worker(w["worker_id"], status="degraded",
                                             current_error="Heartbeat stale > 60s")
                        logger.warning(f"Worker {w['worker_id']} heartbeat stale — marked degraded")
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Heartbeat monitor error: {exc}")

    async def _process_claimed_trade_job(self, worker_id: str, job_row: dict):
        job_type = job_row["job_type"]
        job_id = job_row["job_id"]
        account_id = job_row["account_id"]
        slave_state = self.slaves.get(account_id)
        master_id = job_row.get("master_id")
        master_state = self.masters.get(master_id) if master_id else None
        if not slave_state or not master_state:
            db.record_trade_job_attempt(
                job_id=job_id,
                attempt_no=int(job_row.get("attempts", 0)) + 1,
                status="failed",
                error_message="Missing master or slave state",
            )
            db.mark_trade_job_finished(job_id, "failed", error_message="Missing master or slave state")
            return

        request = json.loads(job_row["request_json"])
        if job_type == "open":
            source = request.get("source", request)
            signal = TradeSignal.model_validate(source)
            await self._execute_open_trade_job(job_id, signal, master_state, slave_state, time.perf_counter())
            return

        if job_type == "close":
            source = request.get("source", request)
            magic_number = int(source["magic_number"])
            symbol = str(source["symbol"])
            await self._close_trade_job(job_id, magic_number, symbol, master_state, slave_state)
            return

        if job_type == "modify":
            source = request.get("source", request)
            modify = ModifySignal.model_validate(source)
            await self._modify_trade_job(job_id, modify, master_state, slave_state)
            return

        db.record_trade_job_attempt(
            job_id=job_id,
            attempt_no=int(job_row.get("attempts", 0)) + 1,
            status="failed",
            error_message=f"Unknown job type: {job_type}",
        )
        db.mark_trade_job_finished(job_id, "failed", error_message=f"Unknown job type: {job_type}")

    async def dispatch_open_intent(self, signal: TradeSignal) -> dict:
        self._reset_daily_counters()
        master_id = self._magic_index.get(signal.magic_number)
        if not master_id:
            db.update_trade_intent_status(signal.signal_id, "rejected", f"No master for magic={signal.magic_number}")
            self._log_event("WARN", f"dispatch_open_intent: no master for magic={signal.magic_number}")
            return {"status": "rejected", "reason": "master_not_found"}

        master_state = self.masters.get(master_id)
        if not master_state:
            db.update_trade_intent_status(signal.signal_id, "rejected", f"Master state missing for {master_id}")
            return {"status": "rejected", "reason": "master_state_missing"}

        target_ids = self._eligible_slave_ids(master_id)
        if not target_ids:
            db.update_trade_intent_status(signal.signal_id, "no_targets", "No enabled slave links")
            self._log_event("WARN", f"dispatch_open_intent: no targets for {master_state.account.label}", master_id=master_id)
            return {"status": "no_targets", "master_id": master_id, "job_ids": []}

        job_ids = []
        for s_id in target_ids:
            slave_state = self.slaves[s_id]
            slave_symbol = self._resolve_symbol(signal.symbol, master_state, slave_state)
            job_id = self._create_trade_job(
                intent_id=signal.signal_id,
                job_type="open",
                master_state=master_state,
                slave_state=slave_state,
                source_symbol=signal.symbol,
                slave_symbol=slave_symbol,
                source_payload=signal.model_dump(mode="json"),
            )
            job_ids.append(job_id)

        db.update_trade_intent_status(signal.signal_id, "accepted")
        self._log_event(
            "INFO",
            f"Dispatched open intent to {len(job_ids)} slave job(s)",
            master_id=master_id,
            signal_id=signal.signal_id,
            symbol=signal.symbol,
        )
        return {"status": "accepted", "master_id": master_id, "job_ids": job_ids, "target_count": len(job_ids)}

    async def dispatch_close_intent(self, intent_id: str, magic_number: int, symbol: str) -> dict:
        self._reset_daily_counters()
        master_id = self._magic_index.get(magic_number)
        if not master_id:
            db.update_trade_intent_status(intent_id, "rejected", f"No master for magic_number={magic_number}")
            return {"status": "rejected", "reason": "master_not_found"}

        master_state = self.masters.get(master_id)
        if not master_state:
            db.update_trade_intent_status(intent_id, "rejected", f"Master state missing for {master_id}")
            return {"status": "rejected", "reason": "master_state_missing"}

        target_ids = self._eligible_slave_ids(master_id)
        if not target_ids:
            db.update_trade_intent_status(intent_id, "no_targets", "No enabled slave links")
            return {"status": "no_targets", "master_id": master_id, "job_ids": []}

        job_ids = []
        for s_id in target_ids:
            slave_state = self.slaves[s_id]
            slave_symbol = self._resolve_symbol(symbol, master_state, slave_state)
            job_id = self._create_trade_job(
                intent_id=intent_id,
                job_type="close",
                master_state=master_state,
                slave_state=slave_state,
                source_symbol=symbol,
                slave_symbol=slave_symbol,
                source_payload={"magic_number": magic_number, "symbol": symbol},
            )
            job_ids.append(job_id)

        db.update_trade_intent_status(intent_id, "accepted")
        self._log_event(
            "INFO",
            f"Dispatched close intent to {len(job_ids)} slave job(s)",
            master_id=master_id,
            signal_id=intent_id,
            symbol=symbol,
        )
        return {"status": "accepted", "master_id": master_id, "job_ids": job_ids, "target_count": len(job_ids)}

    async def dispatch_modify_intent(self, intent_id: str, modify: ModifySignal) -> dict:
        self._reset_daily_counters()
        master_id = self._magic_index.get(modify.magic_number)
        if not master_id:
            db.update_trade_intent_status(intent_id, "rejected", f"No master for magic_number={modify.magic_number}")
            return {"status": "rejected", "reason": "master_not_found"}

        master_state = self.masters.get(master_id)
        if not master_state:
            db.update_trade_intent_status(intent_id, "rejected", f"Master state missing for {master_id}")
            return {"status": "rejected", "reason": "master_state_missing"}

        target_ids = self._eligible_slave_ids(master_id)
        if not target_ids:
            db.update_trade_intent_status(intent_id, "no_targets", "No enabled slave links")
            return {"status": "no_targets", "master_id": master_id, "job_ids": []}

        job_ids = []
        for s_id in target_ids:
            slave_state = self.slaves[s_id]
            slave_symbol = self._resolve_symbol(modify.symbol, master_state, slave_state)
            job_id = self._create_trade_job(
                intent_id=intent_id,
                job_type="modify",
                master_state=master_state,
                slave_state=slave_state,
                source_symbol=modify.symbol,
                slave_symbol=slave_symbol,
                source_payload=modify.model_dump(mode="json"),
            )
            job_ids.append(job_id)

        db.update_trade_intent_status(intent_id, "accepted")
        self._log_event(
            "INFO",
            f"Dispatched modify intent to {len(job_ids)} slave job(s)",
            master_id=master_id,
            signal_id=intent_id,
            symbol=modify.symbol,
        )
        return {"status": "accepted", "master_id": master_id, "job_ids": job_ids, "target_count": len(job_ids)}

    async def _record_trade_job_result(
        self,
        job_id: str,
        job_type: str,
        result: dict,
        success: bool,
        status: str,
        latency_ms: float,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ):
        job_row = db.get_trade_job(job_id) or {}
        attempt_no = int(job_row.get("attempts", 0)) + 1
        db.update_trade_job_status(
            job_id,
            status,
            broker_ticket=result.get("order_ticket") if result else None,
            fill_price=result.get("price") if result else None,
            error_code=error_code,
            error_message=error_message,
        )
        db.record_trade_job_attempt(
            job_id=job_id,
            attempt_no=attempt_no,
            status=status,
            request=result.get("request") if result else None,
            response=result,
            error_code=error_code,
            error_message=error_message,
            latency_ms=latency_ms,
        )

    # ── Signal routing (queue-only — execution handled by worker loop) ──────────

    async def route_signal(self, signal: TradeSignal, t0: float) -> dict:
        """Phase 3/9: Enqueue signal into the job queue; do NOT execute inline.
        The worker loop will pick it up within the next polling cycle (~1s)."""
        self._reset_daily_counters()
        master_id = self._magic_index.get(signal.magic_number)
        if master_id:
            master_state = self.masters.get(master_id)
            if master_state and signal.master_equity:
                master_state.equity = signal.master_equity

        db.save_trade_intent(
            intent_id=signal.signal_id,
            idempotency_key=signal.signal_id,
            intent_type="open",
            payload=signal.model_dump(mode="json"),
            master_id=master_id,
            magic_number=signal.magic_number,
            symbol=signal.symbol,
            direction=signal.type.value,
            status="queued" if master_id else "rejected",
        )
        return await self.dispatch_open_intent(signal)

    async def route_close(self, magic_number: int, symbol: str) -> dict:
        """Phase 3/9: Enqueue close into the job queue; do NOT execute inline."""
        self._reset_daily_counters()
        intent_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"close|{magic_number}|{symbol}|{time.time()}"))
        master_id = self._magic_index.get(magic_number)
        db.save_trade_intent(
            intent_id=intent_id,
            idempotency_key=intent_id,
            intent_type="close",
            payload={"magic_number": magic_number, "symbol": symbol},
            master_id=master_id,
            magic_number=magic_number,
            symbol=symbol,
            direction="close",
            status="queued" if master_id else "rejected",
        )
        return await self.dispatch_close_intent(intent_id, magic_number, symbol)

    async def route_modify(self, intent_id: str, modify: ModifySignal) -> dict:
        """Enqueue a modify into the job queue."""
        self._reset_daily_counters()
        return await self.dispatch_modify_intent(intent_id, modify)

    # ── Execution ─────────────────────────────────────────────────────────────

    async def _execute_open_trade_job(
        self, job_id: str, signal: TradeSignal, master_state: MasterState, slave_state: SlaveState, t0: float
    ) -> TradeResult:
        result = await self._execute_on_slave(signal, master_state, slave_state, t0)
        status = "confirmed" if result.success else ("blocked" if result.slippage_blocked else "failed")
        await self._record_trade_job_result(
            job_id=job_id,
            job_type="open",
            result=result.model_dump(mode="json"),
            success=result.success,
            status=status,
            latency_ms=result.latency_ms,
            error_code=str(result.error_code) if result.error_code is not None else None,
            error_message=result.error_message,
        )
        return result

    async def _close_trade_job(
        self, job_id: str, magic_number: int, symbol: str, master_state: MasterState, state: SlaveState
    ):
        t0 = time.perf_counter()
        request = {"magic_number": magic_number, "symbol": symbol}
        job_row = db.get_trade_job(job_id) or {}
        attempt_no = int(job_row.get("attempts", 0)) + 1
        try:
            await self._close_on_slave(magic_number, symbol, master_state, state)
            latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            db.record_trade_job_attempt(
                job_id=job_id,
                attempt_no=attempt_no,
                status="confirmed",
                request=request,
                response={"status": "ok"},
                latency_ms=latency_ms,
            )
            db.update_trade_job_status(job_id, "confirmed")
        except Exception as exc:
            latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            db.record_trade_job_attempt(
                job_id=job_id,
                attempt_no=attempt_no,
                status="failed",
                request=request,
                error_message=str(exc),
                latency_ms=latency_ms,
            )
            db.update_trade_job_status(job_id, "failed", error_message=str(exc))
            raise

    async def _modify_trade_job(
        self, job_id: str, modify: ModifySignal, master_state: MasterState, slave_state: SlaveState
    ):
        t0 = time.perf_counter()
        request = modify.model_dump(mode="json")
        job_row = db.get_trade_job(job_id) or {}
        attempt_no = int(job_row.get("attempts", 0)) + 1
        try:
            await self._modify_sltp_on_slave(modify, master_state, slave_state)
            latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            db.record_trade_job_attempt(
                job_id=job_id,
                attempt_no=attempt_no,
                status="confirmed",
                request=request,
                response={"status": "ok"},
                latency_ms=latency_ms,
            )
            db.update_trade_job_status(job_id, "confirmed")
        except Exception as exc:
            latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            db.record_trade_job_attempt(
                job_id=job_id,
                attempt_no=attempt_no,
                status="failed",
                request=request,
                error_message=str(exc),
                latency_ms=latency_ms,
            )
            db.update_trade_job_status(job_id, "failed", error_message=str(exc))
            raise

    async def _execute_on_slave(
        self, signal: TradeSignal, master_state: MasterState, slave_state: SlaveState, t0: float
    ) -> TradeResult:
        acc        = slave_state.account
        protection = acc.protection
        slave_symbol = self._resolve_symbol(signal.symbol, master_state, slave_state)

        result = TradeResult(
            account_id=acc.account_id, master_id=master_state.account.master_id,
            signal_id=signal.signal_id, symbol=signal.symbol, slave_symbol=slave_symbol,
            trade_type=signal.type.value, requested_volume=signal.volume,
            executed_volume=0.0, price=signal.price, success=False,
            slippage_checked=False, slippage_deviation=0.0, slippage_blocked=False,
            latency_ms=0,
        )

        try:
            # Phase 8: hard limit — max open trades per slave
            open_count = len(slave_state.open_tickets)
            if open_count >= acc.max_open_trades:
                result.slippage_blocked = True
                result.error_message = (
                    f"Max open trades limit reached: {open_count}/{acc.max_open_trades}"
                )
                return result

            if not MT5_AVAILABLE:
                await asyncio.sleep(0.004)
                current_price = signal.price

                slip_result = prot_engine.check_slippage(
                    master_price=signal.price, current_price=current_price,
                    symbol=slave_symbol, trade_type=signal.type, protection=protection,
                )
                result.slippage_checked = True
                result.slippage_deviation = slip_result.deviation

                if not slip_result.passed:
                    result.slippage_blocked = True
                    result.error_message = slip_result.message
                else:
                    base_volume = self._calculate_volume(signal, master_state, slave_state)
                    volume      = prot_engine.scale_lot(base_volume, protection)
                    slave_sl, slave_tp = prot_engine.calculate_slave_sltp(
                        master_price=signal.price, master_sl=signal.sl, master_tp=signal.tp,
                        slave_price=current_price, symbol=slave_symbol,
                        trade_type=signal.type, protection=protection,
                    )
                    if not protection.sltp_sync_enabled:
                        slave_sl, slave_tp = signal.sl, signal.tp

                    result.executed_volume = volume
                    result.lot_after_risk  = volume
                    result.sl_synced       = slave_sl
                    result.tp_synced       = slave_tp
                    result.success         = True
                    result.order_ticket    = 100000 + int(time.time() * 1000) % 99999
                    result.price           = current_price
                    slave_state.open_tickets.add(result.order_ticket)
                    db.record_slave_position(
                        acc.account_id, master_state.account.master_id, signal.magic_number,
                        slave_symbol, result.order_ticket, current_price, signal.type.value,
                    )
            else:
                lock = self._mt5_lock or asyncio.Lock()
                async with lock:
                    if not self._ensure_slave_account(acc):
                        result.error_message = f"MT5 account switch failed: {mt5.last_error()}"
                        return result
                    info = mt5.account_info()
                    if info:
                        slave_state.equity  = info.equity
                        slave_state.balance = info.balance

                    current_price = self._get_current_price(slave_symbol, signal.type) or signal.price

                    slip_result = prot_engine.check_slippage(
                        master_price=signal.price, current_price=current_price,
                        symbol=slave_symbol, trade_type=signal.type, protection=protection,
                    )
                    result.slippage_checked  = True
                    result.slippage_deviation = slip_result.deviation

                    if not slip_result.passed:
                        result.slippage_blocked = True
                        result.error_message    = slip_result.message
                    else:
                        base_volume = self._calculate_volume(signal, master_state, slave_state)
                        volume      = prot_engine.scale_lot(base_volume, protection)
                        slave_sl, slave_tp = prot_engine.calculate_slave_sltp(
                            master_price=signal.price, master_sl=signal.sl, master_tp=signal.tp,
                            slave_price=current_price, symbol=slave_symbol,
                            trade_type=signal.type, protection=protection,
                        )
                        if not protection.sltp_sync_enabled:
                            slave_sl, slave_tp = signal.sl, signal.tp

                        result.executed_volume = volume
                        result.lot_after_risk  = volume
                        result.sl_synced       = slave_sl
                        result.tp_synced       = slave_tp

                        slippage   = acc.slippage_override or signal.slippage
                        order_type = self._map_order_type(signal.type)
                        req = {
                            "action":        mt5.TRADE_ACTION_DEAL,
                            "symbol":        slave_symbol,
                            "volume":        volume,
                            "type":          order_type,
                            "price":         current_price,
                            "sl":            slave_sl,
                            "tp":            slave_tp,
                            "deviation":     slippage,
                            "magic":         signal.magic_number,
                            "comment":       signal.comment,
                            "type_time":     mt5.ORDER_TIME_GTC,
                            "type_filling":  mt5.ORDER_FILLING_IOC,
                        }
                        r = mt5.order_send(req)
                        if r is None:
                            result.error_message = f"order_send None: {mt5.last_error()}"
                        elif r.retcode == mt5.TRADE_RETCODE_DONE:
                            result.success      = True
                            result.order_ticket = r.order
                            result.price        = r.price
                            slave_state.open_tickets.add(r.order)
                            db.record_slave_position(
                                acc.account_id, master_state.account.master_id, signal.magic_number,
                                slave_symbol, r.order, r.price, signal.type.value,
                            )
                        else:
                            result.error_code    = r.retcode
                            result.error_message = _decode_retcode(r.retcode)

        except Exception as exc:
            result.error_message = str(exc)

        finally:
            ms = round((time.perf_counter() - t0) * 1000, 2)
            result.latency_ms = ms
            self._latencies.append(ms)

            sl  = result.sl_synced  or 0.0
            tp  = result.tp_synced  or 0.0
            vol = result.executed_volume or 0.0
            dev = result.slippage_deviation or 0.0

            if result.slippage_blocked:
                self._blocked_today += 1
                self._log_event(
                    "WARN",
                    f"🛡 SLIPPAGE BLOCKED [{acc.label}] {slave_symbol}: {result.error_message}",
                    master_id=master_state.account.master_id,
                    account_id=acc.account_id, signal_id=signal.signal_id, symbol=slave_symbol,
                )
                notifier.notify_trade_failed(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol, trade_type=signal.type.value,
                    error_message=result.error_message or "Unknown", error_code=None,
                    signal_id=signal.signal_id,
                )
            elif result.success:
                self._copied_today += 1
                self._log_event(
                    "INFO",
                    f"✅ {signal.type.upper()} {vol}L {slave_symbol} @ {result.price:.5f} "
                    f"SL={sl:.5f} TP={tp:.5f} "
                    f"slip={dev:.1f}{protection.slippage_mode.value[0]} "
                    f"risk×{protection.risk_multiplier} [{ms:.0f}ms]",
                    master_id=master_state.account.master_id,
                    account_id=acc.account_id, signal_id=signal.signal_id,
                    symbol=slave_symbol, latency_ms=ms,
                )
                notifier.notify_trade_copied(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol,
                    trade_type=signal.type.value, volume=vol, price=result.price,
                    ticket=result.order_ticket, latency_ms=ms, signal_id=signal.signal_id,
                )
            else:
                self._failed_today += 1
                self._log_event(
                    "ERROR",
                    f"❌ {result.error_message} (code {result.error_code})",
                    master_id=master_state.account.master_id, account_id=acc.account_id,
                    signal_id=signal.signal_id, symbol=slave_symbol, latency_ms=ms,
                )
                notifier.notify_trade_failed(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol, trade_type=signal.type.value,
                    error_message=result.error_message or "Unknown", error_code=result.error_code,
                    signal_id=signal.signal_id,
                )

        return result

    # ── SL/TP Modify ──────────────────────────────────────────────────────────

    async def _modify_sltp_on_slave(self, modify: ModifySignal, master_state: MasterState, slave_state: SlaveState):
        acc        = slave_state.account
        protection = acc.protection
        slave_symbol = self._resolve_symbol(modify.symbol, master_state, slave_state)

        if not protection.sltp_sync_enabled:
            return

        tickets = db.get_slave_tickets(acc.account_id, modify.magic_number, slave_symbol)
        if not tickets:
            self._log_event("WARN", f"SL/TP modify: no tracked positions for {slave_symbol} magic={modify.magic_number}", account_id=acc.account_id)
            return

        if not MT5_AVAILABLE:
            for pos_info in tickets:
                ticket     = pos_info["ticket"]
                open_price = pos_info["open_price"]
                trade_type = TradeType(pos_info["trade_type"])
                new_sl, new_tp = prot_engine.calculate_modify_sltp(
                    master_sl=modify.new_sl, master_tp=modify.new_tp,
                    slave_entry=open_price, master_entry=modify.master_price or open_price,
                    symbol=slave_symbol, trade_type=trade_type, protection=protection,
                )
                logger.info(f"[SIM] Modify #{ticket} {slave_symbol}: SL={new_sl:.5f} TP={new_tp:.5f}")
                db.log_modify(acc.account_id, ticket, slave_symbol, 0.0, 0.0, new_sl, new_tp, True)
                self._log_event("INFO",
                    f"🔄 SL/TP synced #{ticket} {slave_symbol} SL={new_sl:.5f} TP={new_tp:.5f}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)
            return

        lock = self._mt5_lock or asyncio.Lock()
        async with lock:
            if not self._ensure_slave_account(acc):
                self._log_event("ERROR", f"SL/TP modify: account switch failed", account_id=acc.account_id)
                return
            for pos_info in tickets:
                ticket     = pos_info["ticket"]
                open_price = pos_info["open_price"]
                trade_type = TradeType(pos_info["trade_type"])

                new_sl, new_tp = prot_engine.calculate_modify_sltp(
                    master_sl=modify.new_sl, master_tp=modify.new_tp,
                    slave_entry=open_price, master_entry=modify.master_price or open_price,
                    symbol=slave_symbol, trade_type=trade_type, protection=protection,
                )

                request = {
                    "action":   mt5.TRADE_ACTION_SLTP,
                    "symbol":   slave_symbol,
                    "sl":       new_sl,
                    "tp":       new_tp,
                    "position": ticket,
                }
                r = mt5.order_send(request)
                if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                    db.log_modify(acc.account_id, ticket, slave_symbol, 0.0, 0.0, new_sl, new_tp, True)
                    self._log_event("INFO",
                        f"🔄 SL/TP synced #{ticket} {slave_symbol} SL={new_sl:.5f} TP={new_tp:.5f}",
                        master_id=master_state.account.master_id, account_id=acc.account_id)
                else:
                    err = _decode_retcode(r.retcode if r else -1)
                    db.log_modify(acc.account_id, ticket, slave_symbol, 0.0, 0.0, new_sl, new_tp, False, err)
                    self._log_event("ERROR",
                        f"SL/TP modify failed #{ticket}: {err}",
                        master_id=master_state.account.master_id, account_id=acc.account_id)

    # ── Close ─────────────────────────────────────────────────────────────────

    async def _close_on_slave(self, magic_number: int, symbol: str, master_state: MasterState, state: SlaveState):
        slave_symbol = self._resolve_symbol(symbol, master_state, state)

        if not MT5_AVAILABLE:
            tickets = db.get_slave_tickets(state.account.account_id, magic_number, slave_symbol)
            for t in tickets:
                db.remove_slave_position(state.account.account_id, t["ticket"])
            self._log_event("INFO", f"[SIM] Close {slave_symbol} magic={magic_number}", account_id=state.account.account_id)
            return

        acc  = state.account
        lock = self._mt5_lock or asyncio.Lock()
        async with lock:
            if not self._ensure_slave_account(acc):
                self._log_event("ERROR", f"Close: account switch failed", account_id=acc.account_id)
                return
            positions = mt5.positions_get(symbol=slave_symbol) or []
            for pos in positions:
                if pos.magic != magic_number:
                    continue
                close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                tick = mt5.symbol_info_tick(slave_symbol)
                req = {
                    "action":       mt5.TRADE_ACTION_DEAL,
                    "symbol":       slave_symbol,
                    "volume":       pos.volume,
                    "type":         close_type,
                    "position":     pos.ticket,
                    "price":        tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask,
                    "deviation":    10,
                    "magic":        magic_number,
                    "comment":      "OmniClose",
                    "type_time":    mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
                r = mt5.order_send(req)
                if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                    state.open_tickets.discard(pos.ticket)
                    db.remove_slave_position(acc.account_id, pos.ticket)
                    self._log_event("INFO", f"Closed #{pos.ticket}", account_id=acc.account_id)

    # ── Terminal connections ──────────────────────────────────────────────────

    async def _connect_master(self, state: MasterState):
        acc = state.account
        if not acc.enabled:
            state.status = ConnectionStatus.DISCONNECTED; return
        if not MT5_AVAILABLE or acc.terminal_path == "virtual":
            state.status = ConnectionStatus.CONNECTED
            state.equity = 50_000.0; state.balance = 50_000.0
            state.last_ping = datetime.utcnow()
            notifier.notify_master_connected(acc.label, acc.magic_number, acc.server, state.equity)
            return
        # Real master is watched by MasterWatcher (started separately).
        # Mark connected here; equity is updated from watcher poll data.
        state.status = ConnectionStatus.CONNECTED
        state.last_ping = datetime.utcnow()
        self._primary_master = acc
        notifier.notify_master_connected(acc.label, acc.magic_number, acc.server, state.equity)

    async def _connect_slave(self, state: SlaveState):
        """Thin wrapper — delegates entirely to provision_slave()."""
        if not state.account.enabled:
            state.status = ConnectionStatus.DISCONNECTED
            return
        await self.provision_slave(state)

    # ── Terminal provisioning ─────────────────────────────────────────────────

    async def provision_slave(self, state: SlaveState) -> dict:
        """
        Background task: copy golden image → launch terminal → connect via shared lock.
        Fully automatic — user never needs to touch MT5 manually.
        """
        acc  = state.account
        aid  = acc.account_id
        loop = asyncio.get_event_loop()

        def _set(step, msg, done=False, error=None):
            self._provision_status[aid] = {"step": step, "message": msg, "done": done, "error": error}

        try:
            if not MT5_AVAILABLE:
                state.equity, state.balance = 10_000.0, 10_000.0
                state.status    = ConnectionStatus.CONNECTED
                state.last_ping = datetime.utcnow()
                _set(5, "Connected (simulation)", done=True)
                notifier.notify_slave_connected(acc.label, aid, acc.server, state.equity)
                return {"status": "connected", "equity": state.equity}

            # ── Step 1-2: copy golden image + launch portable terminal ─────────
            _set(1, "Copying MT5 terminal image…")
            result = await provisioning.provision_account(
                account_id=aid,
                login=acc.login,
                password=db.get_slave_password(aid) or acc.password,
                server=acc.server,
                role="slave",
            )
            if result["status"] == "error":
                raise RuntimeError(result.get("error", "Provisioning failed"))

            terminal_path = result["terminal_path"]
            acc.terminal_path = terminal_path
            db.update_slave(aid, {"terminal_path": terminal_path})
            logger.info(f"[provision_slave:{aid}] terminal launched: {terminal_path}")

            # ── Step 3: wait for IPC ready (retry under lock, up to ~120 s) ───
            _pwd = db.get_slave_password(aid) or acc.password
            lock = self._mt5_lock or asyncio.Lock()
            connected = False
            for attempt in range(1, 25):          # 24 × 5 s = 120 s max
                _set(3, f"Waiting for MT5 terminal to be ready… ({attempt}/24)")
                async with lock:
                    ok = await loop.run_in_executor(None, lambda: mt5.initialize(
                        path=terminal_path, login=acc.login,
                        password=_pwd, server=acc.server, timeout=8_000,
                    ))
                    if ok:
                        info = mt5.account_info()
                        if info and info.login == acc.login:
                            state.equity  = info.equity
                            state.balance = info.balance
                            connected = True
                            break
                await asyncio.sleep(5)

            if not connected:
                raise RuntimeError(
                    f"MT5 terminal did not become ready after 120 s "
                    f"(login={acc.login} server={acc.server})"
                )

            state.status    = ConnectionStatus.CONNECTED
            state.last_ping = datetime.utcnow()
            _set(5, "Connected", done=True)
            notifier.notify_slave_connected(acc.label, aid, acc.server, state.equity)
            return {"status": "connected", "equity": state.equity}

        except Exception as exc:
            err = str(exc)
            logger.error(f"provision_slave [{acc.label}]: {err}")
            state.status = ConnectionStatus.ERROR
            state.error  = err
            _set(0, err, error=err)
            notifier.notify_slave_error(acc.label, aid, err)
            return {"status": "error", "error": err}

    def deprovision_slave(self, account_id: str):
        """Clean up in-memory state for a removed slave."""
        self._slave_processes.pop(account_id, None)
        self._slave_terminal_paths.pop(account_id, None)
        self._provision_status.pop(account_id, None)

    def get_provision_status(self, account_id: str) -> dict:
        return self._provision_status.get(account_id, {"step": 0, "message": "Unknown", "done": False, "error": None})

    # ── Terminal / account management (MT5 is a process-wide singleton) ─────────

    def _ensure_slave_account(self, acc) -> bool:
        """
        Guarantee MT5 IPC is pointing at this slave's dedicated terminal.
        Caller must hold _mt5_lock.  With per-account terminals, no login
        switching is needed — just switch the IPC path.
        """
        try:
            info = mt5.account_info()
            if info and info.login == acc.login:
                return True   # already on the right terminal
        except Exception:
            pass

        default_path = "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
        terminal = acc.terminal_path

        if terminal and terminal not in (default_path, "virtual", "", None):
            # Switch IPC to this slave's dedicated portable terminal
            return self._switch_terminal(terminal)

        # Fallback for accounts not yet provisioned (should not normally happen)
        _pwd = db.get_slave_password(acc.account_id) or acc.password
        if not mt5.terminal_info():
            if not mt5.initialize(timeout=60_000):
                return False
        return bool(mt5.login(acc.login, password=_pwd, server=acc.server))

    def _switch_terminal(self, terminal_path: str) -> bool:
        """Switch MT5 IPC connection to a specific terminal. Caller must hold _mt5_lock."""
        if self._current_mt5_path == terminal_path and MT5_AVAILABLE and mt5.terminal_info():
            return True
        if MT5_AVAILABLE:
            mt5.shutdown()
            ok = mt5.initialize(path=terminal_path, timeout=180_000)
            if ok:
                self._current_mt5_path = terminal_path
            return ok
        return False

    # ── Ping / Reconnect ──────────────────────────────────────────────────────

    async def ping_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters[master_id]
        state.last_ping = datetime.utcnow()
        return {
            "status": state.status.value,
            "equity": state.equity,
            "last_ping": state.last_ping.isoformat(),
        }

    async def reconnect_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters[master_id]
        state.status = ConnectionStatus.PENDING
        state.error  = None
        await self._connect_master(state)
        return {"status": state.status.value, "master_id": master_id}

    async def reconnect_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        state = self.slaves[account_id]
        state.status = ConnectionStatus.PENDING
        state.error  = None
        # Use provision_slave so a dedicated terminal is (re)created if needed
        asyncio.create_task(self.provision_slave(state))
        return {"status": "provisioning", "account_id": account_id}

    # ── (legacy stub, not called) ─────────────────────────────────────────────

    def _restore_master_login(self):
        pass

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_current_price(self, symbol: str, trade_type: TradeType) -> Optional[float]:
        if not MT5_AVAILABLE:
            return None
        tick = mt5.symbol_info_tick(symbol)
        if not tick:
            return None
        return tick.ask if trade_type in (TradeType.BUY, TradeType.BUY_LIMIT, TradeType.BUY_STOP) else tick.bid

    def _resolve_symbol(self, symbol: str, master_state: MasterState, slave_state: SlaveState) -> str:
        resolved = self.global_symbol_map.get(symbol, symbol)
        resolved = master_state.account.symbol_map.get(resolved, resolved)
        resolved = slave_state.account.symbol_map.get(resolved, resolved)
        return resolved

    def _calculate_volume(self, signal: TradeSignal, master_state: MasterState, slave_state: SlaveState) -> float:
        acc  = slave_state.account
        mode = acc.lot_sizing_mode
        if mode == LotSizingMode.FIXED:
            volume = acc.fixed_lot
        elif mode == LotSizingMode.MULTIPLIER:
            volume = round(signal.volume * acc.multiplier, 2)
        else:
            master_eq = master_state.equity or settings.master_equity
            volume = round(signal.volume * (slave_state.equity / master_eq), 2) if master_eq > 0 else acc.min_lot
        return round(max(acc.min_lot, min(acc.max_lot, volume)), 2)

    def get_master_statuses(self) -> list[MasterStatus]:
        out = []
        for mid, ms in self.masters.items():
            linked = [s_id for s_id, ss in self.slaves.items() if mid in ss.account.master_ids]
            out.append(MasterStatus(
                master_id=mid, label=ms.account.label, magic_number=ms.account.magic_number,
                connection_status=ms.status, equity=ms.equity, balance=ms.balance,
                linked_slaves=linked, trades_today=ms.trades_today, error=ms.error, last_ping=ms.last_ping,
            ))
        return out

    def get_slave_statuses(self) -> list[SlaveStatus]:
        return [
            SlaveStatus(
                account_id=s_id, label=ss.account.label, server=ss.account.server,
                connection_status=ss.status, equity=ss.equity, balance=ss.balance,
                master_ids=ss.account.master_ids, open_trades=len(ss.open_tickets),
                lot_sizing_mode=ss.account.lot_sizing_mode.value,
                protection=ss.account.protection.model_dump(),
                error=ss.error, last_ping=ss.last_ping,
            )
            for s_id, ss in self.slaves.items()
        ]

    def get_full_status(self) -> dict:
        lats = list(self._latencies)
        avg  = round(sum(lats) / len(lats), 2) if lats else 0.0
        masters = self.get_master_statuses()
        slaves  = self.get_slave_statuses()
        return {
            "online": True,
            "masters_total":       len(self.masters),
            "masters_connected":   sum(1 for m in masters if m.connection_status == ConnectionStatus.CONNECTED),
            "slaves_total":        len(self.slaves),
            "slaves_connected":    sum(1 for s in slaves  if s.connection_status == ConnectionStatus.CONNECTED),
            "trades_copied_today": self._copied_today,
            "trades_failed_today": self._failed_today,
            "trades_blocked_today": self._blocked_today,
            "avg_latency_ms":      avg,
            "uptime_seconds":      round(time.time() - self._start_time, 1),
            "timestamp":           datetime.utcnow().isoformat(),
            "masters":             [m.model_dump(mode="json") for m in masters],
            "slaves":              [s.model_dump(mode="json") for s in slaves],
            "telegram_enabled":    settings.telegram_enabled,
        }

    def get_recent_logs(self, limit: int = 200) -> list[dict]:
        return [e.model_dump(mode="json") for e in list(self._log)[-limit:]]

    def _log_event(self, level, message, master_id=None, account_id=None, signal_id=None, symbol=None, latency_ms=None):
        entry = TradeLog(level=level, message=message, master_id=master_id, account_id=account_id, signal_id=signal_id, symbol=symbol, latency_ms=latency_ms)
        self._log.append(entry)
        fn = logger.error if level == "ERROR" else logger.warning if level == "WARN" else logger.info
        fn(f"[{master_id or account_id or 'BRIDGE'}] {message}")

    def _reset_daily_counters(self):
        today = date.today()
        if today != self._today:
            self._copied_today = self._failed_today = self._blocked_today = 0
            for ms in self.masters.values():
                ms.trades_today = 0
            self._today = today

    @staticmethod
    def _map_order_type(trade_type: TradeType):
        if not MT5_AVAILABLE:
            return None
        return {
            TradeType.BUY: mt5.ORDER_TYPE_BUY, TradeType.SELL: mt5.ORDER_TYPE_SELL,
            TradeType.BUY_LIMIT: mt5.ORDER_TYPE_BUY_LIMIT, TradeType.SELL_LIMIT: mt5.ORDER_TYPE_SELL_LIMIT,
            TradeType.BUY_STOP: mt5.ORDER_TYPE_BUY_STOP, TradeType.SELL_STOP: mt5.ORDER_TYPE_SELL_STOP,
        }.get(trade_type, mt5.ORDER_TYPE_BUY)


def _decode_retcode(retcode: int) -> str:
    return {
        10004:"Requote", 10006:"Rejected", 10013:"Invalid request",
        10014:"Invalid volume", 10015:"Invalid price", 10016:"Invalid stops",
        10017:"Trade disabled", 10018:"Market closed", 10019:"Insufficient funds",
        10020:"Prices changed", 10021:"No quotes", 10024:"Too many requests",
        10026:"AutoTrading disabled (server)", 10027:"AutoTrading disabled (client)",
        10031:"No connection", 10033:"Pending orders limit", 10034:"Volume limit",
    }.get(retcode, f"Error {retcode}")
