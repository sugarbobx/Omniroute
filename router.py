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
import logging
import shutil
import subprocess
import time
from collections import deque
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

MT5_BASE_PATH  = Path(r"C:\Program Files\MetaTrader 5")
MT5_SLAVES_DIR = Path(r"C:\MT5-Slaves")

import database as db
import notifier
import protection as prot_engine
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

    # ── Boot / shutdown ──────────────────────────────────────────────────────

    async def startup(self):
        self._mt5_lock = asyncio.Lock()
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

        # Probe MT5 at startup (non-blocking, pathless — just log whether a terminal is up).
        if MT5_AVAILABLE:
            ok = mt5.initialize(timeout=10_000)
            if ok:
                logger.info("MT5 IPC available at startup")
                mt5.shutdown()
            else:
                logger.info("No MT5 terminal running at startup — slaves will connect via provision_slave")

        for ms in self.masters.values():
            await self._connect_master(ms)

        # Connect slaves as background tasks so the server starts immediately.
        # UI polls /slaves/{id}/provision_status for live progress.
        for ss in self.slaves.values():
            asyncio.create_task(self.provision_slave(ss))

        notifier.notify_bridge_started(len(self.masters), len(self.slaves))

    async def shutdown(self):
        notifier.notify_bridge_stopped()
        await notifier.close_client()
        if MT5_AVAILABLE:
            mt5.shutdown()

    # ── CRUD ─────────────────────────────────────────────────────────────────

    async def add_master(self, account: MasterAccount) -> dict:
        if account.magic_number in self._magic_index:
            return {"status": "duplicate_magic", "existing_master_id": self._magic_index[account.magic_number]}
        state = MasterState(account)
        self.masters[account.master_id] = state
        self._magic_index[account.magic_number] = account.master_id
        db.save_master(account)
        await self._connect_master(state)
        self._log_event("INFO", f"Master added: {account.label} magic={account.magic_number}", master_id=account.master_id)
        return {"status": "added", "master_id": account.master_id, "connected": state.status == ConnectionStatus.CONNECTED}

    def remove_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters.pop(master_id)
        self._magic_index.pop(state.account.magic_number, None)
        db.delete_master(master_id)
        for s in self.slaves.values():
            s.account.master_ids = [m for m in s.account.master_ids if m != master_id]
        if self._primary_master and self._primary_master.master_id == master_id:
            self._primary_master = None
        return {"status": "removed", "master_id": master_id}

    async def add_slave(self, account: SlaveAccount) -> dict:
        if account.account_id in self.slaves:
            return {"status": "already_registered", "account_id": account.account_id}
        state = SlaveState(account)
        self.slaves[account.account_id] = state
        db.save_slave(account)
        # Provision asynchronously so the HTTP response returns immediately;
        # the caller can poll /slaves/{id} or /slaves/{id}/provision_status for progress.
        asyncio.create_task(self.provision_slave(state))
        self._log_event("INFO", f"Slave added: {account.label}", account_id=account.account_id)
        return {"status": "provisioning", "account_id": account.account_id}

    def remove_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves.pop(account_id)
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

    # ── Signal routing ────────────────────────────────────────────────────────

    async def route_signal(self, signal: TradeSignal, t0: float):
        self._reset_daily_counters()
        master_id = self._magic_index.get(signal.magic_number)
        if not master_id:
            self._log_event("WARN", f"No master for magic={signal.magic_number}")
            return
        master_state = self.masters.get(master_id)
        if not master_state:
            return
        if signal.master_equity:
            master_state.equity = signal.master_equity

        linked_ids = [
            s_id for s_id, ss in self.slaves.items()
            if master_id in ss.account.master_ids
            and ss.account.enabled
            and ss.status == ConnectionStatus.CONNECTED
        ]
        if not linked_ids:
            self._log_event("WARN", f"No connected slaves for master {master_state.account.label}", master_id=master_id)
            return

        notifier.notify_trade_detected(
            master_label=master_state.account.label, magic_number=signal.magic_number,
            symbol=signal.symbol, trade_type=signal.type.value, volume=signal.volume,
            price=signal.price, sl=signal.sl, tp=signal.tp, signal_id=signal.signal_id,
        )

        tasks = [self._execute_on_slave(signal, master_state, self.slaves[s_id], t0) for s_id in linked_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        master_state.trades_today += 1

    async def route_close(self, magic_number: int, symbol: str):
        master_id = self._magic_index.get(magic_number)
        if not master_id:
            return
        master_state = self.masters.get(master_id)
        if not master_state:
            return
        linked = [s_id for s_id, ss in self.slaves.items()
                  if master_id in ss.account.master_ids and ss.status == ConnectionStatus.CONNECTED]
        tasks = [self._close_on_slave(magic_number, symbol, master_state, self.slaves[s_id]) for s_id in linked]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def route_modify(self, modify: ModifySignal):
        master_id = self._magic_index.get(modify.magic_number)
        if not master_id:
            self._log_event("WARN", f"route_modify: no master for magic={modify.magic_number}")
            return
        master_state = self.masters.get(master_id)
        if not master_state:
            return

        linked = [s_id for s_id, ss in self.slaves.items()
                  if master_id in ss.account.master_ids and ss.status == ConnectionStatus.CONNECTED]

        tasks = [self._modify_sltp_on_slave(modify, master_state, self.slaves[s_id]) for s_id in linked]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Execution ─────────────────────────────────────────────────────────────

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
        # Real master: mark connected without switching the terminal to the master broker.
        # The terminal stays logged into the slave account for Python trade execution.
        # Master equity is updated from each trade signal's master_equity field.
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
        Connect slave to MT5.  Runs all blocking MT5 calls in a thread executor so
        the event loop stays responsive during the ~70–250 s cross-broker server switch.

        mt5.login() has no timeout parameter; after it returns (even with IPC timeout),
        we re-initialize IPC and verify account_info() — the terminal often completes
        the server switch even when the IPC response is lost.
        """
        acc = state.account
        aid = acc.account_id
        loop = asyncio.get_event_loop()

        def _set(step, msg):
            self._provision_status[aid] = {"step": step, "message": msg, "done": False, "error": None}

        try:
            _set(3, "Connecting to MT5…")

            if not MT5_AVAILABLE:
                state.equity, state.balance = 10_000.0, 10_000.0
                state.status    = ConnectionStatus.CONNECTED
                state.last_ping = datetime.utcnow()
                self._provision_status[aid] = {"step": 5, "message": "Connected (simulation)", "done": True, "error": None}
                notifier.notify_slave_connected(acc.label, aid, acc.server, state.equity)
                return {"status": "connected", "equity": state.equity}

            lock = self._mt5_lock or asyncio.Lock()
            async with lock:
                # Step 3 — connect MT5 IPC.
                # NOTE: mt5.initialize(path=...) does not work on this setup;
                # always use pathless initialize() which connects to any running terminal.
                _set(3, "Connecting to MT5 IPC…")
                if not mt5.terminal_info():
                    ok = await loop.run_in_executor(None, lambda: mt5.initialize(timeout=60_000))
                    if not ok:
                        raise RuntimeError(
                            f"MT5 IPC unavailable: {mt5.last_error()}. "
                            "Ensure MetaTrader 5 is open and logged in."
                        )

                # Already on the right account — nothing to do.
                info = mt5.account_info()
                if info and info.login == acc.login:
                    state.equity, state.balance = info.equity, info.balance
                    state.status    = ConnectionStatus.CONNECTED
                    state.last_ping = datetime.utcnow()
                    self._provision_status[aid] = {"step": 5, "message": "Connected", "done": True, "error": None}
                    notifier.notify_slave_connected(acc.label, aid, acc.server, state.equity)
                    return {"status": "connected", "equity": state.equity}

                # Step 4 — switch account.
                # If the terminal is currently on a different broker we need mt5.login().
                # This blocks (no timeout param) and can take ~70–250 s for a server switch;
                # run in a thread so the event loop stays responsive.
                _set(4, "Logging in to broker (may take several minutes on first connect)…")
                await loop.run_in_executor(
                    None,
                    lambda: mt5.login(acc.login, password=acc.password, server=acc.server),
                )

                # After login (or IPC timeout), re-initialize and verify.
                # The terminal may have completed the switch even if IPC response was lost.
                _set(4, "Verifying connection…")
                if not mt5.terminal_info():
                    await loop.run_in_executor(None, lambda: mt5.initialize(timeout=60_000))

                info = mt5.account_info()
                if not info or info.login != acc.login:
                    raise RuntimeError(
                        f"MT5 login failed: {mt5.last_error()}. "
                        "Please open the slave MT5 terminal and manually login to "
                        f"{acc.server} (account {acc.login}), then click Reconnect."
                    )

                state.equity, state.balance = info.equity, info.balance

            state.status    = ConnectionStatus.CONNECTED
            state.last_ping = datetime.utcnow()
            self._provision_status[aid] = {"step": 5, "message": "Connected", "done": True, "error": None}
            notifier.notify_slave_connected(acc.label, aid, acc.server, state.equity)
            return {"status": "connected", "equity": state.equity}

        except Exception as exc:
            err = str(exc)
            logger.error(f"provision_slave [{acc.label}]: {err}")
            state.status = ConnectionStatus.ERROR
            state.error  = err
            self._provision_status[aid] = {"step": 0, "message": err, "done": False, "error": err}
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
        Guarantee MT5 is connected to this slave's account. Caller must hold _mt5_lock.
        Uses pathless mt5.initialize() (path-based init doesn't work on this host).
        """
        try:
            info = mt5.account_info()
            if info and info.login == acc.login:
                return True
        except Exception:
            pass
        if not mt5.terminal_info():
            if not mt5.initialize(timeout=60_000):
                return False
        info = mt5.account_info()
        if info and info.login == acc.login:
            return True
        # Account switch — may block for up to ~100 s; caller runs this in a thread executor
        # via provision_slave() for any blocking scenario.
        return bool(mt5.login(acc.login, password=acc.password, server=acc.server))

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
