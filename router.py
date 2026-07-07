"""
router.py — OmniRoute v2.3
Trade Protection wired in:
  • Slippage gate before every market order_send
  • LotScaler applied on top of base volume
  • SL/TP translated via SLTPCalculator on open
  • SL/TP modify synced to all linked slaves via route_modify()
  • DB position tracking for modify lookups

v2.3: all MT5 access goes through the shared SessionManager (mt5_client), so each
account runs on its own terminal subprocess and no SDK call blocks the event loop.
Pending order types are executed as TRADE_ACTION_PENDING; CLOSE-typed signals are
rejected here and must use the /trade-close path.
"""

import asyncio
import logging
import time
from collections import deque
from datetime import datetime
from typing import Dict, Optional

import database as db
import notifier
import protection as prot_engine
import mt5_client
from mt5_client import MT5_AVAILABLE, RETCODE_DONE, SessionManager
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

_PENDING_TYPES = {TradeType.BUY_LIMIT, TradeType.SELL_LIMIT,
                  TradeType.BUY_STOP, TradeType.SELL_STOP}
_BUY_TYPES = {TradeType.BUY, TradeType.BUY_LIMIT, TradeType.BUY_STOP}

_ORDER_TYPE_TOKEN = {
    TradeType.BUY: "ORDER_TYPE_BUY", TradeType.SELL: "ORDER_TYPE_SELL",
    TradeType.BUY_LIMIT: "ORDER_TYPE_BUY_LIMIT", TradeType.SELL_LIMIT: "ORDER_TYPE_SELL_LIMIT",
    TradeType.BUY_STOP: "ORDER_TYPE_BUY_STOP", TradeType.SELL_STOP: "ORDER_TYPE_SELL_STOP",
}


def _is_virtual(acc: MasterAccount) -> bool:
    """Virtual-bot masters carry placeholder credentials and own no terminal."""
    return acc.login == 0 or acc.server == "virtual"


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

    def __init__(self, sessions: Optional[SessionManager] = None):
        self.masters: Dict[str, MasterState] = {}
        self.slaves:  Dict[str, SlaveState]  = {}
        self.sessions = sessions or SessionManager()
        self._magic_index: Dict[int, str] = {}
        self.global_symbol_map: Dict[str, str] = {}
        self._log:       deque[TradeLog] = deque(maxlen=self.MAX_LOG)
        self._latencies: deque[float]    = deque(maxlen=500)
        self._copied_today  = 0
        self._failed_today  = 0
        self._blocked_today = 0
        self._today         = datetime.utcnow().date()
        self._start_time    = time.time()

    # ── Boot / shutdown ──────────────────────────────────────────────────────

    async def startup(self):
        # Restore persisted global symbol map
        self.global_symbol_map = db.get_setting("global_symbol_map", {}) or {}
        # Restore recent activity log so the UI isn't blank after a restart
        for row in db.load_recent_logs(self.MAX_LOG):
            self._log.append(TradeLog(
                level=row["level"], message=row["message"], master_id=row.get("master_id"),
                account_id=row.get("account_id"), signal_id=row.get("signal_id"),
                symbol=row.get("symbol"), latency_ms=row.get("latency_ms"),
            ))

        masters = db.load_all_masters()
        slaves  = db.load_all_slaves()
        for m in masters:
            self.masters[m.master_id] = MasterState(m)
            self._magic_index[m.magic_number] = m.master_id
        for s in slaves:
            self.slaves[s.account_id] = SlaveState(s)
        for s_id, s_state in self.slaves.items():
            s_state.account.master_ids = db.get_masters_for_slave(s_id)
        logger.info(f"Loaded {len(self.masters)} masters, {len(self.slaves)} slaves")
        tasks = (
            [self._connect_master(ms) for ms in self.masters.values()] +
            [self._connect_slave(ss)  for ss in self.slaves.values()]
        )
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        notifier.notify_bridge_started(len(self.masters), len(self.slaves))

    async def shutdown(self):
        notifier.notify_bridge_stopped()
        await notifier.close_client()
        await self.sessions.shutdown_all()

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

    async def remove_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters.pop(master_id)
        self._magic_index.pop(state.account.magic_number, None)
        db.delete_master(master_id)
        await self.sessions.remove(master_id)
        for s in self.slaves.values():
            s.account.master_ids = [m for m in s.account.master_ids if m != master_id]
        return {"status": "removed", "master_id": master_id, "was_virtual": _is_virtual(state.account)}

    async def add_slave(self, account: SlaveAccount) -> dict:
        if account.account_id in self.slaves:
            return {"status": "already_registered", "account_id": account.account_id}
        state = SlaveState(account)
        self.slaves[account.account_id] = state
        db.save_slave(account)
        await self._connect_slave(state)
        self._log_event("INFO", f"Slave added: {account.label}", account_id=account.account_id)
        return {"status": "added", "account_id": account.account_id, "connected": state.status == ConnectionStatus.CONNECTED}

    async def remove_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves.pop(account_id)
        db.delete_slave(account_id)
        await self.sessions.remove(account_id)
        return {"status": "removed", "account_id": account_id}

    async def update_master(self, master_id: str, data: dict) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        db.update_master(master_id, data)
        state = self.masters[master_id]
        acc = state.account
        for k, v in data.items():
            if hasattr(acc, k):
                setattr(acc, k, v)
        if "magic_number" in data:
            old_magic = next((mn for mn, mid in self._magic_index.items() if mid == master_id), None)
            if old_magic is not None:
                self._magic_index.pop(old_magic, None)
            self._magic_index[acc.magic_number] = master_id
        # Credentials may have changed — drop the stale session so a reconnect
        # builds a fresh terminal with the new login/server.
        await self.sessions.remove(master_id)
        await self._connect_master(state)
        return {"status": "updated", "master_id": master_id, "connected": state.status == ConnectionStatus.CONNECTED}

    async def update_slave(self, account_id: str, data: dict) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        db.update_slave(account_id, data)
        state = self.slaves[account_id]
        acc = state.account
        for k, v in data.items():
            if k == "lot_sizing_mode":
                acc.lot_sizing_mode = LotSizingMode(v)
            elif hasattr(acc, k):
                setattr(acc, k, v)
        await self.sessions.remove(account_id)
        await self._connect_slave(state)
        return {"status": "updated", "account_id": account_id, "connected": state.status == ConnectionStatus.CONNECTED}

    async def ping_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters[master_id]
        state.last_ping = datetime.utcnow()
        return {"status": state.status.value, "equity": state.equity,
                "last_ping": state.last_ping.isoformat()}

    async def reconnect_master(self, master_id: str) -> dict:
        if master_id not in self.masters:
            return {"status": "not_found"}
        state = self.masters[master_id]
        state.status = ConnectionStatus.PENDING
        state.error = None
        await self.sessions.remove(master_id)
        await self._connect_master(state)
        return {"status": state.status.value, "master_id": master_id, "error": state.error}

    async def reconnect_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        state = self.slaves[account_id]
        state.status = ConnectionStatus.PENDING
        state.error = None
        await self.sessions.remove(account_id)
        await self._connect_slave(state)
        return {"status": state.status.value, "account_id": account_id, "error": state.error}

    def get_provision_status(self, account_id: str) -> dict:
        """Session-based provisioning status. Each account owns one terminal
        session; 'done' means it is connected. (The prior multi-terminal build
        exposed a multi-step provisioning flow — this preserves that endpoint
        contract on the session architecture.)"""
        state = self.slaves.get(account_id)
        if not state:
            return {"step": 0, "message": "Unknown", "done": False, "error": "not_found"}
        if state.status == ConnectionStatus.CONNECTED:
            return {"step": 3, "message": "Connected", "done": True, "error": None}
        if state.status == ConnectionStatus.ERROR:
            return {"step": 0, "message": "Connection error", "done": False, "error": state.error}
        return {"step": 1, "message": state.status.value, "done": False, "error": None}

    def update_protection(self, account_id: str, protection: TradeProtection) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves[account_id].account.protection = protection
        db.update_slave_protection(account_id, protection)
        self._log_event("INFO", f"Protection updated: profile={protection.risk_profile_label}", account_id=account_id)
        return {"status": "updated", "account_id": account_id, "protection": protection.model_dump()}

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

    async def route_signal(self, signal: TradeSignal, t0: float) -> list[TradeResult]:
        self._reset_daily_counters()
        if signal.type == TradeType.CLOSE:
            # A CLOSE is not an open; route it through the close path instead of
            # silently opening a BUY (the old _map_order_type fallback did exactly that).
            await self.route_close(signal.magic_number, signal.symbol)
            return []
        master_id = self._magic_index.get(signal.magic_number)
        if not master_id:
            self._log_event("WARN", f"No master for magic={signal.magic_number}")
            return []
        master_state = self.masters.get(master_id)
        if not master_state:
            return []
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
            return []

        notifier.notify_trade_detected(
            master_label=master_state.account.label, magic_number=signal.magic_number,
            symbol=signal.symbol, trade_type=signal.type.value, volume=signal.volume,
            price=signal.price, sl=signal.sl, tp=signal.tp, signal_id=signal.signal_id,
        )

        tasks = [self._execute_on_slave(signal, master_state, self.slaves[s_id], t0) for s_id in linked_ids]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        results: list[TradeResult] = []
        for r in gathered:
            if isinstance(r, Exception):
                self._log_event("ERROR", f"Slave execution crashed: {r}", master_id=master_id)
            elif r is not None:
                results.append(r)
        master_state.trades_today += 1
        return results

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
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, Exception):
                    self._log_event("ERROR", f"Slave close crashed: {r}", master_id=master_id)

    async def route_modify(self, modify: ModifySignal):
        """SL/TP sync: propagate master modify to all linked slave positions."""
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
            for r in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(r, Exception):
                    self._log_event("ERROR", f"Slave modify crashed: {r}", master_id=master_id)

    # ── Execution ─────────────────────────────────────────────────────────────

    async def _execute_on_slave(
        self, signal: TradeSignal, master_state: MasterState, slave_state: SlaveState, t0: float
    ) -> TradeResult:
        acc          = slave_state.account
        protection   = acc.protection
        slave_symbol = self._resolve_symbol(signal.symbol, master_state, slave_state)
        session      = self.sessions.get(acc.account_id)
        is_pending   = signal.type in _PENDING_TYPES

        # ── Step 1: Get current market price on slave ──────────────────────
        current_price = await self._get_current_price(session, slave_symbol, signal.type)
        if current_price is None:
            current_price = signal.price  # fallback to master price in sim mode

        # ── Step 2: Slippage gate (market orders only — pending prices are
        #            intentionally away from market) ─────────────────────────
        slip_dev = 0.0
        if not is_pending:
            slip_result = prot_engine.check_slippage(
                master_price=signal.price, current_price=current_price,
                symbol=slave_symbol, trade_type=signal.type, protection=protection,
            )
            slip_dev = slip_result.deviation
            if not slip_result.passed:
                self._blocked_today += 1
                msg = f"🛡 SLIPPAGE BLOCKED [{acc.label}] {slave_symbol}: {slip_result.message}"
                self._log_event("WARN", msg, master_id=master_state.account.master_id,
                                account_id=acc.account_id, signal_id=signal.signal_id, symbol=slave_symbol)
                notifier.notify_trade_failed(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol, trade_type=signal.type.value,
                    error_message=slip_result.message, error_code=None, signal_id=signal.signal_id,
                )
                return TradeResult(
                    account_id=acc.account_id, master_id=master_state.account.master_id,
                    signal_id=signal.signal_id, symbol=signal.symbol, slave_symbol=slave_symbol,
                    trade_type=signal.type.value, requested_volume=signal.volume, executed_volume=0.0,
                    price=signal.price, success=False, error_message=slip_result.message,
                    slippage_checked=True, slippage_deviation=slip_result.deviation, slippage_blocked=True,
                    latency_ms=round((time.perf_counter() - t0) * 1000, 2),
                )

        # ── Step 3: Base volume then risk multiplier ───────────────────────
        base_volume  = self._calculate_volume(signal, master_state, slave_state)
        volume       = prot_engine.scale_lot(base_volume, protection)

        # ── Step 4: Slave SL/TP ────────────────────────────────────────────
        # Pending orders anchor SL/TP off the intended entry price, market off the tick.
        anchor_price = signal.price if is_pending else current_price
        slave_sl, slave_tp = prot_engine.calculate_slave_sltp(
            master_price=signal.price, master_sl=signal.sl, master_tp=signal.tp,
            slave_price=anchor_price, symbol=slave_symbol,
            trade_type=signal.type, protection=protection,
        )
        if not protection.sltp_sync_enabled:
            slave_sl, slave_tp = signal.sl, signal.tp

        order_price = signal.price if is_pending else current_price
        slippage = acc.slippage_override or signal.slippage

        result = TradeResult(
            account_id=acc.account_id, master_id=master_state.account.master_id,
            signal_id=signal.signal_id, symbol=signal.symbol, slave_symbol=slave_symbol,
            trade_type=signal.type.value, requested_volume=signal.volume,
            executed_volume=volume, price=order_price, success=False,
            slippage_checked=not is_pending, slippage_deviation=slip_dev, slippage_blocked=False,
            lot_after_risk=volume, sl_synced=slave_sl, tp_synced=slave_tp, latency_ms=0,
        )

        try:
            req = {
                "action": "TRADE_ACTION_PENDING" if is_pending else "TRADE_ACTION_DEAL",
                "symbol": slave_symbol, "volume": volume,
                "type": _ORDER_TYPE_TOKEN[signal.type], "price": order_price,
                "sl": slave_sl, "tp": slave_tp, "deviation": slippage,
                "magic": signal.magic_number, "comment": signal.comment,
                "type_time": "ORDER_TIME_GTC", "type_filling": "ORDER_FILLING_IOC",
            }
            r = await session.order_send(req) if session else None
            if r is None:
                result.error_message = "no terminal session for slave"
            elif r["retcode"] == RETCODE_DONE:
                result.success      = True
                result.order_ticket = r["order"]
                result.price        = r["price"]
                slave_state.open_tickets.add(r["order"])
                db.record_slave_position(
                    acc.account_id, master_state.account.master_id, signal.magic_number,
                    slave_symbol, r["order"], r["price"], signal.type.value,
                )
            else:
                result.error_code    = r["retcode"]
                result.error_message = r.get("error") or _decode_retcode(r["retcode"])
        except Exception as exc:
            result.error_message = str(exc)
        finally:
            ms = round((time.perf_counter() - t0) * 1000, 2)
            result.latency_ms = ms
            self._latencies.append(ms)
            if result.success:
                self._copied_today += 1
                self._log_event(
                    "INFO",
                    f"✅ {signal.type.value.upper()} {volume}L {slave_symbol} @ {result.price:.5f} "
                    f"SL={slave_sl:.5f} TP={slave_tp:.5f} "
                    f"slip={slip_dev:.1f}{protection.slippage_mode.value[0]} "
                    f"risk×{protection.risk_multiplier} [{ms:.0f}ms]",
                    master_id=master_state.account.master_id, account_id=acc.account_id,
                    signal_id=signal.signal_id, symbol=slave_symbol, latency_ms=ms,
                )
                notifier.notify_trade_copied(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol, trade_type=signal.type.value,
                    volume=volume, price=result.price, ticket=result.order_ticket,
                    latency_ms=ms, signal_id=signal.signal_id,
                )
            else:
                self._failed_today += 1
                self._log_event(
                    "ERROR", f"❌ {result.error_message} (code {result.error_code})",
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
        acc          = slave_state.account
        protection   = acc.protection
        slave_symbol = self._resolve_symbol(modify.symbol, master_state, slave_state)
        session      = self.sessions.get(acc.account_id)
        if not protection.sltp_sync_enabled:
            return

        tickets = db.get_slave_tickets(acc.account_id, modify.magic_number, slave_symbol)
        if not tickets:
            self._log_event("WARN", f"SL/TP modify: no tracked positions for {slave_symbol} magic={modify.magic_number}", account_id=acc.account_id)
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
            old_sl, old_tp = 0.0, 0.0
            request = {"action": "TRADE_ACTION_SLTP", "symbol": slave_symbol,
                       "sl": new_sl, "tp": new_tp, "position": ticket}
            r = await session.order_send(request) if session else None
            if r and r["retcode"] == RETCODE_DONE:
                db.log_modify(acc.account_id, ticket, slave_symbol, old_sl, old_tp, new_sl, new_tp, True)
                self._log_event("INFO",
                    f"🔄 SL/TP synced #{ticket} {slave_symbol} SL={new_sl:.5f} TP={new_tp:.5f}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)
            else:
                err = r.get("error") if r else "no session"
                err = err or _decode_retcode(r["retcode"] if r else -1)
                db.log_modify(acc.account_id, ticket, slave_symbol, old_sl, old_tp, new_sl, new_tp, False, err)
                self._log_event("ERROR", f"SL/TP modify failed #{ticket}: {err}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)

    # ── Close ─────────────────────────────────────────────────────────────────

    async def _close_on_slave(self, magic_number: int, symbol: str, master_state: MasterState, state: SlaveState):
        acc          = state.account
        slave_symbol = self._resolve_symbol(symbol, master_state, state)
        session      = self.sessions.get(acc.account_id)

        positions = await session.positions_get(symbol=slave_symbol, magic=magic_number) if session else []
        if not positions:
            # No live positions to match (sim mode, or already flat): fall back to
            # our own DB tracking so counters and records stay consistent.
            for t in db.get_slave_tickets(acc.account_id, magic_number, slave_symbol):
                state.open_tickets.discard(t["ticket"])
                db.remove_slave_position(acc.account_id, t["ticket"])
            self._log_event("INFO", f"Close {slave_symbol} magic={magic_number} (tracked)", account_id=acc.account_id)
            return

        for pos in positions:
            close_is_buy = pos["type"] != 0  # position BUY(0) closes with a SELL
            tick = await session.symbol_info_tick(slave_symbol)
            price = (tick["ask"] if close_is_buy else tick["bid"]) if tick else pos["price_open"]
            req = {
                "action": "TRADE_ACTION_DEAL", "symbol": slave_symbol, "volume": pos["volume"],
                "type": "ORDER_TYPE_BUY" if close_is_buy else "ORDER_TYPE_SELL",
                "position": pos["ticket"], "price": price, "deviation": 10,
                "magic": magic_number, "comment": "OmniClose",
                "type_time": "ORDER_TIME_GTC", "type_filling": "ORDER_FILLING_IOC",
            }
            r = await session.order_send(req)
            if r and r["retcode"] == RETCODE_DONE:
                state.open_tickets.discard(pos["ticket"])
                db.remove_slave_position(acc.account_id, pos["ticket"])
                self._log_event("INFO", f"Closed #{pos['ticket']}", account_id=acc.account_id)

    # ── Terminal connections ──────────────────────────────────────────────────

    async def _connect_master(self, state: MasterState):
        acc = state.account
        if not acc.enabled:
            state.status = ConnectionStatus.DISCONNECTED; return
        # Virtual-bot masters have no terminal; they only route to slaves.
        if _is_virtual(acc):
            state.status = ConnectionStatus.CONNECTED
            state.last_ping = datetime.utcnow()
            return
        try:
            sess = await self.sessions.get_or_create(
                acc.master_id, path=acc.terminal_path, login=acc.login,
                password=acc.password, server=acc.server, sim_equity=50_000.0, is_master=True)
            state.equity, state.balance = sess.equity, sess.balance
            state.status = ConnectionStatus.CONNECTED
            state.last_ping = datetime.utcnow()
            notifier.notify_master_connected(acc.label, acc.magic_number, acc.server, state.equity)
        except Exception as e:
            state.status = ConnectionStatus.ERROR; state.error = str(e)
            notifier.notify_master_error(acc.label, acc.magic_number, str(e))

    async def _connect_slave(self, state: SlaveState):
        acc = state.account
        if not acc.enabled:
            state.status = ConnectionStatus.DISCONNECTED; return
        try:
            sess = await self.sessions.get_or_create(
                acc.account_id, path=acc.terminal_path, login=acc.login,
                password=acc.password, server=acc.server, sim_equity=10_000.0)
            state.equity, state.balance = sess.equity, sess.balance
            state.status = ConnectionStatus.CONNECTED
            state.last_ping = datetime.utcnow()
            notifier.notify_slave_connected(acc.label, acc.account_id, acc.server, state.equity)
        except Exception as e:
            state.status = ConnectionStatus.ERROR; state.error = str(e)
            notifier.notify_slave_error(acc.label, acc.account_id, str(e))

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _get_current_price(self, session, symbol: str, trade_type: TradeType) -> Optional[float]:
        # In sim mode we deliberately return None so the caller falls back to the
        # master's price (deviation 0) — sim has no independent market to slip against.
        if not MT5_AVAILABLE or session is None:
            return None
        tick = await session.symbol_info_tick(symbol)
        if not tick:
            return None
        return tick["ask"] if trade_type in _BUY_TYPES else tick["bid"]

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
        try:
            db.append_log(level, message, master_id, account_id, signal_id, symbol, latency_ms)
        except Exception:
            pass  # never let logging persistence break the trade path
        fn = logger.error if level == "ERROR" else logger.warning if level == "WARN" else logger.info
        fn(f"[{master_id or account_id or 'BRIDGE'}] {message}")

    def _reset_daily_counters(self):
        today = datetime.utcnow().date()
        if today != self._today:
            self._copied_today = self._failed_today = self._blocked_today = 0
            for ms in self.masters.values():
                ms.trades_today = 0
            self._today = today


def _decode_retcode(retcode: int) -> str:
    return {
        10004:"Requote", 10006:"Rejected", 10013:"Invalid request",
        10014:"Invalid volume", 10015:"Invalid price", 10016:"Invalid stops",
        10017:"Trade disabled", 10018:"Market closed", 10019:"Insufficient funds",
        10020:"Prices changed", 10021:"No quotes", 10024:"Too many requests",
        10026:"AutoTrading disabled (server)", 10027:"AutoTrading disabled (client)",
        10031:"No connection", 10033:"Pending orders limit", 10034:"Volume limit",
    }.get(retcode, f"Error {retcode}")
