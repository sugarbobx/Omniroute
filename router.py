"""
router.py — OmniRoute v2.2
Trade Protection wired in:
  • Slippage gate before every order_send
  • LotScaler applied on top of base volume
  • SL/TP translated via SLTPCalculator on open
  • SL/TP modify synced to all linked slaves via route_modify()
  • DB position tracking for modify lookups
"""

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, date
from typing import Dict, Optional

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

    # ── Boot / shutdown ──────────────────────────────────────────────────────

    async def startup(self):
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
        return {"status": "removed", "master_id": master_id}

    async def add_slave(self, account: SlaveAccount) -> dict:
        if account.account_id in self.slaves:
            return {"status": "already_registered", "account_id": account.account_id}
        state = SlaveState(account)
        self.slaves[account.account_id] = state
        db.save_slave(account)
        await self._connect_slave(state)
        self._log_event("INFO", f"Slave added: {account.label}", account_id=account.account_id)
        return {"status": "added", "account_id": account.account_id, "connected": state.status == ConnectionStatus.CONNECTED}

    def remove_slave(self, account_id: str) -> dict:
        if account_id not in self.slaves:
            return {"status": "not_found"}
        self.slaves.pop(account_id)
        db.delete_slave(account_id)
        return {"status": "removed", "account_id": account_id}

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

        notifier.notify_trade_detected(
            master_label=master_state.account.label, magic_number=signal.magic_number,
            symbol=signal.symbol, trade_type=signal.type.value, volume=signal.volume,
            price=signal.price, sl=signal.sl, tp=signal.tp, signal_id=signal.signal_id,
        )

        linked_ids = [
            s_id for s_id, ss in self.slaves.items()
            if master_id in ss.account.master_ids
            and ss.account.enabled
            and ss.status == ConnectionStatus.CONNECTED
        ]
        if not linked_ids:
            self._log_event("WARN", f"No connected slaves for master {master_state.account.label}", master_id=master_id)
            return

        tasks = [self._execute_on_slave(signal, master_state, self.slaves[s_id], t0) for s_id in linked_ids]
        await asyncio.gather(*tasks, return_exceptions=True)
        master_state.trades_today += 1

    async def route_close(self, magic_number: int, symbol: str):
        master_id = self._magic_index.get(magic_number)
        if not master_id:
            return
        linked = [s_id for s_id, ss in self.slaves.items()
                  if master_id in ss.account.master_ids and ss.status == ConnectionStatus.CONNECTED]
        tasks = [self._close_on_slave(magic_number, symbol, self.slaves[s_id]) for s_id in linked]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def route_modify(self, modify: ModifySignal):
        """
        SL/TP sync: propagate master modify to all linked slave positions.
        Called by the new /trade-modify endpoint from the Master EA.
        """
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
        acc          = slave_state.account
        protection   = acc.protection
        slave_symbol = self._resolve_symbol(signal.symbol, master_state, slave_state)

        # ── Step 1: Get current market price on slave ──────────────────────
        current_price = self._get_current_price(slave_symbol, signal.type)
        if current_price is None:
            current_price = signal.price  # fallback to master price in sim mode

        # ── Step 2: Slippage gate ──────────────────────────────────────────
        slip_result = prot_engine.check_slippage(
            master_price=signal.price,
            current_price=current_price,
            symbol=slave_symbol,
            trade_type=signal.type,
            protection=protection,
        )
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
                price=signal.price, success=False,
                error_message=slip_result.message,
                slippage_checked=True, slippage_deviation=slip_result.deviation, slippage_blocked=True,
                latency_ms=round((time.perf_counter() - t0) * 1000, 2),
            )

        # ── Step 3: Calculate base volume then apply risk multiplier ───────
        base_volume  = self._calculate_volume(signal, master_state, slave_state)
        volume       = prot_engine.scale_lot(base_volume, protection)

        # ── Step 4: Calculate slave SL/TP ─────────────────────────────────
        slave_sl, slave_tp = prot_engine.calculate_slave_sltp(
            master_price=signal.price,
            master_sl=signal.sl,
            master_tp=signal.tp,
            slave_price=current_price,
            symbol=slave_symbol,
            trade_type=signal.type,
            protection=protection,
        )
        # If SL/TP sync disabled, pass through master values unchanged
        if not protection.sltp_sync_enabled:
            slave_sl = signal.sl
            slave_tp = signal.tp

        slippage = acc.slippage_override or signal.slippage

        result = TradeResult(
            account_id=acc.account_id, master_id=master_state.account.master_id,
            signal_id=signal.signal_id, symbol=signal.symbol, slave_symbol=slave_symbol,
            trade_type=signal.type.value, requested_volume=signal.volume,
            executed_volume=volume, price=signal.price, success=False,
            slippage_checked=True, slippage_deviation=slip_result.deviation, slippage_blocked=False,
            lot_after_risk=volume, sl_synced=slave_sl, tp_synced=slave_tp,
            latency_ms=0,
        )

        try:
            if not MT5_AVAILABLE:
                await asyncio.sleep(0.004)
                result.success      = True
                result.order_ticket = 100000 + int(time.time() * 1000) % 99999
                result.price        = current_price
                slave_state.open_tickets.add(result.order_ticket)
                # Record position for future modify lookups
                db.record_slave_position(
                    acc.account_id, master_state.account.master_id, signal.magic_number,
                    slave_symbol, result.order_ticket, current_price, signal.type.value,
                )
            else:
                order_type = self._map_order_type(signal.type)
                req = {
                    "action": mt5.TRADE_ACTION_DEAL, "symbol": slave_symbol,
                    "volume": volume, "type": order_type, "price": current_price,
                    "sl": slave_sl, "tp": slave_tp,
                    "deviation": slippage, "magic": signal.magic_number,
                    "comment": signal.comment,
                    "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_IOC,
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

            if result.success:
                self._copied_today += 1
                self._log_event(
                    "INFO",
                    f"✅ {signal.type.upper()} {volume}L {slave_symbol} @ {result.price:.5f} "
                    f"SL={slave_sl:.5f} TP={slave_tp:.5f} "
                    f"slip={slip_result.deviation:.1f}{protection.slippage_mode.value[0]} "
                    f"risk×{protection.risk_multiplier} [{ms:.0f}ms]",
                    master_id=master_state.account.master_id,
                    account_id=acc.account_id, signal_id=signal.signal_id,
                    symbol=slave_symbol, latency_ms=ms,
                )
                notifier.notify_trade_copied(
                    master_label=master_state.account.label, slave_label=acc.label,
                    slave_id=acc.account_id, symbol=slave_symbol,
                    trade_type=signal.type.value, volume=volume, price=result.price,
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
        acc       = slave_state.account
        protection = acc.protection
        slave_symbol = self._resolve_symbol(modify.symbol, master_state, slave_state)

        if not protection.sltp_sync_enabled:
            return

        # Fetch all open tickets for this magic+symbol on this slave
        tickets = db.get_slave_tickets(acc.account_id, modify.magic_number, slave_symbol)
        if not tickets:
            self._log_event("WARN", f"SL/TP modify: no tracked positions for {slave_symbol} magic={modify.magic_number}", account_id=acc.account_id)
            return

        for pos_info in tickets:
            ticket     = pos_info["ticket"]
            open_price = pos_info["open_price"]
            trade_type = TradeType(pos_info["trade_type"])

            # Calculate translated SL/TP for this slave position
            new_sl, new_tp = prot_engine.calculate_modify_sltp(
                master_sl=modify.new_sl,
                master_tp=modify.new_tp,
                slave_entry=open_price,
                master_entry=modify.master_price or open_price,
                symbol=slave_symbol,
                trade_type=trade_type,
                protection=protection,
            )

            old_sl, old_tp = 0.0, 0.0  # We don't track current values in-memory

            if not MT5_AVAILABLE:
                logger.info(f"[SIM] Modify #{ticket} {slave_symbol}: SL={new_sl:.5f} TP={new_tp:.5f}")
                db.log_modify(acc.account_id, ticket, slave_symbol, old_sl, old_tp, new_sl, new_tp, True)
                self._log_event("INFO",
                    f"🔄 SL/TP synced #{ticket} {slave_symbol} SL={new_sl:.5f} TP={new_tp:.5f}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)
                continue

            request = {
                "action":   mt5.TRADE_ACTION_SLTP,
                "symbol":   slave_symbol,
                "sl":       new_sl,
                "tp":       new_tp,
                "position": ticket,
            }
            r = mt5.order_send(request)
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                db.log_modify(acc.account_id, ticket, slave_symbol, old_sl, old_tp, new_sl, new_tp, True)
                self._log_event("INFO",
                    f"🔄 SL/TP synced #{ticket} {slave_symbol} SL={new_sl:.5f} TP={new_tp:.5f}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)
            else:
                err = _decode_retcode(r.retcode if r else -1)
                db.log_modify(acc.account_id, ticket, slave_symbol, old_sl, old_tp, new_sl, new_tp, False, err)
                self._log_event("ERROR",
                    f"SL/TP modify failed #{ticket}: {err}",
                    master_id=master_state.account.master_id, account_id=acc.account_id)

    # ── Close ─────────────────────────────────────────────────────────────────

    async def _close_on_slave(self, magic_number: int, symbol: str, state: SlaveState):
        slave_symbol = symbol  # simplified; resolver needs master_state
        if not MT5_AVAILABLE:
            tickets = db.get_slave_tickets(state.account.account_id, magic_number, slave_symbol)
            for t in tickets:
                db.remove_slave_position(state.account.account_id, t["ticket"])
            self._log_event("INFO", f"[SIM] Close {slave_symbol} magic={magic_number}", account_id=state.account.account_id)
            return
        positions = mt5.positions_get(symbol=slave_symbol) or []
        for pos in positions:
            if pos.magic != magic_number:
                continue
            close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            tick = mt5.symbol_info_tick(slave_symbol)
            req = {
                "action": mt5.TRADE_ACTION_DEAL, "symbol": slave_symbol, "volume": pos.volume,
                "type": close_type, "position": pos.ticket,
                "price": tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask,
                "deviation": 10, "magic": magic_number, "comment": "OmniClose",
                "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_IOC,
            }
            r = mt5.order_send(req)
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                state.open_tickets.discard(pos.ticket)
                db.remove_slave_position(state.account.account_id, pos.ticket)
                self._log_event("INFO", f"Closed #{pos.ticket}", account_id=state.account.account_id)

    # ── Terminal connections ──────────────────────────────────────────────────

    async def _connect_master(self, state: MasterState):
        acc = state.account
        if not acc.enabled:
            state.status = ConnectionStatus.DISCONNECTED; return
        if not MT5_AVAILABLE:
            state.status = ConnectionStatus.CONNECTED
            state.equity = 50_000.0; state.balance = 50_000.0
            state.last_ping = datetime.utcnow()
            notifier.notify_master_connected(acc.label, acc.magic_number, acc.server, state.equity)
            return
        try:
            ok = mt5.initialize(path=acc.terminal_path, login=acc.login, password=acc.password, server=acc.server, timeout=10_000)
            if not ok:
                state.status = ConnectionStatus.ERROR
                state.error  = f"MT5 init failed: {mt5.last_error()}"
                notifier.notify_master_error(acc.label, acc.magic_number, state.error)
                return
            info = mt5.account_info()
            if info: state.equity, state.balance = info.equity, info.balance
            state.status = ConnectionStatus.CONNECTED; state.last_ping = datetime.utcnow()
            notifier.notify_master_connected(acc.label, acc.magic_number, acc.server, state.equity)
        except Exception as e:
            state.status = ConnectionStatus.ERROR; state.error = str(e)
            notifier.notify_master_error(acc.label, acc.magic_number, str(e))

    async def _connect_slave(self, state: SlaveState):
        acc = state.account
        if not acc.enabled:
            state.status = ConnectionStatus.DISCONNECTED; return
        if not MT5_AVAILABLE:
            state.status = ConnectionStatus.CONNECTED
            state.equity = 10_000.0; state.balance = 10_000.0
            state.last_ping = datetime.utcnow()
            notifier.notify_slave_connected(acc.label, acc.account_id, acc.server, state.equity)
            return
        try:
            ok = mt5.initialize(path=acc.terminal_path, login=acc.login, password=acc.password, server=acc.server, timeout=10_000)
            if not ok:
                state.status = ConnectionStatus.ERROR
                state.error  = f"MT5 init failed: {mt5.last_error()}"
                notifier.notify_slave_error(acc.label, acc.account_id, state.error)
                return
            info = mt5.account_info()
            if info: state.equity, state.balance = info.equity, info.balance
            state.status = ConnectionStatus.CONNECTED; state.last_ping = datetime.utcnow()
            notifier.notify_slave_connected(acc.label, acc.account_id, acc.server, state.equity)
        except Exception as e:
            state.status = ConnectionStatus.ERROR; state.error = str(e)
            notifier.notify_slave_error(acc.label, acc.account_id, str(e))

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
