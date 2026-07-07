"""
bot.py — OmniRoute v2.3 Bot Engine
Runs one asyncio task per enabled virtual bot. Each loop tick:
  fetch OHLC → strategy.evaluate() → risk gates → execute / route.

Two signal modes:
  • standalone — bot sends orders directly through its own terminal session
  • connected  — bot injects a TradeSignal into the copier (router.route_signal);
                 the copier fan-out and per-slave protection apply unchanged, and
                 the returned per-slave results tell the bot whether anything
                 actually executed.

All MT5 access goes through the shared SessionManager (mt5_client), so no SDK call
blocks the event loop and simulation mode stays fully functional.

Durable safety state (kill switch, daily PnL, peak equity for drawdown) is persisted
to the settings table so a killed bot stays killed across restarts.
"""

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Dict, Optional

from pydantic import BaseModel, Field

import database as db
import instruments
import notifier
import strategy_loader
from mt5_client import MT5_AVAILABLE, RETCODE_DONE, SessionManager
from models import TradeSignal, TradeType

logger = logging.getLogger("bot")

POLL_INTERVALS = {"M1": 10, "M5": 15, "M15": 30, "H1": 60}
BARS_TO_FETCH = 250  # enough history for EMA(200) trend filters


def _today_str() -> str:
    return datetime.now(timezone.utc).date().isoformat()


class BotConfig(BaseModel):
    bot_id: str
    label: str
    symbol: str
    timeframe: str = "M5"
    magic_number: int
    base_volume: float = 0.1
    strategy_id: Optional[str] = None
    enabled: bool = True
    forward_test: bool = False
    mode: str = "standalone"         # "standalone" | "connected"
    linked_master_ids: list[str] = Field(default_factory=list)


@dataclass
class BotRuntimeState:
    """Typed execution state — replaces the old free-form dict so a mis-typed key
    is a clear error rather than a silent new field."""
    open_position: bool = False
    last_direction: str = "NONE"
    daily_pnl: float = 0.0
    consecutive_losses: int = 0
    killed: bool = False
    partial_closed: bool = False
    entry_price: Optional[float] = None
    entry_volume: float = 0.0
    ticket: Optional[int] = None
    status: str = "running"
    peak_equity: float = 0.0
    trail_sl: Optional[float] = None
    state_day: str = field(default_factory=_today_str)

    def durable(self) -> dict:
        return {k: getattr(self, k) for k in
                ("killed", "daily_pnl", "consecutive_losses", "peak_equity", "state_day")}


# ══════════════════════════════════════════════════════════════════════════
# Engine
# ══════════════════════════════════════════════════════════════════════════

class BotEngine:
    """One asyncio task per enabled bot; reload_and_synchronize() reconciles
    running tasks against DB state. Terminal access is shared with the copier
    via a single SessionManager."""

    def __init__(self, copy_router, sessions: Optional[SessionManager] = None):
        self.router = copy_router
        self.sessions = sessions or getattr(copy_router, "sessions", None) or SessionManager()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.execution_states: Dict[str, BotRuntimeState] = {}
        self._today = _today_str()

    # ── session access ──────────────────────────────────────────────────

    async def _session_for(self, bot: BotConfig):
        """Ensure a terminal session exists for a standalone bot. In sim mode this
        is a never-failing SimSession; in live mode it initializes the default
        terminal (bots carry no separate credentials)."""
        sess = self.sessions.get(bot.bot_id)
        if sess and sess.connected:
            return sess
        if not MT5_AVAILABLE:
            return self.sessions.create_sim(bot.bot_id)
        try:
            return await self.sessions.get_or_create(bot.bot_id, path=None)
        except Exception as e:
            logger.warning(f"Bot {bot.label}: standalone terminal unavailable ({e}) — "
                           f"consider connected mode for live trading")
            return self.sessions.create_sim(bot.bot_id)

    async def fetch_rates(self, bot: BotConfig, timeframe: str) -> Optional[dict]:
        sess = await self._session_for(bot)
        return await sess.copy_rates(bot.symbol, timeframe, BARS_TO_FETCH)

    async def get_equity(self, bot: BotConfig) -> float:
        sess = await self._session_for(bot)
        info = await sess.account_info()
        return float(info["equity"]) if info else sess.equity

    # ── lifecycle ───────────────────────────────────────────────────────

    async def reload_and_synchronize(self):
        strategy_loader.ensure_strategies_dir()
        bots = {b["bot_id"]: b for b in db.get_all_virtual_bots()}

        for bot_id in bots:
            state = self.router.masters.get(bot_id)
            if state:
                from models import ConnectionStatus
                state.status = ConnectionStatus.CONNECTED
                state.error = None

        for bot_id in list(self.active_tasks.keys()):
            b = bots.get(bot_id)
            if not b or not b["enabled"]:
                await self.kill_bot_task(bot_id)

        for bot_id, b in bots.items():
            if not b["enabled"] or bot_id in self.active_tasks:
                continue
            strat_row = db.get_strategy_for_bot(bot_id)
            if not strat_row:
                logger.info(f"Bot {b['label']} enabled but has no strategy — not started")
                continue
            config = BotConfig(
                bot_id=bot_id, label=b["label"], symbol=b["symbol"] or strat_row["symbol"],
                timeframe=b["timeframe"], magic_number=b["magic_number"],
                base_volume=b["base_volume"], strategy_id=strat_row["strategy_id"],
                enabled=True, forward_test=b["forward_test"], mode=b["mode"],
            )
            await self._boot_bot(config)
        logger.info(f"Bot engine synchronized: {len(self.active_tasks)} running")

    async def _boot_bot(self, config: BotConfig):
        try:
            runtime = await strategy_loader.load_strategy(config.strategy_id)
        except strategy_loader.StrategyLoadError as e:
            logger.warning(f"Bot {config.label}: strategy load failed — {e}")
            return

        state = self.execution_states.get(config.bot_id)
        if state is None:
            state = BotRuntimeState()
            # Restore durable safety state so a killed bot stays killed across restarts.
            saved = db.load_bot_state(config.bot_id)
            for k, v in saved.items():
                if hasattr(state, k) and v is not None:
                    setattr(state, k, v)
            self.execution_states[config.bot_id] = state
        self._reset_daily_if_needed(state)

        # Reconcile against actual positions only in live mode; sim has no terminal
        # to ask, so the persisted in-memory state is the truth.
        if MT5_AVAILABLE:
            sess = await self._session_for(config)
            positions = await sess.positions_get(magic=config.magic_number)
            state.open_position = len(positions) > 0
            if positions:
                p = positions[0]
                state.entry_price = p["price_open"]
                state.entry_volume = p["volume"]
                state.ticket = p["ticket"]
                state.last_direction = "BUY" if p["type"] == 0 else "SELL"

        state.status = "killed" if state.killed else "running"
        task = asyncio.create_task(self._run_strategy_loop(config, runtime),
                                   name=f"bot:{config.bot_id}")
        self.active_tasks[config.bot_id] = task
        logger.info(f"Bot started: {config.label} [{config.symbol} {config.timeframe} "
                    f"{config.mode}{' FWD-TEST' if config.forward_test else ''}]")

    async def kill_bot_task(self, bot_id: str):
        task = self.active_tasks.pop(bot_id, None)
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if bot_id in self.execution_states:
            self.execution_states[bot_id].status = "stopped"
        logger.info(f"Bot task killed: {bot_id}")

    def _persist(self, bot_id: str, state: BotRuntimeState):
        try:
            db.save_bot_state(bot_id, state.durable())
        except Exception:
            pass

    # ── main loop ───────────────────────────────────────────────────────

    async def _run_strategy_loop(self, bot: BotConfig, runtime):
        poll = POLL_INTERVALS.get(bot.timeframe, 15)
        state = self.execution_states[bot.bot_id]
        trend_tf = (runtime.blocks.get("filters") or {}).get("trend_tf")
        while True:
            try:
                self._reset_daily_if_needed(state)
                if state.killed:
                    state.status = "killed"
                    await asyncio.sleep(poll)
                    continue

                md = await self.fetch_rates(bot, bot.timeframe)
                if not md:
                    logger.warning(f"Bot {bot.label}: no rates for {bot.symbol}")
                    await asyncio.sleep(poll)
                    continue
                if trend_tf:
                    trend_md = await self.fetch_rates(bot, trend_tf)
                    md["trend_close"] = trend_md["close"] if trend_md else None
                md["symbol"] = bot.symbol
                md["state"] = {"open_position": state.open_position,
                               "last_direction": state.last_direction}

                direction = runtime.evaluate(md)

                if state.open_position:
                    await self._manage_open_position(bot, runtime, md, state)

                if direction in ("BUY", "SELL"):
                    if (state.open_position and direction != state.last_direction
                            and (runtime.blocks.get("position") or {}).get("reverse_on_signal")):
                        await self._close_position(bot, runtime, md["close"][-1], state,
                                                   reason="reverse signal")
                    if not state.open_position and await self._risk_gates_pass(bot, runtime, state):
                        await self._process_signal_action(bot, runtime, direction, md, state)
                elif direction == "CLOSE" and state.open_position:
                    await self._close_position(bot, runtime, md["close"][-1], state, reason="strategy exit")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Bot {bot.label} loop error: {e}", exc_info=True)
            await asyncio.sleep(poll)

    # ── risk gates ──────────────────────────────────────────────────────

    def _reset_daily_if_needed(self, state: BotRuntimeState):
        today = _today_str()
        self._today = today
        if state.state_day != today:
            state.state_day = today
            state.daily_pnl = 0.0
            state.consecutive_losses = 0
            state.killed = False   # a fresh day clears a daily-loss kill

    async def _risk_gates_pass(self, bot: BotConfig, runtime, state: BotRuntimeState) -> bool:
        risk = runtime.blocks.get("risk") or {}
        equity = await self.get_equity(bot)

        # Track peak equity for drawdown enforcement.
        if equity > state.peak_equity:
            state.peak_equity = equity
            self._persist(bot.bot_id, state)

        # Max drawdown from peak equity (persistent, not just intraday).
        dd_pct = risk.get("max_drawdown_pct")
        if dd_pct and state.peak_equity > 0:
            drawdown = (state.peak_equity - equity) / state.peak_equity * 100.0
            if drawdown >= dd_pct:
                self._kill(bot, state, f"max drawdown {drawdown:.1f}% ≥ {dd_pct}%")
                return False

        kill_pct = risk.get("kill_switch_pct")
        if kill_pct and equity > 0 and state.daily_pnl <= -equity * kill_pct / 100.0:
            self._kill(bot, state, f"kill switch: daily PnL {state.daily_pnl:.2f}")
            return False

        daily_limit = risk.get("daily_loss_limit_pct")
        if daily_limit and equity > 0 and state.daily_pnl <= -equity * daily_limit / 100.0:
            return False  # done for today, not killed

        cooldown_n = risk.get("cooldown_after_losses")
        if cooldown_n and state.consecutive_losses >= cooldown_n:
            return False

        max_concurrent = (runtime.blocks.get("position") or {}).get("max_concurrent", 1)
        if state.open_position and max_concurrent <= 1:
            return False
        return True

    def _kill(self, bot: BotConfig, state: BotRuntimeState, reason: str):
        state.killed = True
        state.status = "killed"
        self._persist(bot.bot_id, state)
        logger.warning(f"Bot {bot.label}: KILLED — {reason}")
        notifier.notify_master_error(f"🤖 {bot.label}", bot.magic_number, f"Bot killed: {reason}")

    # ── execution ───────────────────────────────────────────────────────

    async def _process_signal_action(self, bot: BotConfig, runtime, direction: str,
                                     md: dict, state: BotRuntimeState):
        price = md["close"][-1]
        exit_block = runtime.blocks.get("exit") or {}
        sl, tp = self._fixed_sltp(exit_block, direction, price, md)

        if bot.mode == "connected":
            signal = TradeSignal(
                symbol=bot.symbol,
                type=TradeType.BUY if direction == "BUY" else TradeType.SELL,
                volume=bot.base_volume, price=price, sl=sl, tp=tp,
                magic_number=bot.magic_number, comment=f"OmniBot:{bot.label[:15]}",
            )
            results = await self.router.route_signal(signal, time.perf_counter())
            # Only consider ourselves in a position if a slave actually executed.
            success = any(getattr(r, "success", False) for r in results)
            ticket = next((r.order_ticket for r in results if getattr(r, "success", False)), None)
            if not success:
                logger.info(f"Bot {bot.label}: connected signal produced no fills — not entering")
        else:
            success, ticket = await self._standalone_open(bot, direction, price, sl, tp)

        if success:
            state.open_position = True
            state.last_direction = direction
            state.partial_closed = False
            state.entry_price = price
            state.entry_volume = bot.base_volume
            state.ticket = ticket
            db.log_strategy_result({
                "result_id": str(uuid.uuid4())[:12], "strategy_id": bot.strategy_id,
                "bot_id": bot.bot_id, "signal_direction": direction,
                "executed_at": datetime.utcnow().isoformat(),
                "entry_price": price, "exit_price": None, "pnl": None,
                "mode": "forward_test" if bot.forward_test else "live",
            })
            logger.info(f"Bot {bot.label}: {direction} {bot.base_volume}L "
                        f"{bot.symbol} @ {price:.5f} [{bot.mode}]")
            notifier.notify_trade_detected(
                master_label=f"🤖 {bot.label}", magic_number=bot.magic_number,
                symbol=bot.symbol, trade_type=direction.lower(), volume=bot.base_volume,
                price=price, sl=sl, tp=tp, signal_id=bot.bot_id,
            )

    def _fixed_sltp(self, exit_block: dict, direction: str, price: float, md: dict):
        pip = instruments.pip_size(self._symbol(md))
        sl_pips, tp_pips = exit_block.get("sl_pips"), exit_block.get("tp_pips")
        sign = 1 if direction == "BUY" else -1
        sl = price - sign * sl_pips * pip if sl_pips else 0.0
        tp = price + sign * tp_pips * pip if tp_pips else 0.0
        return round(sl, 5), round(tp, 5)

    @staticmethod
    def _symbol(md: dict) -> str:
        return md.get("symbol", "")

    async def _standalone_open(self, bot: BotConfig, direction: str, price: float,
                               sl: float, tp: float):
        sess = await self._session_for(bot)
        req = {
            "action": "TRADE_ACTION_DEAL", "symbol": bot.symbol, "volume": bot.base_volume,
            "type": "ORDER_TYPE_BUY" if direction == "BUY" else "ORDER_TYPE_SELL",
            "price": price, "sl": sl, "tp": tp, "deviation": 10,
            "magic": bot.magic_number, "comment": f"OmniBot:{bot.label[:15]}",
            "type_time": "ORDER_TIME_GTC", "type_filling": "ORDER_FILLING_IOC",
        }
        r = await sess.order_send(req)
        if not r or r["retcode"] != RETCODE_DONE:
            logger.error(f"Bot {bot.label}: order failed retcode={r.get('retcode') if r else None}")
            return False, None
        return True, r["order"]

    async def _close_position(self, bot: BotConfig, runtime, exit_price: float,
                              state: BotRuntimeState, reason: str,
                              volume: Optional[float] = None, partial: bool = False):
        vol = volume or state.entry_volume
        if bot.mode == "connected":
            await self.router.route_close(bot.magic_number, bot.symbol)
        else:
            sess = await self._session_for(bot)
            close_dir = "SELL" if state.last_direction == "BUY" else "BUY"
            req = {
                "action": "TRADE_ACTION_DEAL", "symbol": bot.symbol, "volume": vol,
                "type": "ORDER_TYPE_SELL" if close_dir == "SELL" else "ORDER_TYPE_BUY",
                "price": exit_price, "deviation": 10, "magic": bot.magic_number,
                "comment": "OmniBot:close",
                "type_time": "ORDER_TIME_GTC", "type_filling": "ORDER_FILLING_IOC",
            }
            if state.ticket:
                req["position"] = state.ticket
            await sess.order_send(req)

        sign = 1 if state.last_direction == "BUY" else -1
        entry = state.entry_price or exit_price
        pnl = round((exit_price - entry) * sign * vol * instruments.contract_size(bot.symbol), 2)
        state.daily_pnl += pnl
        state.consecutive_losses = state.consecutive_losses + 1 if pnl < 0 else 0
        self._persist(bot.bot_id, state)

        db.log_strategy_result({
            "result_id": str(uuid.uuid4())[:12], "strategy_id": bot.strategy_id,
            "bot_id": bot.bot_id, "signal_direction": "CLOSE" if not partial else "PARTIAL_CLOSE",
            "executed_at": datetime.utcnow().isoformat(),
            "entry_price": entry, "exit_price": exit_price, "pnl": pnl,
            "mode": "forward_test" if bot.forward_test else "live",
        })
        logger.info(f"Bot {bot.label}: {'partial ' if partial else ''}close {vol}L "
                    f"@ {exit_price:.5f} pnl={pnl:+.2f} ({reason})")

        if partial:
            state.entry_volume = round(state.entry_volume - vol, 2)
            state.partial_closed = True
        else:
            state.open_position = False
            state.last_direction = "NONE"
            state.entry_price = None
            state.entry_volume = 0.0
            state.ticket = None
            state.partial_closed = False
            state.trail_sl = None

    # ── open-position management: partial close + trailing stop ────────

    async def _manage_open_position(self, bot: BotConfig, runtime, md: dict, state: BotRuntimeState):
        price = md["close"][-1]
        entry = state.entry_price
        if entry is None:
            return
        sign = 1 if state.last_direction == "BUY" else -1
        exit_block = runtime.blocks.get("exit") or {}
        pos_block = runtime.blocks.get("position") or {}
        pip = instruments.pip_size(bot.symbol)

        pc_pct, pc_rr = pos_block.get("partial_close_pct"), pos_block.get("partial_close_at_rr")
        sl_pips = exit_block.get("sl_pips")
        if (pc_pct and pc_rr and sl_pips and not state.partial_closed
                and bot.mode == "standalone"):
            risk_dist = sl_pips * pip
            profit_dist = (price - entry) * sign
            if risk_dist > 0 and profit_dist / risk_dist >= pc_rr:
                vol = round(state.entry_volume * pc_pct / 100.0, 2)
                if vol >= 0.01:
                    await self._close_position(bot, runtime, price, state,
                                               reason=f"partial @ {pc_rr}RR",
                                               volume=vol, partial=True)

        if exit_block.get("trailing_stop"):
            if exit_block.get("trail_type", "atr") == "atr":
                atr_series = strategy_loader.atr(md["high"], md["low"], md["close"], 14)
                dist = (atr_series[-1] or 0) * (exit_block.get("trail_atr_multiplier") or 1.5)
            else:
                dist = (exit_block.get("trail_pips") or 20) * pip
            if dist > 0:
                new_sl = round(price - sign * dist, 5)
                cur_sl = state.trail_sl
                if cur_sl is None or (new_sl - cur_sl) * sign > 0:
                    state.trail_sl = new_sl
                    await self._apply_trail_sl(bot, new_sl, state)
            cur_sl = state.trail_sl
            if cur_sl is not None and (price - cur_sl) * sign <= 0:
                await self._close_position(bot, runtime, price, state, reason="trailing stop")

    async def _apply_trail_sl(self, bot: BotConfig, new_sl: float, state: BotRuntimeState):
        if not MT5_AVAILABLE or bot.mode == "connected" or not state.ticket:
            return  # sim / connected: trail enforced in-loop, no broker modify needed
        sess = await self._session_for(bot)
        req = {"action": "TRADE_ACTION_SLTP", "symbol": bot.symbol,
               "sl": new_sl, "tp": 0.0, "position": state.ticket}
        await sess.order_send(req)

    # ── status for the API/UI ───────────────────────────────────────────

    def get_bot_status(self, bot_id: str) -> dict:
        state = self.execution_states.get(bot_id)
        running = bot_id in self.active_tasks and not self.active_tasks[bot_id].done()
        if state and state.killed:
            status = "killed"
        elif running:
            status = "running"
        else:
            status = "stopped"
        return {
            "status": status,
            "open_position": state.open_position if state else False,
            "daily_pnl": round(state.daily_pnl, 2) if state else 0.0,
            "consecutive_losses": state.consecutive_losses if state else 0,
        }
