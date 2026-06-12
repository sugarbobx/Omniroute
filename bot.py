"""
bot.py — OmniRoute v2.3 Bot Engine
Runs one asyncio task per enabled virtual bot. Each loop tick:
  fetch OHLC → strategy.evaluate() → risk gates → execute / route.

Two signal modes:
  • standalone — bot sends orders directly via the MT5 SDK
  • connected  — bot injects a TradeSignal into the copier (router.route_signal);
                 its magic_number is a masters row with is_virtual_bot=1, so the
                 existing copier fan-out and per-slave protection apply unchanged.

All MT5 SDK calls go through run_in_executor — the C extension must never block
the event loop. When MetaTrader5 is unavailable, deterministic mock data keeps
the whole system functional ([SIMULATION MODE]).
"""

import asyncio
import hashlib
import logging
import math
import time
import uuid
from datetime import datetime, date
from typing import Dict, Optional

from pydantic import BaseModel, Field

import database as db
import notifier
import strategy_loader
from models import ConnectionStatus, TradeSignal, TradeType

logger = logging.getLogger("bot")

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None  # type: ignore
    MT5_AVAILABLE = False

TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}
POLL_INTERVALS = {"M1": 10, "M5": 15, "M15": 30, "H1": 60}
BARS_TO_FETCH = 250  # enough history for EMA(200) trend filters

SIM_EQUITY = 10_000.0


class BotConfig(BaseModel):
    bot_id: str
    label: str
    symbol: str
    timeframe: str = "M5"            # M1 | M5 | M15 | H1
    magic_number: int
    base_volume: float = 0.1
    strategy_id: Optional[str] = None
    enabled: bool = True
    forward_test: bool = False
    mode: str = "standalone"         # "standalone" | "connected"
    linked_master_ids: list[str] = Field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════
# MT5 access — executor-wrapped, with simulation fallbacks
# ══════════════════════════════════════════════════════════════════════════

def _mt5_timeframe(tf: str):
    return {
        "M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15, "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4, "D1": mt5.TIMEFRAME_D1,
    }.get(tf, mt5.TIMEFRAME_M5)


def _sim_rates(symbol: str, timeframe: str, count: int) -> dict:
    """Deterministic synthetic OHLC: a slow sine trend plus hash-based noise,
    keyed by symbol and candle index so repeated calls within the same candle
    return identical data."""
    logger.info(f"[SIMULATION MODE] copy_rates_from_pos({symbol}, {timeframe}, {count})")
    tf_sec = TIMEFRAME_SECONDS.get(timeframe, 300)
    now_idx = int(time.time() // tf_sec)
    seed_base = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
    base_price = 1.0 + (seed_base % 2000) / 1000.0  # symbol-stable base ~1.0–3.0

    def noise(idx: int) -> float:
        h = int(hashlib.md5(f"{symbol}:{idx}".encode()).hexdigest()[:8], 16)
        return (h / 0xFFFFFFFF) - 0.5  # [-0.5, 0.5]

    opens, highs, lows, closes, volumes, times = [], [], [], [], [], []
    prev_close = None
    for i in range(count):
        idx = now_idx - count + 1 + i
        trend = math.sin(idx / 30.0) * 0.01 * base_price
        c = base_price + trend + noise(idx) * 0.002 * base_price
        o = prev_close if prev_close is not None else c + noise(idx - 1) * 0.001 * base_price
        spread = abs(noise(idx + 7)) * 0.0015 * base_price + 0.0001 * base_price
        highs.append(round(max(o, c) + spread, 5))
        lows.append(round(min(o, c) - spread, 5))
        opens.append(round(o, 5))
        closes.append(round(c, 5))
        volumes.append(100 + int(abs(noise(idx + 13)) * 900))
        times.append(idx * tf_sec)
        prev_close = c
    return {"open": opens, "high": highs, "low": lows, "close": closes,
            "volume": volumes, "time": times}


async def fetch_rates(symbol: str, timeframe: str, count: int = BARS_TO_FETCH) -> Optional[dict]:
    """Fetch OHLC as parallel lists (oldest → newest)."""
    if not MT5_AVAILABLE:
        return _sim_rates(symbol, timeframe, count)
    loop = asyncio.get_running_loop()
    rates = await loop.run_in_executor(
        None, mt5.copy_rates_from_pos, symbol, _mt5_timeframe(timeframe), 0, count)
    if rates is None or len(rates) == 0:
        return None
    return {
        "open":   [float(r["open"]) for r in rates],
        "high":   [float(r["high"]) for r in rates],
        "low":    [float(r["low"]) for r in rates],
        "close":  [float(r["close"]) for r in rates],
        "volume": [int(r["tick_volume"]) for r in rates],
        "time":   [int(r["time"]) for r in rates],
    }


class _SimOrderResult:
    def __init__(self, price: float):
        self.retcode = 10009  # TRADE_RETCODE_DONE
        self.order = 500000 + int(time.time() * 1000) % 99999
        self.price = price


async def send_order(request: dict) -> object:
    if not MT5_AVAILABLE:
        logger.info(f"[SIMULATION MODE] order_send({request.get('symbol')} "
                    f"{request.get('type')} vol={request.get('volume')})")
        return _SimOrderResult(request.get("price", 0.0))
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, mt5.order_send, request)


async def get_positions(magic: int, symbol: Optional[str] = None) -> list:
    if not MT5_AVAILABLE:
        logger.info(f"[SIMULATION MODE] positions_get(magic={magic})")
        return []
    loop = asyncio.get_running_loop()
    def _get():
        positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
        return [p for p in (positions or []) if p.magic == magic]
    return await loop.run_in_executor(None, _get)


async def get_equity() -> float:
    if not MT5_AVAILABLE:
        return SIM_EQUITY
    loop = asyncio.get_running_loop()
    info = await loop.run_in_executor(None, mt5.account_info)
    return float(info.equity) if info else SIM_EQUITY


# ══════════════════════════════════════════════════════════════════════════
# Engine
# ══════════════════════════════════════════════════════════════════════════

class BotEngine:
    """One asyncio task per enabled bot; reload_and_synchronize() reconciles
    running tasks against DB state."""

    def __init__(self, copy_router):
        self.router = copy_router  # CopyRouter — bot → copier, never the reverse
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.execution_states: Dict[str, dict] = {}
        self._today = date.today()

    # ── lifecycle ───────────────────────────────────────────────────────

    async def reload_and_synchronize(self):
        strategy_loader.ensure_strategies_dir()
        bots = {b["bot_id"]: b for b in db.get_all_virtual_bots()}

        # Virtual bot masters have no terminal; the copier's startup connect
        # attempt fails on them in live mode — mark them connected here.
        for bot_id in bots:
            state = self.router.masters.get(bot_id)
            if state:
                state.status = ConnectionStatus.CONNECTED
                state.error = None

        # Kill tasks whose bot was deleted or disabled
        for bot_id in list(self.active_tasks.keys()):
            b = bots.get(bot_id)
            if not b or not b["enabled"]:
                await self.kill_bot_task(bot_id)

        # Boot tasks for enabled bots with an assigned strategy
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
        state = self.execution_states.setdefault(config.bot_id, {
            "open_position": False, "last_direction": "NONE", "daily_pnl": 0.0,
            "consecutive_losses": 0, "killed": False, "partial_closed": False,
            "entry_price": None, "entry_volume": 0.0, "ticket": None, "status": "running",
        })
        # Reconcile against actual MT5 positions — never assume flat blindly.
        # In simulation there is no terminal to ask: the in-memory state from
        # setdefault above is the truth, so don't overwrite it with the mock's
        # empty position list.
        if MT5_AVAILABLE:
            positions = await get_positions(config.magic_number)
            state["open_position"] = len(positions) > 0
            if positions:
                state["entry_price"] = positions[0].price_open
                state["entry_volume"] = positions[0].volume
                state["ticket"] = positions[0].ticket
                state["last_direction"] = "BUY" if positions[0].type == 0 else "SELL"
        state["status"] = "running"
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
            self.execution_states[bot_id]["status"] = "stopped"
        logger.info(f"Bot task killed: {bot_id}")

    # ── main loop ───────────────────────────────────────────────────────

    async def _run_strategy_loop(self, bot: BotConfig, runtime):
        poll = POLL_INTERVALS.get(bot.timeframe, 15)
        state = self.execution_states[bot.bot_id]
        trend_tf = (runtime.blocks.get("filters") or {}).get("trend_tf")
        while True:
            try:
                self._reset_daily_if_needed(state)
                if state["killed"]:
                    state["status"] = "killed"
                    await asyncio.sleep(poll)
                    continue

                md = await fetch_rates(bot.symbol, bot.timeframe)
                if not md:
                    logger.warning(f"Bot {bot.label}: no rates for {bot.symbol}")
                    await asyncio.sleep(poll)
                    continue
                if trend_tf:
                    trend_md = await fetch_rates(bot.symbol, trend_tf)
                    md["trend_close"] = trend_md["close"] if trend_md else None
                md["state"] = {"open_position": state["open_position"],
                               "last_direction": state["last_direction"]}

                direction = runtime.evaluate(md)

                if state["open_position"]:
                    await self._manage_open_position(bot, runtime, md, state)

                if direction in ("BUY", "SELL"):
                    if (state["open_position"] and direction != state["last_direction"]
                            and (runtime.blocks.get("position") or {}).get("reverse_on_signal")):
                        await self._close_position(bot, runtime, md["close"][-1], state,
                                                   reason="reverse signal")
                    if not state["open_position"] and await self._risk_gates_pass(bot, runtime, state):
                        await self._process_signal_action(bot, runtime, direction, md, state)
                elif direction == "CLOSE" and state["open_position"]:
                    await self._close_position(bot, runtime, md["close"][-1], state, reason="strategy exit")

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Bot {bot.label} loop error: {e}", exc_info=True)
            await asyncio.sleep(poll)

    # ── risk gates ──────────────────────────────────────────────────────

    def _reset_daily_if_needed(self, state: dict):
        today = date.today()
        if today != self._today:
            self._today = today
            for s in self.execution_states.values():
                s["daily_pnl"] = 0.0
                s["consecutive_losses"] = 0

    async def _risk_gates_pass(self, bot: BotConfig, runtime, state: dict) -> bool:
        risk = runtime.blocks.get("risk") or {}
        equity = await get_equity()
        kill_pct = risk.get("kill_switch_pct")
        if kill_pct and equity > 0 and state["daily_pnl"] <= -equity * kill_pct / 100.0:
            state["killed"] = True
            state["status"] = "killed"
            logger.warning(f"Bot {bot.label}: KILL SWITCH tripped (daily pnl {state['daily_pnl']:.2f})")
            notifier.notify_master_error(f"🤖 {bot.label}", bot.magic_number,
                                         f"Kill switch tripped: daily PnL {state['daily_pnl']:.2f}")
            return False
        daily_limit = risk.get("daily_loss_limit_pct")
        if daily_limit and equity > 0 and state["daily_pnl"] <= -equity * daily_limit / 100.0:
            return False  # done trading for today, but not killed
        cooldown_n = risk.get("cooldown_after_losses")
        if cooldown_n and state["consecutive_losses"] >= cooldown_n:
            return False  # paused until daily reset
        max_concurrent = (runtime.blocks.get("position") or {}).get("max_concurrent", 1)
        if state["open_position"] and max_concurrent <= 1:
            return False
        return True

    # ── execution ───────────────────────────────────────────────────────

    async def _process_signal_action(self, bot: BotConfig, runtime, direction: str,
                                     md: dict, state: dict):
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
            await self.router.route_signal(signal, time.perf_counter())
            success, ticket = True, None
        else:
            success, ticket = await self._standalone_open(bot, direction, price, sl, tp)

        if success:
            state.update(open_position=True, last_direction=direction,
                         partial_closed=False, entry_price=price,
                         entry_volume=bot.base_volume, ticket=ticket)
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
        """Fixed pip TP/SL from the exit block; 0 means none."""
        pip = self._pip_size(md)
        sl_pips, tp_pips = exit_block.get("sl_pips"), exit_block.get("tp_pips")
        sign = 1 if direction == "BUY" else -1
        sl = price - sign * sl_pips * pip if sl_pips else 0.0
        tp = price + sign * tp_pips * pip if tp_pips else 0.0
        return round(sl, 5), round(tp, 5)

    @staticmethod
    def _pip_size(md: dict) -> float:
        """Infer pip size from price magnitude (no symbol_info in sim mode)."""
        p = md["close"][-1]
        if p > 500:
            return 0.1      # gold, indices
        if p > 20:
            return 0.01     # JPY pairs, oil
        return 0.0001       # forex majors

    async def _standalone_open(self, bot: BotConfig, direction: str, price: float,
                               sl: float, tp: float):
        req = {
            "action": mt5.TRADE_ACTION_DEAL if MT5_AVAILABLE else "DEAL",
            "symbol": bot.symbol, "volume": bot.base_volume,
            "type": (mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL)
                    if MT5_AVAILABLE else direction,
            "price": price, "sl": sl, "tp": tp,
            "deviation": 10, "magic": bot.magic_number,
            "comment": f"OmniBot:{bot.label[:15]}",
        }
        if MT5_AVAILABLE:
            req["type_time"] = mt5.ORDER_TIME_GTC
            req["type_filling"] = mt5.ORDER_FILLING_IOC
        r = await send_order(req)
        if r is None or r.retcode != (mt5.TRADE_RETCODE_DONE if MT5_AVAILABLE else 10009):
            logger.error(f"Bot {bot.label}: order failed retcode="
                         f"{getattr(r, 'retcode', None)}")
            return False, None
        return True, r.order

    async def _close_position(self, bot: BotConfig, runtime, exit_price: float,
                              state: dict, reason: str, volume: Optional[float] = None,
                              partial: bool = False):
        vol = volume or state["entry_volume"]
        if bot.mode == "connected":
            await self.router.route_close(bot.magic_number, bot.symbol)
        else:
            close_dir = "SELL" if state["last_direction"] == "BUY" else "BUY"
            req = {
                "action": mt5.TRADE_ACTION_DEAL if MT5_AVAILABLE else "DEAL",
                "symbol": bot.symbol, "volume": vol,
                "type": (mt5.ORDER_TYPE_SELL if close_dir == "SELL" else mt5.ORDER_TYPE_BUY)
                        if MT5_AVAILABLE else close_dir,
                "price": exit_price, "deviation": 10, "magic": bot.magic_number,
                "comment": "OmniBot:close",
            }
            if MT5_AVAILABLE:
                req["position"] = state["ticket"]
                req["type_time"] = mt5.ORDER_TIME_GTC
                req["type_filling"] = mt5.ORDER_FILLING_IOC
            await send_order(req)

        sign = 1 if state["last_direction"] == "BUY" else -1
        entry = state["entry_price"] or exit_price
        pnl = round((exit_price - entry) * sign * vol * self._contract_size(exit_price), 2)
        state["daily_pnl"] += pnl
        state["consecutive_losses"] = state["consecutive_losses"] + 1 if pnl < 0 else 0

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
            state["entry_volume"] = round(state["entry_volume"] - vol, 2)
            state["partial_closed"] = True
        else:
            state.update(open_position=False, last_direction="NONE",
                         entry_price=None, entry_volume=0.0, ticket=None,
                         partial_closed=False)

    @staticmethod
    def _contract_size(price: float) -> float:
        # rough notional per-lot multiplier so sim PnL is plausible
        if price > 500:
            return 100.0       # gold
        if price > 20:
            return 1000.0
        return 100_000.0       # forex standard lot

    # ── open-position management: partial close + trailing stop ────────

    async def _manage_open_position(self, bot: BotConfig, runtime, md: dict, state: dict):
        price = md["close"][-1]
        entry = state["entry_price"]
        if entry is None:
            return
        sign = 1 if state["last_direction"] == "BUY" else -1
        exit_block = runtime.blocks.get("exit") or {}
        pos_block = runtime.blocks.get("position") or {}
        pip = self._pip_size(md)

        # Partial close at R:R milestone — once per trade
        pc_pct, pc_rr = pos_block.get("partial_close_pct"), pos_block.get("partial_close_at_rr")
        sl_pips = exit_block.get("sl_pips")
        if (pc_pct and pc_rr and sl_pips and not state["partial_closed"]
                and bot.mode == "standalone"):
            risk_dist = sl_pips * pip
            profit_dist = (price - entry) * sign
            if risk_dist > 0 and profit_dist / risk_dist >= pc_rr:
                vol = round(state["entry_volume"] * pc_pct / 100.0, 2)
                if vol >= 0.01:
                    await self._close_position(bot, runtime, price, state,
                                               reason=f"partial @ {pc_rr}RR",
                                               volume=vol, partial=True)

        # Trailing stop
        if exit_block.get("trailing_stop"):
            if exit_block.get("trail_type", "atr") == "atr":
                atr_series = strategy_loader.atr(md["high"], md["low"], md["close"], 14)
                dist = (atr_series[-1] or 0) * (exit_block.get("trail_atr_multiplier") or 1.5)
            else:
                dist = (exit_block.get("trail_pips") or 20) * pip
            if dist > 0:
                new_sl = round(price - sign * dist, 5)
                cur_sl = state.get("trail_sl")
                # only ratchet in the favorable direction
                if cur_sl is None or (new_sl - cur_sl) * sign > 0:
                    state["trail_sl"] = new_sl
                    await self._apply_trail_sl(bot, new_sl, state)
            # trail breach → close
            cur_sl = state.get("trail_sl")
            if cur_sl is not None and (price - cur_sl) * sign <= 0:
                await self._close_position(bot, runtime, price, state, reason="trailing stop")
                state.pop("trail_sl", None)

    async def _apply_trail_sl(self, bot: BotConfig, new_sl: float, state: dict):
        if not MT5_AVAILABLE or bot.mode == "connected" or not state.get("ticket"):
            # sim / connected mode: trail enforced in-loop, no broker modify needed
            return
        req = {"action": mt5.TRADE_ACTION_SLTP, "symbol": bot.symbol,
               "sl": new_sl, "tp": 0.0, "position": state["ticket"]}
        await send_order(req)

    # ── status for the API/UI ───────────────────────────────────────────

    def get_bot_status(self, bot_id: str) -> dict:
        state = self.execution_states.get(bot_id)
        running = bot_id in self.active_tasks and not self.active_tasks[bot_id].done()
        if state and state.get("killed"):
            status = "killed"
        elif running:
            status = "running"
        else:
            status = "stopped"
        return {
            "status": status,
            "open_position": state["open_position"] if state else False,
            "daily_pnl": round(state["daily_pnl"], 2) if state else 0.0,
            "consecutive_losses": state["consecutive_losses"] if state else 0,
        }
