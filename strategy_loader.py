"""
strategy_loader.py — OmniRoute v2.3
Unified strategy loading for the Bot Engine.

Two strategy modes share one runtime interface:
  • visual — strategies/{id}.json block tree, evaluated by VisualStrategyRuntime
  • code   — strategies/{id}.py exposing evaluate(market_data) -> str

All indicator math is pure Python (no pandas/numpy) operating on the OHLC
lists returned by mt5.copy_rates_from_pos.

market_data dict passed to evaluate():
  {
    "open": [...], "high": [...], "low": [...], "close": [...], "volume": [...],
    "time": [epoch seconds, ...],            # oldest → newest
    "trend_close": [...] | None,             # higher-TF closes, when trend filter active
    "state": {"open_position": bool, "last_direction": "BUY"|"SELL"|"NONE"}
  }

evaluate() returns: "BUY" | "SELL" | "CLOSE" | "HOLD"
"""

import importlib.util
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import database as db

logger = logging.getLogger("strategy_loader")

STRATEGIES_DIR = Path(__file__).parent / "strategies"

SIGNALS = ("BUY", "SELL", "CLOSE", "HOLD")

# UTC hours for session filters (start inclusive, end exclusive)
SESSION_HOURS = {
    "sydney":   (21, 6),
    "tokyo":    (0, 9),
    "london":   (7, 16),
    "new_york": (12, 21),
}


class StrategyLoadError(Exception):
    pass


def ensure_strategies_dir():
    STRATEGIES_DIR.mkdir(parents=True, exist_ok=True)
    init_file = STRATEGIES_DIR / "__init__.py"
    if not init_file.exists():
        init_file.write_text("", encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════
# Indicators — pure Python, each returns a list aligned with the input
# (None for warm-up bars where the value is undefined)
# ══════════════════════════════════════════════════════════════════════════

def sma(values: list, period: int) -> list:
    out = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    window_sum = sum(values[:period])
    out[period - 1] = window_sum / period
    for i in range(period, len(values)):
        window_sum += values[i] - values[i - period]
        out[i] = window_sum / period
    return out


def ema(values: list, period: int) -> list:
    out = [None] * len(values)
    if period <= 0 or len(values) < period:
        return out
    k = 2.0 / (period + 1)
    prev = sum(values[:period]) / period  # seed with SMA
    out[period - 1] = prev
    for i in range(period, len(values)):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def rsi(values: list, period: int = 14) -> list:
    out = [None] * len(values)
    if len(values) < period + 1:
        return out
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        d = values[i] - values[i - 1]
        if d > 0:
            gains += d
        else:
            losses -= d
    avg_gain, avg_loss = gains / period, losses / period
    out[period] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1 + avg_gain / avg_loss)
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (period - 1) + gain) / period   # Wilder smoothing
        avg_loss = (avg_loss * (period - 1) + loss) / period
        out[i] = 100.0 if avg_loss == 0 else 100.0 - 100.0 / (1 + avg_gain / avg_loss)
    return out


def macd(values: list, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple:
    """Returns (macd_line, signal_line, histogram) lists."""
    ema_fast, ema_slow = ema(values, fast), ema(values, slow)
    line = [None if (f is None or s is None) else f - s for f, s in zip(ema_fast, ema_slow)]
    valid = [v for v in line if v is not None]
    sig_valid = ema(valid, signal)
    pad = len(line) - len(valid)
    sig = [None] * pad + sig_valid
    hist = [None if (l is None or s is None) else l - s for l, s in zip(line, sig)]
    return line, sig, hist


def bollinger_pct_b(values: list, period: int = 20, num_std: float = 2.0) -> list:
    """%B — price position within the bands. <0 below lower band, >1 above upper."""
    out = [None] * len(values)
    mid = sma(values, period)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        m = mid[i]
        var = sum((v - m) ** 2 for v in window) / period
        std = var ** 0.5
        upper, lower = m + num_std * std, m - num_std * std
        rng = upper - lower
        out[i] = 0.5 if rng == 0 else (values[i] - lower) / rng
    return out


def atr(high: list, low: list, close: list, period: int = 14) -> list:
    out = [None] * len(close)
    if len(close) < period + 1:
        return out
    trs = []
    for i in range(1, len(close)):
        tr = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
        trs.append(tr)
    prev = sum(trs[:period]) / period
    out[period] = prev
    for i in range(period + 1, len(close)):
        prev = (prev * (period - 1) + trs[i - 1]) / period  # Wilder smoothing
        out[i] = prev
    return out


def stochastic(high: list, low: list, close: list, period: int = 14, smooth: int = 3) -> list:
    """Slow %K (raw %K smoothed by SMA)."""
    raw = [None] * len(close)
    for i in range(period - 1, len(close)):
        hh = max(high[i - period + 1:i + 1])
        ll = min(low[i - period + 1:i + 1])
        rng = hh - ll
        raw[i] = 50.0 if rng == 0 else (close[i] - ll) / rng * 100.0
    out = [None] * len(close)
    for i in range(len(close)):
        window = [raw[j] for j in range(max(0, i - smooth + 1), i + 1)]
        if all(v is not None for v in window) and len(window) == smooth:
            out[i] = sum(window) / smooth
    return out


def cci(high: list, low: list, close: list, period: int = 20) -> list:
    out = [None] * len(close)
    tp = [(h + l + c) / 3 for h, l, c in zip(high, low, close)]
    for i in range(period - 1, len(close)):
        window = tp[i - period + 1:i + 1]
        m = sum(window) / period
        mean_dev = sum(abs(v - m) for v in window) / period
        out[i] = 0.0 if mean_dev == 0 else (tp[i] - m) / (0.015 * mean_dev)
    return out


# ══════════════════════════════════════════════════════════════════════════
# Runtimes
# ══════════════════════════════════════════════════════════════════════════

def _indicator_series(name: str, period: int, md: dict, closes: Optional[list] = None) -> list:
    """Dispatch an indicator name from the block schema to its series."""
    c = closes if closes is not None else md["close"]
    name = (name or "").upper()
    if name == "RSI":
        return rsi(c, period or 14)
    if name == "EMA":
        return ema(c, period or 20)
    if name == "SMA":
        return sma(c, period or 20)
    if name == "MACD":
        return macd(c)[2]  # histogram — crosses 0 on signal-line crossovers
    if name == "BB":
        return bollinger_pct_b(c, period or 20)
    if name == "ATR":
        return atr(md["high"], md["low"], c, period or 14)
    if name == "STOCH":
        return stochastic(md["high"], md["low"], c, period or 14)
    if name == "CCI":
        return cci(md["high"], md["low"], c, period or 20)
    raise StrategyLoadError(f"Unknown indicator: {name}")


def _check_operator(series: list, op: str, value: float) -> bool:
    if not series or series[-1] is None:
        return False
    cur = series[-1]
    prev = series[-2] if len(series) > 1 else None
    if op == "lt":
        return cur < value
    if op == "gt":
        return cur > value
    if op == "cross_above":
        return prev is not None and prev <= value < cur
    if op == "cross_below":
        return prev is not None and prev >= value > cur
    return False


class VisualStrategyRuntime:
    """Evaluates a visual-mode block tree (see prompt schema)."""

    def __init__(self, strategy_id: str, blocks: dict):
        self.strategy_id = strategy_id
        self.blocks = blocks
        self.mode = "visual"
        if "entry" not in blocks:
            raise StrategyLoadError(f"Strategy {strategy_id}: missing 'entry' block")

    # ── filters ─────────────────────────────────────────────────────────

    def _session_ok(self, now: datetime) -> bool:
        sessions = (self.blocks.get("filters") or {}).get("sessions")
        if not sessions:
            return True
        hour = now.hour
        for s in sessions:
            start, end = SESSION_HOURS.get(s.lower(), (0, 24))
            if (start <= hour < end) if start < end else (hour >= start or hour < end):
                return True
        return False

    def _day_ok(self, now: datetime) -> bool:
        days = (self.blocks.get("filters") or {}).get("days")
        if days is None:
            return True
        return now.weekday() in days  # 0=Monday, matches schema

    def _atr_ok(self, md: dict) -> bool:
        f = self.blocks.get("filters") or {}
        amin, amax = f.get("atr_min"), f.get("atr_max")
        if amin is None and amax is None:
            return True
        series = atr(md["high"], md["low"], md["close"], 14)
        cur = series[-1]
        if cur is None:
            return False
        if amin is not None and cur < amin:
            return False
        if amax is not None and cur > amax:
            return False
        return True

    def _trend_ok(self, md: dict, direction: str) -> bool:
        f = self.blocks.get("filters") or {}
        if not f.get("trend_tf"):
            return True
        trend_close = md.get("trend_close")
        if not trend_close:
            return True  # higher-TF data unavailable — don't block
        period = f.get("trend_ema_period") or 200
        series = ema(trend_close, min(period, max(2, len(trend_close) - 1)))
        if series[-1] is None:
            return True
        price = trend_close[-1]
        return price > series[-1] if direction == "BUY" else price < series[-1]

    # ── evaluation ──────────────────────────────────────────────────────

    def evaluate(self, market_data: dict) -> str:
        md = market_data
        state = md.get("state") or {}

        if state.get("open_position"):
            exit_block = self.blocks.get("exit") or {}
            if exit_block.get("type", "indicator") == "indicator" and exit_block.get("indicator"):
                series = _indicator_series(exit_block["indicator"], exit_block.get("period", 14), md)
                if _check_operator(series, exit_block.get("operator", "gt"), exit_block.get("value", 0)):
                    return "CLOSE"
            # fixed-type exits (tp/sl pips) are enforced by the broker/bot engine
            return "HOLD"

        entry = self.blocks["entry"]
        direction = (entry.get("direction") or "BUY").upper()
        now = datetime.now(timezone.utc)
        if not (self._session_ok(now) and self._day_ok(now) and self._atr_ok(md)
                and self._trend_ok(md, direction)):
            return "HOLD"

        series = _indicator_series(entry.get("indicator", "RSI"), entry.get("period", 14), md)
        if _check_operator(series, entry.get("operator", "lt"), entry.get("value", 0)):
            return direction
        return "HOLD"


class CodeStrategyRuntime:
    """Wraps a user Python file exposing evaluate(market_data) -> str."""

    def __init__(self, strategy_id: str, file_path: Path):
        self.strategy_id = strategy_id
        self.mode = "code"
        self.blocks = {}  # code strategies carry no risk/filter blocks
        spec = importlib.util.spec_from_file_location(f"strategy_{strategy_id}", file_path)
        if spec is None or spec.loader is None:
            raise StrategyLoadError(f"Cannot import strategy file: {file_path}")
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            raise StrategyLoadError(f"Strategy {strategy_id} failed to import: {e}") from e
        fn = getattr(module, "evaluate", None)
        if not callable(fn):
            raise StrategyLoadError(f"Strategy {strategy_id}: no evaluate(market_data) function")
        self._fn = fn

    def evaluate(self, market_data: dict) -> str:
        result = self._fn(market_data)
        if result not in SIGNALS:
            logger.warning(f"Strategy {self.strategy_id} returned {result!r} — treating as HOLD")
            return "HOLD"
        return result


# ══════════════════════════════════════════════════════════════════════════
# Loader
# ══════════════════════════════════════════════════════════════════════════

async def load_strategy(strategy_id: str):
    """Load a strategy by ID. The DB row holds metadata; the file is the
    source of truth for the logic. Raises StrategyLoadError on any problem."""
    row = db.get_strategy(strategy_id)
    if not row:
        raise StrategyLoadError(f"Strategy {strategy_id} not found in DB")
    file_path = Path(row["file_path"])
    if not file_path.is_absolute():
        file_path = Path(__file__).parent / file_path
    if not file_path.exists():
        raise StrategyLoadError(f"Strategy file missing: {file_path}")
    if row["mode"] == "visual":
        try:
            blocks = json.loads(file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise StrategyLoadError(f"Strategy {strategy_id}: invalid JSON — {e}") from e
        return VisualStrategyRuntime(strategy_id, blocks)
    return CodeStrategyRuntime(strategy_id, file_path)


def strategy_file_path(strategy_id: str, mode: str) -> Path:
    ensure_strategies_dir()
    ext = "json" if mode == "visual" else "py"
    return STRATEGIES_DIR / f"{strategy_id}.{ext}"
