"""
Unit tests for OmniRoute's pure logic: instrument metadata, protection math,
indicators, block operators, and the visual strategy runtime. These are the
functions that ultimately size and gate real orders, so they get direct coverage
independent of the FastAPI/MT5 layers.

Run: pytest -q   (from the project root)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import instruments
import strategy_loader as sl
import protection as prot
from models import TradeProtection, TradeType, SlippageMode, SyncMode


# ── instruments ──────────────────────────────────────────────────────────────

def test_instrument_forex_defaults():
    i = instruments.instrument_info("EURUSD")
    assert i.point == 0.00001
    assert i.pip == 0.0001
    assert i.contract_size == 100_000.0

def test_instrument_jpy_pair():
    i = instruments.instrument_info("USDJPY")
    assert i.point == 0.001
    assert i.pip == 0.01

def test_instrument_gold_and_index():
    assert instruments.instrument_info("XAUUSD").point == 0.01
    assert instruments.instrument_info("US30").point == 1.0
    # metals/indices quote pip == point
    assert instruments.pip_size("XAUUSD") == 0.01


# ── protection: slippage ─────────────────────────────────────────────────────

def test_slippage_passes_within_tolerance():
    p = TradeProtection(slippage_enabled=True, slippage_max=3.0, slippage_mode=SlippageMode.PIPS)
    r = prot.check_slippage(1.10000, 1.10015, "EURUSD", TradeType.BUY, p)  # 1.5 pips
    assert r.passed is True

def test_slippage_blocks_beyond_tolerance():
    p = TradeProtection(slippage_enabled=True, slippage_max=1.0, slippage_mode=SlippageMode.PIPS,
                        slippage_action="cancel")
    r = prot.check_slippage(1.10000, 1.10050, "EURUSD", TradeType.BUY, p)  # 5 pips
    assert r.passed is False

def test_slippage_execute_anyway_overrides_block():
    p = TradeProtection(slippage_enabled=True, slippage_max=1.0, slippage_mode=SlippageMode.PIPS,
                        slippage_action="execute_anyway")
    r = prot.check_slippage(1.10000, 1.10050, "EURUSD", TradeType.BUY, p)
    assert r.passed is True
    assert r.action_taken == "warning"

def test_slippage_disabled_always_passes():
    p = TradeProtection(slippage_enabled=False)
    r = prot.check_slippage(1.10000, 1.50000, "EURUSD", TradeType.BUY, p)
    assert r.passed is True


# ── protection: lot scaling ──────────────────────────────────────────────────

def test_scale_lot_multiplier_and_clamp():
    p = TradeProtection(risk_multiplier=0.5, risk_min_lot=0.01, risk_max_lot=10.0)
    assert prot.scale_lot(0.20, p) == 0.10
    p2 = TradeProtection(risk_multiplier=2.0, risk_max_lot=0.30)
    assert prot.scale_lot(0.20, p2) == 0.30   # clamped to ceiling
    p3 = TradeProtection(risk_multiplier=0.01, risk_min_lot=0.05)
    assert prot.scale_lot(0.20, p3) == 0.05   # 0.20*0.01≈0 → clamped to floor


# ── protection: SL/TP translation ────────────────────────────────────────────

def test_sltp_buy_places_on_correct_side():
    p = TradeProtection(sltp_sync_enabled=True, sltp_sync_mode=SyncMode.FULL)
    sl_price, tp_price = prot.calculate_slave_sltp(
        master_price=1.10000, master_sl=1.09800, master_tp=1.10400,
        slave_price=1.10010, symbol="EURUSD", trade_type=TradeType.BUY, protection=p)
    assert sl_price < 1.10010 < tp_price   # SL below, TP above entry for a BUY

def test_sltp_none_mode_returns_zero():
    p = TradeProtection(sltp_sync_enabled=True, sltp_sync_mode=SyncMode.NONE)
    assert prot.calculate_slave_sltp(1.1, 1.09, 1.11, 1.1, "EURUSD", TradeType.BUY, p) == (0.0, 0.0)


# ── indicators ───────────────────────────────────────────────────────────────

def test_sma_basic():
    out = sl.sma([1, 2, 3, 4, 5], 3)
    assert out[:2] == [None, None]
    assert out[2] == 2.0 and out[3] == 3.0 and out[4] == 4.0

def test_rsi_all_gains_is_100():
    vals = list(range(1, 40))  # strictly increasing → RSI 100
    out = sl.rsi(vals, 14)
    assert out[-1] == 100.0

def test_ema_warmup_then_values():
    out = sl.ema([1] * 30, 10)
    # constant series → EMA equals the constant after warmup
    assert out[-1] == 1.0
    assert out[8] is None and out[9] is not None

def test_atr_positive():
    n = 40
    high = [10 + i * 0.1 for i in range(n)]
    low = [9 + i * 0.1 for i in range(n)]
    close = [9.5 + i * 0.1 for i in range(n)]
    out = sl.atr(high, low, close, 14)
    assert out[-1] is not None and out[-1] > 0


# ── block operators ──────────────────────────────────────────────────────────

def test_operator_lt_gt():
    assert sl._check_operator([5, 3], "lt", 4) is True
    assert sl._check_operator([5, 6], "gt", 4) is True

def test_operator_cross_above():
    assert sl._check_operator([29, 31], "cross_above", 30) is True
    assert sl._check_operator([31, 32], "cross_above", 30) is False

def test_operator_none_series():
    assert sl._check_operator([None], "gt", 0) is False


# ── visual runtime: reverse-on-signal is no longer dead code ─────────────────

def _md(closes, open_position=False, last="NONE"):
    n = len(closes)
    return {"open": closes[:], "high": [c + 0.001 for c in closes],
            "low": [c - 0.001 for c in closes], "close": closes[:],
            "volume": [100] * n, "time": list(range(n)),
            "state": {"open_position": open_position, "last_direction": last}}

def test_visual_entry_fires():
    blocks = {"entry": {"indicator": "RSI", "period": 14, "operator": "gt",
                        "value": 50, "direction": "BUY"}}
    rt = sl.VisualStrategyRuntime("t", blocks)
    # rising series → RSI ~100 > 50 → BUY
    out = rt.evaluate(_md([1 + i * 0.01 for i in range(40)]))
    assert out == "BUY"

def test_visual_reverse_on_signal_surfaces_direction_when_open():
    blocks = {"entry": {"indicator": "RSI", "period": 14, "operator": "gt",
                        "value": 50, "direction": "BUY"},
              "position": {"reverse_on_signal": True}}
    rt = sl.VisualStrategyRuntime("t", blocks)
    out = rt.evaluate(_md([1 + i * 0.01 for i in range(40)], open_position=True, last="SELL"))
    # With reverse_on_signal on, an open position still yields the entry direction
    assert out == "BUY"

def test_visual_no_reverse_holds_when_open():
    blocks = {"entry": {"indicator": "RSI", "period": 14, "operator": "gt",
                        "value": 50, "direction": "BUY"},
              "position": {"reverse_on_signal": False}}
    rt = sl.VisualStrategyRuntime("t", blocks)
    out = rt.evaluate(_md([1 + i * 0.01 for i in range(40)], open_position=True, last="SELL"))
    assert out == "HOLD"


# ── protection presets sanity ────────────────────────────────────────────────

def test_presets_use_pips_not_points():
    for name in ("ultra_safe", "conservative", "default", "aggressive"):
        assert prot.RISK_PRESETS[name].slippage_mode == SlippageMode.PIPS
