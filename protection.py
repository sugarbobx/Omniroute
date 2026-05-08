"""
protection.py — OmniRoute Trade Protection Engine

Three independent, composable guards that run before every trade execution:

  1. SlippageGuard  — compares current market price to master's entry price.
                      If deviation exceeds threshold, returns a blocked result.

  2. LotScaler      — applies risk_multiplier on top of the base calculated
                      volume, then clamps to [risk_min_lot, risk_max_lot].

  3. SLTPCalculator — translates master SL/TP to slave prices using offset
                      and scale parameters.  Used on both open and modify.

All functions are synchronous and pure (no side-effects, no I/O).
The router calls them inline before sending orders — zero added latency
when MT5 is unavailable because they operate purely on floats.
"""

import logging
from typing import Optional

from models import (
    SlippageCheckResult,
    SlippageMode,
    SyncMode,
    TradeProtection,
    TradeType,
)

logger = logging.getLogger("protection")

# ── Point sizes by asset class (fallback when MT5 not available) ─────────────
# Keys are symbol prefixes; values are the pip size in price terms.
_POINT_SIZE_MAP = {
    "XAU": 0.01,    # Gold  — 1 point = 0.01
    "XAG": 0.001,   # Silver
    "US30": 1.0,    "DJ30": 1.0,    "WS30": 1.0,   # Indices
    "NAS": 0.1,     "US100": 0.1,   "USTEC": 0.1,
    "UK100": 0.1,   "GER40": 0.1,
    "USOIL": 0.01,  "UKOIL": 0.01,  "WTI": 0.01,
    "BTC": 1.0,     "ETH": 0.1,
}
_DEFAULT_POINT = 0.00001  # Standard forex


def _get_point_size(symbol: str) -> float:
    sym = symbol.upper()
    for prefix, size in _POINT_SIZE_MAP.items():
        if sym.startswith(prefix):
            return size
    return _DEFAULT_POINT


def _get_pip_size(symbol: str) -> float:
    """1 pip = 10 points for most instruments."""
    return _get_point_size(symbol) * 10


# ═══════════════════════════════════════════════════════════════════════════════
# 1. SLIPPAGE GUARD
# ═══════════════════════════════════════════════════════════════════════════════

def check_slippage(
    master_price:   float,
    current_price:  float,
    symbol:         str,
    trade_type:     TradeType,
    protection:     TradeProtection,
) -> SlippageCheckResult:
    """
    Compare the master's entry price against the current market price on the
    slave broker.  Returns whether the trade should proceed.

    For BUY:  current_price (ask) vs master_price — higher ask = bad slippage
    For SELL: current_price (bid) vs master_price — lower bid = bad slippage
    """
    if not protection.slippage_enabled:
        return SlippageCheckResult(
            passed=True, master_price=master_price, current_price=current_price,
            deviation=0.0, max_allowed=protection.slippage_max,
            mode=protection.slippage_mode, action_taken="allowed",
            message="Slippage protection disabled",
        )

    # Raw price deviation (always positive)
    raw_dev = abs(current_price - master_price)

    mode = protection.slippage_mode

    if mode == SlippageMode.POINTS:
        point   = _get_point_size(symbol)
        dev     = round(raw_dev / point, 1)
        max_val = protection.slippage_max

    elif mode == SlippageMode.PIPS:
        pip     = _get_pip_size(symbol)
        dev     = round(raw_dev / pip, 2)
        max_val = protection.slippage_max

    else:  # PERCENT
        dev     = round((raw_dev / master_price) * 100, 4) if master_price > 0 else 0.0
        max_val = protection.slippage_max

    passed = dev <= max_val

    if passed:
        action  = "allowed"
        message = f"Slippage {dev:.2f} {mode.value} ≤ max {max_val} — OK"
    else:
        if protection.slippage_action == "execute_anyway":
            action  = "warning"
            message = f"⚠ Slippage {dev:.2f} {mode.value} exceeded max {max_val} — executing anyway (action=execute_anyway)"
            passed  = True   # Override: allow but warn
        else:
            action  = "blocked"
            message = f"🛡 Slippage {dev:.2f} {mode.value} exceeded max {max_val} — TRADE BLOCKED"

    logger.debug(f"Slippage check [{symbol}]: {message}")

    return SlippageCheckResult(
        passed=passed,
        master_price=master_price,
        current_price=current_price,
        deviation=dev,
        max_allowed=max_val,
        mode=mode,
        action_taken=action,
        message=message,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 2. LOT SCALER
# ═══════════════════════════════════════════════════════════════════════════════

def scale_lot(
    base_volume:  float,
    protection:   TradeProtection,
) -> float:
    """
    Apply the risk_multiplier on top of the base calculated volume.
    Also enforces risk_min_lot and risk_max_lot from the protection config.

    Example:
      base_volume = 0.2 (from equity_ratio calculation)
      risk_multiplier = 0.5  → 0.10 lots  (conservative)
      risk_multiplier = 2.0  → 0.40 lots  (aggressive)
    """
    scaled = round(base_volume * protection.risk_multiplier, 2)
    clamped = max(protection.risk_min_lot, min(protection.risk_max_lot, scaled))
    result  = round(clamped, 2)

    if result != base_volume:
        logger.debug(
            f"LotScaler [{protection.risk_profile_label}]: "
            f"{base_volume:.2f} × {protection.risk_multiplier} → {scaled:.2f} "
            f"→ clamped {result:.2f}"
        )

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SL/TP CALCULATOR
# ═══════════════════════════════════════════════════════════════════════════════

def calculate_slave_sltp(
    master_price:  float,
    master_sl:     float,
    master_tp:     float,
    slave_price:   float,
    symbol:        str,
    trade_type:    TradeType,
    protection:    TradeProtection,
) -> tuple[float, float]:
    """
    Translate master SL/TP levels to slave-equivalent prices.

    The three transformations applied in order:
      1. Scale: adjust the SL/TP distance from entry by sltp_scale_sl/tp
      2. Offset: add a fixed pip offset (positive = further from price)
      3. Clamp: ensure SL/TP remain on the correct side of price

    Returns (slave_sl, slave_tp) — either may be 0.0 if not applicable.

    sync_mode controls which values are actually set:
      FULL    → both SL and TP
      SL_ONLY → only SL (TP returned as 0.0)
      TP_ONLY → only TP (SL returned as 0.0)
      NONE    → (0.0, 0.0)
    """
    prot = protection
    mode = prot.sltp_sync_mode

    if mode == SyncMode.NONE or not prot.sltp_sync_enabled:
        return 0.0, 0.0

    point      = _get_point_size(symbol)
    is_buy     = trade_type in (TradeType.BUY, TradeType.BUY_LIMIT, TradeType.BUY_STOP)

    # ── Stop Loss ─────────────────────────────────────────────────────────────
    slave_sl = 0.0
    if mode in (SyncMode.FULL, SyncMode.SL_ONLY) and master_sl != 0.0:
        # Distance from master entry to master SL
        sl_distance = abs(master_price - master_sl)
        # Scale the distance
        sl_distance_scaled = sl_distance * prot.sltp_scale_sl
        # Add fixed point offset (positive = move SL further from entry)
        sl_distance_final = sl_distance_scaled + (prot.sltp_offset_sl * point)
        sl_distance_final = max(0.0, sl_distance_final)  # can't go negative
        # Place SL relative to the slave's entry price
        if is_buy:
            slave_sl = round(slave_price - sl_distance_final, 5)
        else:
            slave_sl = round(slave_price + sl_distance_final, 5)
        # Safety clamp: SL must be on the correct side
        if is_buy and slave_sl >= slave_price:
            slave_sl = 0.0
        elif not is_buy and slave_sl <= slave_price:
            slave_sl = 0.0

    # ── Take Profit ───────────────────────────────────────────────────────────
    slave_tp = 0.0
    if mode in (SyncMode.FULL, SyncMode.TP_ONLY) and master_tp != 0.0:
        tp_distance = abs(master_tp - master_price)
        tp_distance_scaled = tp_distance * prot.sltp_scale_tp
        tp_distance_final  = tp_distance_scaled + (prot.sltp_offset_tp * point)
        tp_distance_final  = max(0.0, tp_distance_final)
        if is_buy:
            slave_tp = round(slave_price + tp_distance_final, 5)
        else:
            slave_tp = round(slave_price - tp_distance_final, 5)
        if is_buy and slave_tp <= slave_price:
            slave_tp = 0.0
        elif not is_buy and slave_tp >= slave_price:
            slave_tp = 0.0

    logger.debug(
        f"SLTPCalc [{symbol} {trade_type.value}]: "
        f"master SL={master_sl} TP={master_tp} → slave SL={slave_sl} TP={slave_tp} "
        f"(scale SL={prot.sltp_scale_sl} TP={prot.sltp_scale_tp}, "
        f"offset SL={prot.sltp_offset_sl} TP={prot.sltp_offset_tp})"
    )

    return slave_sl, slave_tp


def calculate_modify_sltp(
    master_sl:    float,
    master_tp:    float,
    slave_entry:  float,
    master_entry: float,
    symbol:       str,
    trade_type:   TradeType,
    protection:   TradeProtection,
) -> tuple[float, float]:
    """
    Wrapper used specifically for SL/TP modify signals.
    Uses slave_entry (actual slave position open price) for offset calculations.
    """
    return calculate_slave_sltp(
        master_price=master_entry,
        master_sl=master_sl,
        master_tp=master_tp,
        slave_price=slave_entry,
        symbol=symbol,
        trade_type=trade_type,
        protection=protection,
    )


# ── Preset risk profiles ──────────────────────────────────────────────────────

RISK_PRESETS: dict[str, TradeProtection] = {
    "ultra_safe": TradeProtection(
        slippage_enabled=True, slippage_max=1.0, slippage_mode=SlippageMode.POINTS,
        risk_profile_label="ultra_safe", risk_multiplier=0.25, risk_max_lot=1.0,
        sltp_sync_enabled=True, sltp_sync_mode=SyncMode.FULL,
        sltp_scale_sl=0.8, sltp_scale_tp=1.2,  # tighter SL, wider TP
    ),
    "conservative": TradeProtection(
        slippage_enabled=True, slippage_max=2.0, slippage_mode=SlippageMode.POINTS,
        risk_profile_label="conservative", risk_multiplier=0.5, risk_max_lot=5.0,
        sltp_sync_enabled=True, sltp_sync_mode=SyncMode.FULL,
        sltp_scale_sl=1.0, sltp_scale_tp=1.0,
    ),
    "default": TradeProtection(
        slippage_enabled=True, slippage_max=3.0, slippage_mode=SlippageMode.POINTS,
        risk_profile_label="default", risk_multiplier=1.0, risk_max_lot=10.0,
        sltp_sync_enabled=True, sltp_sync_mode=SyncMode.FULL,
    ),
    "aggressive": TradeProtection(
        slippage_enabled=True, slippage_max=5.0, slippage_mode=SlippageMode.POINTS,
        risk_profile_label="aggressive", risk_multiplier=2.0, risk_max_lot=20.0,
        sltp_sync_enabled=True, sltp_sync_mode=SyncMode.FULL,
        sltp_scale_sl=1.0, sltp_scale_tp=1.5,
    ),
    "no_protection": TradeProtection(
        slippage_enabled=False,
        risk_profile_label="no_protection", risk_multiplier=1.0, risk_max_lot=100.0,
        sltp_sync_enabled=False, sltp_sync_mode=SyncMode.NONE,
    ),
}
