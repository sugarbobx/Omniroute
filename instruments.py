"""
instruments.py — OmniRoute
Single source of truth for per-symbol trading metadata: point size, pip size,
and contract (notional-per-lot) size.

Previously this knowledge was scattered and inconsistent across three places:
  • protection._POINT_SIZE_MAP        (prefix table)
  • bot._pip_size                     (price-magnitude guess)
  • bot._contract_size                (price-magnitude guess)

Everything now funnels through instrument_info(symbol). In live mode the MT5
access layer may enrich the returned InstrumentInfo with values pulled from
mt5.symbol_info (point, trade_contract_size); when those are unavailable we fall
back to the prefix table below, and finally to the forex default.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class InstrumentInfo:
    symbol: str
    point: float          # smallest price increment (e.g. 0.00001 for EURUSD)
    pip: float            # conventional pip (usually 10 * point)
    contract_size: float  # notional units per 1.0 lot (for PnL estimation)
    digits: int


# Prefix → (point, contract_size). pip is derived as point * 10 unless the asset
# quotes in "points == pips" terms (metals, indices, crypto), where pip == point.
# contract_size is the notional multiplier used only for simulated PnL; live mode
# overrides it from the broker.
_PREFIX_TABLE = {
    # symbol prefix : (point, contract_size, pip_equals_point)
    "XAU":   (0.01,  100.0,     True),   # Gold
    "XAG":   (0.001, 5000.0,    True),   # Silver
    "US30":  (1.0,   1.0,       True),   # Indices
    "DJ30":  (1.0,   1.0,       True),
    "WS30":  (1.0,   1.0,       True),
    "NAS":   (0.1,   1.0,       True),
    "US100": (0.1,   1.0,       True),
    "USTEC": (0.1,   1.0,       True),
    "UK100": (0.1,   1.0,       True),
    "GER40": (0.1,   1.0,       True),
    "SPX":   (0.1,   1.0,       True),
    "US500": (0.1,   1.0,       True),
    "USOIL": (0.01,  1000.0,    True),
    "UKOIL": (0.01,  1000.0,    True),
    "WTI":   (0.01,  1000.0,    True),
    "BTC":   (1.0,   1.0,       True),   # Crypto
    "ETH":   (0.1,   1.0,       True),
}

_FOREX_POINT = 0.00001
_FOREX_CONTRACT = 100_000.0   # standard lot
_JPY_POINT = 0.001            # JPY pairs quote to 3 decimals


def _digits_for(point: float) -> int:
    # e.g. 0.00001 -> 5, 0.01 -> 2, 1.0 -> 0
    d = 0
    p = point
    while p < 1.0 and d < 8:
        p *= 10
        d += 1
    return d


def instrument_info(symbol: str) -> InstrumentInfo:
    """Best-effort metadata from the symbol name alone (no broker connection)."""
    sym = (symbol or "").upper()

    for prefix, (point, contract, pip_eq_point) in _PREFIX_TABLE.items():
        if sym.startswith(prefix):
            pip = point if pip_eq_point else point * 10
            return InstrumentInfo(sym, point, pip, contract, _digits_for(point))

    # Forex: JPY quote pairs use 0.001 point, everything else 0.00001
    if "JPY" in sym:
        return InstrumentInfo(sym, _JPY_POINT, _JPY_POINT * 10, _FOREX_CONTRACT,
                              _digits_for(_JPY_POINT))
    return InstrumentInfo(sym, _FOREX_POINT, _FOREX_POINT * 10, _FOREX_CONTRACT,
                          _digits_for(_FOREX_POINT))


def instrument_info_from_broker(symbol: str, point: float, digits: int,
                                 contract_size: float) -> InstrumentInfo:
    """Build InstrumentInfo from live mt5.symbol_info values. pip is 10*point for
    5/3-digit forex, otherwise equal to point."""
    pip = point * 10 if digits in (3, 5) else point
    return InstrumentInfo(symbol.upper(), point, pip, contract_size or 1.0, digits)


def point_size(symbol: str) -> float:
    return instrument_info(symbol).point


def pip_size(symbol: str) -> float:
    return instrument_info(symbol).pip


def contract_size(symbol: str) -> float:
    return instrument_info(symbol).contract_size
