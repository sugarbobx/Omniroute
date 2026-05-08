"""
models.py — OmniRoute v2.2
New in this version:
  • TradeProtection: per-slave slippage guard, lot scaling profile, SL/TP sync toggle
  • SlaveAccount: trade_protection field + risk_profile label
  • ModifySignal: SL/TP update payload from Master EA
  • SlippageCheckResult: outcome of the slippage gate
  • ProtectionEvent: logged when a trade is blocked or modified
"""

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ── Enums ────────────────────────────────────────────────────────────────────

class TradeType(str, Enum):
    BUY        = "buy"
    SELL       = "sell"
    BUY_LIMIT  = "buy_limit"
    SELL_LIMIT = "sell_limit"
    BUY_STOP   = "buy_stop"
    SELL_STOP  = "sell_stop"
    CLOSE      = "close"


class LotSizingMode(str, Enum):
    EQUITY_RATIO = "equity_ratio"
    FIXED        = "fixed"
    MULTIPLIER   = "multiplier"


class AccountRole(str, Enum):
    MASTER = "master"
    SLAVE  = "slave"


class ConnectionStatus(str, Enum):
    CONNECTED    = "connected"
    DISCONNECTED = "disconnected"
    ERROR        = "error"
    PENDING      = "pending"


class SlippageMode(str, Enum):
    POINTS  = "points"   # Max deviation in broker points (1 point = 0.00001 for forex)
    PERCENT = "percent"  # Max deviation as % of entry price (e.g. 0.05 = 0.05%)
    PIPS    = "pips"     # Max deviation in pips (1 pip = 10 points for most pairs)


class SyncMode(str, Enum):
    FULL    = "full"     # Sync both SL and TP
    SL_ONLY = "sl_only"  # Only sync Stop Loss
    TP_ONLY = "tp_only"  # Only sync Take Profit
    NONE    = "none"     # No SL/TP synchronisation


# ── Trade Protection Config (per slave) ──────────────────────────────────────

class TradeProtection(BaseModel):
    """
    All three protection features live here.
    Stored as a JSON blob in the slaves table.
    """

    # ── 1. Slippage Protection ───────────────────────────────────────────────
    slippage_enabled:    bool        = True
    slippage_max:        float       = Field(3.0, description="Maximum allowed slippage before aborting copy")
    slippage_mode:       SlippageMode = SlippageMode.POINTS
    slippage_action:     str         = "cancel"   # "cancel" | "execute_anyway"

    # ── 2. Lot Scaling (Risk Profile) ────────────────────────────────────────
    # When lot_sizing_mode = MULTIPLIER these are ignored (multiplier field is used).
    # When mode = EQUITY_RATIO or FIXED, risk_multiplier is applied on top.
    risk_profile_label:  str   = "default"  # human label: "conservative", "aggressive", etc.
    risk_multiplier:     float = Field(1.0, ge=0.01, le=100.0,
                                       description="Final multiplier applied to calculated lot. 0.5 = half, 2.0 = double.")
    risk_max_lot:        float = Field(10.0, gt=0, description="Absolute lot ceiling after multiplier")
    risk_min_lot:        float = Field(0.01, gt=0)

    # ── 3. SL/TP Synchronisation ─────────────────────────────────────────────
    sltp_sync_enabled:   bool     = True
    sltp_sync_mode:      SyncMode = SyncMode.FULL
    sltp_offset_sl:      float    = Field(0.0,
                                          description="Add fixed offset to master SL in points. Negative = tighter SL.")
    sltp_offset_tp:      float    = Field(0.0,
                                          description="Add fixed offset to master TP in points. Positive = wider TP.")
    sltp_scale_sl:       float    = Field(1.0, ge=0.0,
                                          description="Scale master SL distance. 0.5 = half the distance.")
    sltp_scale_tp:       float    = Field(1.0, ge=0.0,
                                          description="Scale master TP distance. 2.0 = double the distance.")

    class Config:
        json_schema_extra = {
            "example": {
                "slippage_enabled": True,
                "slippage_max": 2.0,
                "slippage_mode": "points",
                "risk_profile_label": "conservative",
                "risk_multiplier": 0.5,
                "risk_max_lot": 5.0,
                "sltp_sync_enabled": True,
                "sltp_sync_mode": "full",
                "sltp_offset_sl": 0.0,
                "sltp_scale_sl": 1.0,
                "sltp_scale_tp": 1.0,
            }
        }


# ── Slippage check outcome ────────────────────────────────────────────────────

class SlippageCheckResult(BaseModel):
    passed:         bool
    master_price:   float
    current_price:  float
    deviation:      float         # in the configured mode's unit
    max_allowed:    float
    mode:           SlippageMode
    action_taken:   str           # "allowed" | "blocked" | "warning"
    message:        str


# ── Master Account ────────────────────────────────────────────────────────────

class MasterAccount(BaseModel):
    master_id:     str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    label:         str = Field(...)
    login:         int = Field(...)
    password:      str = Field(...)
    server:        str = Field(...)
    terminal_path: str = Field("C:\\Program Files\\MetaTrader 5\\terminal64.exe")
    magic_number:  int = Field(...)
    symbol_map:    dict[str, str] = Field(default_factory=dict)
    enabled:       bool = True
    created_at:    datetime = Field(default_factory=datetime.utcnow)

    @field_validator("label")
    @classmethod
    def label_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("label cannot be empty")
        return v.strip()


# ── Slave Account ─────────────────────────────────────────────────────────────

class SlaveAccount(BaseModel):
    account_id:    str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    label:         str = Field(...)
    login:         int = Field(...)
    password:      str = Field(...)
    server:        str = Field(...)
    terminal_path: str = Field("C:\\Program Files\\MetaTrader 5\\terminal64.exe")

    master_ids:    list[str] = Field(default_factory=list)

    lot_sizing_mode: LotSizingMode = LotSizingMode.EQUITY_RATIO
    fixed_lot:     float = Field(0.01)
    multiplier:    float = Field(1.0)
    max_lot:       float = Field(10.0)
    min_lot:       float = Field(0.01)

    symbol_map:    dict[str, str] = Field(default_factory=dict)

    max_open_trades:   int = Field(20)
    slippage_override: Optional[int] = None

    # ── Trade Protection (new) ───────────────────────────────────────────────
    protection: TradeProtection = Field(
        default_factory=TradeProtection,
        description="Slippage guard, lot scaling profile, and SL/TP sync settings"
    )

    enabled:    bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("label")
    @classmethod
    def label_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("label cannot be empty")
        return v.strip()


# ── Unified account creation ──────────────────────────────────────────────────

class AddAccountRequest(BaseModel):
    role:          AccountRole
    label:         str
    login:         int
    password:      str
    server:        str
    terminal_path: str = "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
    magic_number:  Optional[int]  = None
    symbol_map:    dict[str, str] = Field(default_factory=dict)
    lot_sizing_mode: LotSizingMode = LotSizingMode.EQUITY_RATIO
    fixed_lot:     float = 0.01
    multiplier:    float = 1.0
    max_lot:       float = 10.0
    min_lot:       float = 0.01
    max_open_trades: int = 20
    protection:    TradeProtection = Field(default_factory=TradeProtection)


# ── Linking ───────────────────────────────────────────────────────────────────

class LinkRequest(BaseModel):
    master_id:  str
    account_id: str


class UnlinkRequest(BaseModel):
    master_id:  str
    account_id: str


# ── Trade Signal ──────────────────────────────────────────────────────────────

class TradeSignal(BaseModel):
    """Payload sent by the Master EA on open."""
    signal_id:    str   = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    symbol:       str   = Field(...)
    type:         TradeType
    volume:       float = Field(..., gt=0)
    price:        float = Field(..., gt=0)
    sl:           float = Field(0.0)
    tp:           float = Field(0.0)
    magic_number: int   = Field(...)
    comment:      str   = Field("OmniRoute")
    slippage:     int   = Field(3)
    master_equity: Optional[float] = None
    timestamp:    datetime = Field(default_factory=datetime.utcnow)

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return v.upper().strip()


# ── Modify Signal (SL/TP update from Master EA) ───────────────────────────────

class ModifySignal(BaseModel):
    """
    Sent by the Master EA when a position's SL or TP is changed.
    The bridge uses magic_number to find all matching slave tickets
    and calls TRADE_ACTION_SLTP on each one.
    """
    magic_number: int   = Field(..., description="Identifies which master position was modified")
    symbol:       str   = Field(...)
    new_sl:       float = Field(0.0, description="New Stop Loss price (0 = remove)")
    new_tp:       float = Field(0.0, description="New Take Profit price (0 = remove)")
    master_price: float = Field(0.0, description="Current master position entry price (for offset calculations)")
    timestamp:    datetime = Field(default_factory=datetime.utcnow)

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return v.upper().strip()


# ── Execution Result ──────────────────────────────────────────────────────────

class TradeResult(BaseModel):
    account_id:       str
    master_id:        Optional[str] = None
    signal_id:        str
    symbol:           str
    slave_symbol:     str
    trade_type:       str
    requested_volume: float
    executed_volume:  float
    price:            float
    order_ticket:     Optional[int] = None
    success:          bool
    error_code:       Optional[int] = None
    error_message:    Optional[str] = None
    latency_ms:       float
    # Protection metadata
    slippage_checked:   bool = False
    slippage_deviation: Optional[float] = None
    slippage_blocked:   bool = False
    lot_after_risk:     Optional[float] = None
    sl_synced:          Optional[float] = None
    tp_synced:          Optional[float] = None
    timestamp:          datetime = Field(default_factory=datetime.utcnow)


# ── Log entry ─────────────────────────────────────────────────────────────────

class TradeLog(BaseModel):
    id:         str  = Field(default_factory=lambda: str(uuid.uuid4())[:12])
    level:      str  = "INFO"
    message:    str
    master_id:  Optional[str] = None
    account_id: Optional[str] = None
    signal_id:  Optional[str] = None
    symbol:     Optional[str] = None
    latency_ms: Optional[float] = None
    timestamp:  datetime = Field(default_factory=datetime.utcnow)


# ── Status snapshots ──────────────────────────────────────────────────────────

class MasterStatus(BaseModel):
    master_id:         str
    label:             str
    magic_number:      int
    connection_status: ConnectionStatus
    equity:            float
    balance:           float
    linked_slaves:     list[str]
    trades_today:      int
    error:             Optional[str] = None
    last_ping:         Optional[datetime] = None


class SlaveStatus(BaseModel):
    account_id:        str
    label:             str
    server:            str
    connection_status: ConnectionStatus
    equity:            float
    balance:           float
    master_ids:        list[str]
    open_trades:       int
    lot_sizing_mode:   str
    protection:        Optional[dict] = None
    error:             Optional[str] = None
    last_ping:         Optional[datetime] = None


class BridgeStatus(BaseModel):
    online:               bool
    masters_total:        int
    masters_connected:    int
    slaves_total:         int
    slaves_connected:     int
    trades_copied_today:  int
    trades_failed_today:  int
    avg_latency_ms:       float
    uptime_seconds:       float
    timestamp:            datetime
    masters:              list[MasterStatus]
    slaves:               list[SlaveStatus]
