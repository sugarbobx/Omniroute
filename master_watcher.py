"""
master_watcher.py — Autonomous master account watcher for OmniRoute.

Replaces the old EA signal-push model. A MasterWatcher:
  • Holds its own MT5 session logged in with the investor (read-only) password.
  • Polls mt5.positions_get() + mt5.orders_get() every POLL_INTERVAL seconds.
  • Diffs each snapshot against the previous one to detect:
      - new ticket    → open intent
      - gone ticket   → close intent
      - SL/TP change  → modify intent
  • On first connect (or any restart), records existing positions as the
    baseline so pre-existing trades are NEVER emitted as new intents.
  • Runs as a single asyncio task per master account.
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional

import database as db
from models import ModifySignal, TradeSignal, TradeType

if TYPE_CHECKING:
    from router import CopyRouter

logger = logging.getLogger("watcher")

try:
    import MetaTrader5 as mt5
    MT5_AVAILABLE = True
except ImportError:
    mt5 = None  # type: ignore
    MT5_AVAILABLE = False

POLL_INTERVAL = 1.5          # seconds between position polls
CONNECT_RETRY  = 10          # seconds between reconnect attempts after IPC loss
MAX_CONNECT_RETRIES = 20     # give up after this many consecutive IPC failures


def _pos_to_dict(pos) -> dict:
    """Convert an mt5.TradePosition namedtuple to a plain dict."""
    return {
        "ticket":      int(pos.ticket),
        "symbol":      str(pos.symbol),
        "type":        int(pos.type),          # 0=BUY, 1=SELL
        "volume":      float(pos.volume),
        "price_open":  float(pos.price_open),
        "sl":          float(pos.sl),
        "tp":          float(pos.tp),
        "magic":       int(pos.magic),
        "comment":     str(pos.comment),
        "time":        int(pos.time),
    }


def _order_to_dict(order) -> dict:
    return {
        "ticket":  int(order.ticket),
        "symbol":  str(order.symbol),
        "type":    int(order.type),
        "volume":  float(order.volume_initial),
        "price":   float(order.price_open),
        "sl":      float(order.sl),
        "tp":      float(order.tp),
        "magic":   int(order.magic),
        "comment": str(order.comment),
        "pending": True,
    }


def _trade_type_from_mt5(mt5_type: int) -> TradeType:
    return {
        0: TradeType.BUY,
        1: TradeType.SELL,
        2: TradeType.BUY_LIMIT,
        3: TradeType.SELL_LIMIT,
        4: TradeType.BUY_STOP,
        5: TradeType.SELL_STOP,
    }.get(mt5_type, TradeType.BUY)


class MasterWatcher:
    """
    One instance per master account. Runs as a single asyncio task.
    """

    def __init__(self, master_id: str, login: int, investor_password: str,
                 server: str, terminal_path: Optional[str], router: "CopyRouter"):
        self.master_id         = master_id
        self.login             = login
        self.investor_password = investor_password
        self.server            = server
        self.terminal_path     = terminal_path  # path to portable terminal exe; None = shared
        self.router            = router

        self._snapshot: dict[int, dict] = {}   # ticket → position/order dict
        self._baseline: set[int] = set()        # tickets existing at startup
        self._connected    = False
        self._stop_event   = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self):
        self._task = asyncio.create_task(self._run(), name=f"watcher:{self.master_id}")
        logger.info(f"[watcher:{self.master_id}] task started")

    def stop(self):
        self._stop_event.set()
        if self._task:
            self._task.cancel()

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def _run(self):
        loop = asyncio.get_event_loop()
        retries = 0

        while not self._stop_event.is_set():
            if not self._connected:
                ok = await loop.run_in_executor(None, self._connect)
                if ok:
                    self._connected = True
                    retries = 0
                    await self._establish_baseline(loop)
                else:
                    retries += 1
                    if retries >= MAX_CONNECT_RETRIES:
                        logger.error(f"[watcher:{self.master_id}] giving up after {retries} connect failures")
                        break
                    logger.warning(f"[watcher:{self.master_id}] connect failed, retry {retries}/{MAX_CONNECT_RETRIES}")
                    await asyncio.sleep(CONNECT_RETRY)
                    continue

            try:
                new_snapshot = await loop.run_in_executor(None, self._poll)
                if new_snapshot is None:
                    # IPC lost
                    self._connected = False
                    logger.warning(f"[watcher:{self.master_id}] IPC lost, will reconnect")
                    await asyncio.sleep(CONNECT_RETRY)
                    continue

                await self._process_diff(self._snapshot, new_snapshot)
                self._snapshot = new_snapshot

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[watcher:{self.master_id}] poll error: {exc}")
                self._connected = False

            await asyncio.sleep(POLL_INTERVAL)

        await loop.run_in_executor(None, self._disconnect)
        logger.info(f"[watcher:{self.master_id}] stopped")

    # ── MT5 calls (run in executor) ───────────────────────────────────────────

    def _connect(self) -> bool:
        if not MT5_AVAILABLE:
            logger.info(f"[watcher:{self.master_id}] MT5 not available — simulation mode")
            return True
        try:
            kwargs = dict(login=self.login, password=self.investor_password,
                          server=self.server, timeout=60_000)
            if self.terminal_path:
                kwargs["path"] = self.terminal_path
            ok = mt5.initialize(**kwargs)
            if not ok:
                logger.debug(f"[watcher:{self.master_id}] mt5.initialize failed: {mt5.last_error()}")
                return False
            info = mt5.account_info()
            if not info or info.login != self.login:
                logger.debug(f"[watcher:{self.master_id}] login mismatch after initialize")
                mt5.shutdown()
                return False
            logger.info(f"[watcher:{self.master_id}] connected login={self.login} server={self.server}")
            return True
        except Exception as exc:
            logger.error(f"[watcher:{self.master_id}] connect exception: {exc}")
            return False

    def _disconnect(self):
        if MT5_AVAILABLE:
            try:
                mt5.shutdown()
            except Exception:
                pass

    def _poll(self) -> Optional[dict[int, dict]]:
        """Return current snapshot dict, or None if IPC is lost."""
        if not MT5_AVAILABLE:
            return {}   # empty snapshot in sim mode
        try:
            positions = mt5.positions_get() or []
            orders    = mt5.orders_get()    or []
            snapshot = {}
            for p in positions:
                d = _pos_to_dict(p)
                snapshot[d["ticket"]] = d
            for o in orders:
                d = _order_to_dict(o)
                snapshot[d["ticket"]] = d
            return snapshot
        except Exception as exc:
            logger.error(f"[watcher:{self.master_id}] poll exception: {exc}")
            return None

    # ── Baseline ──────────────────────────────────────────────────────────────

    async def _establish_baseline(self, loop: asyncio.AbstractEventLoop):
        """
        On first connect (or restart), capture existing positions as baseline.
        These tickets will never generate trade intents — only changes from
        this point forward are actionable.
        """
        snap = await loop.run_in_executor(None, self._poll)
        if snap is None:
            snap = {}

        # Merge with any persisted baseline from a previous run
        persisted = db.get_watcher_baseline(self.master_id)
        combined  = set(snap.keys()) | persisted

        if combined:
            db.save_watcher_baseline(self.master_id, {t: snap.get(t, {}) for t in combined})
            logger.info(f"[watcher:{self.master_id}] baseline recorded: {len(combined)} ticket(s)")

        self._baseline = combined
        self._snapshot = snap

    # ── Diff engine ───────────────────────────────────────────────────────────

    async def _process_diff(self, old: dict[int, dict], new: dict[int, dict]):
        old_tickets = set(old.keys())
        new_tickets = set(new.keys())

        opened  = new_tickets - old_tickets - self._baseline
        closed  = old_tickets - new_tickets - self._baseline
        common  = old_tickets & new_tickets - self._baseline

        for ticket in opened:
            await self._emit_open(new[ticket])

        for ticket in closed:
            await self._emit_close(old[ticket])

        for ticket in common:
            await self._emit_modify_if_changed(old[ticket], new[ticket])

        # Once a baseline ticket is no longer present, remove it from baseline
        gone_baseline = self._baseline - new_tickets
        if gone_baseline:
            self._baseline -= gone_baseline
            # Prune from DB as well so they don't reappear after restart
            remaining = {t: new.get(t, {}) for t in self._baseline}
            db.save_watcher_baseline(self.master_id, remaining)

    # ── Intent emitters ───────────────────────────────────────────────────────

    async def _emit_open(self, pos: dict):
        logger.info(f"[watcher:{self.master_id}] OPEN detected ticket={pos['ticket']} {pos.get('symbol')} {pos.get('type')}")
        intent_id = str(uuid.uuid4())[:12]
        trade_type = _trade_type_from_mt5(pos["type"])
        signal = TradeSignal(
            signal_id    = intent_id,
            symbol       = pos["symbol"],
            type         = trade_type,
            volume       = pos["volume"],
            price        = pos["price_open"],
            sl           = pos["sl"],
            tp           = pos["tp"],
            magic_number = pos.get("magic", 0) or self._magic_number_for_master(),
            comment      = pos.get("comment", "OmniRoute"),
        )
        try:
            await self.router.route_signal(signal, time.perf_counter())
        except Exception as exc:
            logger.error(f"[watcher:{self.master_id}] route_signal failed: {exc}")

    async def _emit_close(self, pos: dict):
        logger.info(f"[watcher:{self.master_id}] CLOSE detected ticket={pos['ticket']} {pos.get('symbol')}")
        magic = pos.get("magic", 0) or self._magic_number_for_master()
        try:
            await self.router.route_close(magic, pos["symbol"])
        except Exception as exc:
            logger.error(f"[watcher:{self.master_id}] route_close failed: {exc}")

    async def _emit_modify_if_changed(self, old: dict, new: dict):
        sl_changed = abs(old.get("sl", 0) - new.get("sl", 0)) > 1e-7
        tp_changed = abs(old.get("tp", 0) - new.get("tp", 0)) > 1e-7
        if not (sl_changed or tp_changed):
            return
        logger.info(
            f"[watcher:{self.master_id}] MODIFY detected ticket={new['ticket']} "
            f"SL {old.get('sl')}→{new.get('sl')} TP {old.get('tp')}→{new.get('tp')}"
        )
        intent_id = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                    f"modify|{self.master_id}|{new['ticket']}|{time.time()}"))
        magic = new.get("magic", 0) or self._magic_number_for_master()
        modify = ModifySignal(
            magic_number = magic,
            symbol       = new["symbol"],
            new_sl       = new["sl"],
            new_tp       = new["tp"],
            master_price = new["price_open"],
        )
        try:
            await self.router.route_modify(intent_id, modify)
        except Exception as exc:
            logger.error(f"[watcher:{self.master_id}] route_modify failed: {exc}")

    def _magic_number_for_master(self) -> int:
        state = self.router.masters.get(self.master_id)
        return state.account.magic_number if state else 0
