"""
mt5_client.py — OmniRoute MT5 access layer

The single entry point every other module uses to reach MetaTrader 5. It solves
two problems the old inline `import MetaTrader5` pattern could not:

  1. Multi-terminal.  The MT5 SDK is single-connection per process, so we run one
     mt5_worker.py subprocess per terminal (keyed by account/bot id). All accounts
     trade concurrently on their own terminals instead of clobbering one global
     connection.

  2. Non-blocking.  Every SDK round-trip happens in a worker and is awaited over a
     pipe via run_in_executor, so a slow broker never freezes the event loop, the
     WebSocket push, or other accounts' execution.

Simulation mode (MT5 SDK not importable, or OMNIROUTE_SIMULATE=1) keeps the whole
system functional with deterministic synthetic data — same behavior the app had
before, now centralized so router and bot agree on prices and fills.

Public surface:
    manager = SessionManager()
    sess = await manager.get_or_create(account_id, path=..., login=..., ...)
    await sess.copy_rates(symbol, "M5", 250)
    await sess.order_send(request)          # request uses STRING action/type tokens
    await sess.positions_get(symbol, magic)
    await manager.shutdown_all()
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import subprocess
import sys
import time
from typing import Optional

import instruments

logger = logging.getLogger("mt5_client")

try:
    import MetaTrader5 as _mt5  # noqa: F401  (presence check only)
    _SDK_PRESENT = True
except ImportError:
    _SDK_PRESENT = False

# Force-simulate via env even when the SDK is present (useful for dev on the VPS)
SIMULATE = os.environ.get("OMNIROUTE_SIMULATE", "").strip() in ("1", "true", "yes")
MT5_AVAILABLE = _SDK_PRESENT and not SIMULATE

TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800,
                     "H1": 3600, "H4": 14400, "D1": 86400}

RETCODE_DONE = 10009


# ══════════════════════════════════════════════════════════════════════════
# Deterministic simulated market data (shared by every sim session)
# ══════════════════════════════════════════════════════════════════════════

def sim_rates(symbol: str, timeframe: str, count: int) -> dict:
    """Deterministic synthetic OHLC: slow sine trend + hash noise, keyed by
    symbol and candle index so repeated calls in the same candle are identical."""
    tf_sec = TIMEFRAME_SECONDS.get(timeframe, 300)
    now_idx = int(time.time() // tf_sec)
    seed_base = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16)
    info = instruments.instrument_info(symbol)
    # Base price anchored to a plausible level for the asset class.
    if info.point >= 1.0:
        base_price = 1000.0 + seed_base % 40000        # indices / BTC
    elif info.point >= 0.01:
        base_price = 100.0 + seed_base % 3000 / 1.0    # gold / oil / JPY
    else:
        base_price = 1.0 + (seed_base % 2000) / 1000.0  # forex ~1.0–3.0

    def noise(idx: int) -> float:
        h = int(hashlib.md5(f"{symbol}:{idx}".encode()).hexdigest()[:8], 16)
        return (h / 0xFFFFFFFF) - 0.5

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


def sim_tick(symbol: str) -> dict:
    r = sim_rates(symbol, "M1", 2)
    price = r["close"][-1]
    spread = instruments.point_size(symbol) * 10
    return {"bid": round(price - spread / 2, 5), "ask": round(price + spread / 2, 5),
            "time": int(time.time())}


# ══════════════════════════════════════════════════════════════════════════
# Sessions
# ══════════════════════════════════════════════════════════════════════════

class TerminalError(Exception):
    pass


class _BaseSession:
    def __init__(self, account_id: str, sim_equity: float):
        self.account_id = account_id
        self.equity = sim_equity
        self.balance = sim_equity
        self.connected = False


class SimSession(_BaseSession):
    """No terminal — deterministic data and always-successful fills."""

    async def initialize(self) -> bool:
        self.connected = True
        return True

    async def account_info(self) -> dict:
        return {"equity": self.equity, "balance": self.balance}

    async def symbol_info(self, symbol: str) -> dict:
        info = instruments.instrument_info(symbol)
        return {"point": info.point, "digits": info.digits,
                "trade_contract_size": info.contract_size,
                "volume_min": 0.01, "volume_max": 100.0, "volume_step": 0.01}

    async def symbol_info_tick(self, symbol: str) -> dict:
        return sim_tick(symbol)

    async def copy_rates(self, symbol: str, timeframe: str, count: int) -> Optional[dict]:
        return sim_rates(symbol, timeframe, count)

    async def order_send(self, request: dict) -> dict:
        price = request.get("price") or sim_tick(request.get("symbol", "X"))["ask"]
        ticket = 500000 + int(time.time() * 1000) % 499999
        return {"retcode": RETCODE_DONE, "order": ticket, "price": price, "comment": "sim"}

    async def positions_get(self, symbol=None, magic=None) -> list:
        return []

    async def shutdown(self):
        self.connected = False


class WorkerSession(_BaseSession):
    """Live — owns one mt5_worker.py subprocess bound to one terminal."""

    def __init__(self, account_id, path, login, password, server, sim_equity=0.0):
        super().__init__(account_id, sim_equity)
        self._path, self._login = path, login
        self._password, self._server = password, server
        self._proc: Optional[subprocess.Popen] = None
        self._lock = asyncio.Lock()
        self._rid = 0

    async def _call(self, cmd: str, args: dict, timeout: float = 15.0):
        """Serialize one request/response round-trip with the worker."""
        if self._proc is None or self._proc.poll() is not None:
            raise TerminalError(f"worker for {self.account_id} not running")
        loop = asyncio.get_running_loop()
        async with self._lock:
            self._rid += 1
            payload = json.dumps({"id": self._rid, "cmd": cmd, "args": args}) + "\n"

            def _io():
                self._proc.stdin.write(payload)
                self._proc.stdin.flush()
                return self._proc.stdout.readline()

            line = await asyncio.wait_for(loop.run_in_executor(None, _io), timeout)
        if not line:
            raise TerminalError(f"worker for {self.account_id} closed the pipe")
        resp = json.loads(line)
        if not resp.get("ok"):
            raise TerminalError(resp.get("error", "unknown worker error"))
        return resp.get("result")

    async def initialize(self) -> bool:
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mt5_worker.py")
        self._proc = subprocess.Popen(
            [sys.executable, worker],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8", bufsize=1,
        )
        res = await self._call("initialize", {
            "path": self._path, "login": self._login,
            "password": self._password, "server": self._server, "timeout": 10000,
        }, timeout=25.0)
        if not res or not res.get("ok_init"):
            err = (res or {}).get("error", "init failed")
            raise TerminalError(err)
        self.equity = res.get("equity", 0.0)
        self.balance = res.get("balance", 0.0)
        self.connected = True
        return True

    async def account_info(self) -> Optional[dict]:
        res = await self._call("account_info", {})
        if res:
            self.equity, self.balance = res["equity"], res["balance"]
        return res

    async def symbol_info(self, symbol: str) -> Optional[dict]:
        return await self._call("symbol_info", {"symbol": symbol})

    async def symbol_info_tick(self, symbol: str) -> Optional[dict]:
        return await self._call("symbol_info_tick", {"symbol": symbol})

    async def copy_rates(self, symbol: str, timeframe: str, count: int) -> Optional[dict]:
        return await self._call("copy_rates", {"symbol": symbol,
                                               "timeframe": timeframe, "count": count})

    async def order_send(self, request: dict) -> dict:
        return await self._call("order_send", {"request": request})

    async def positions_get(self, symbol=None, magic=None) -> list:
        return await self._call("positions_get", {"symbol": symbol, "magic": magic}) or []

    async def shutdown(self):
        if self._proc and self._proc.poll() is None:
            try:
                await self._call("shutdown", {}, timeout=5.0)
            except Exception:
                pass
            try:
                self._proc.terminate()
            except Exception:
                pass
        self.connected = False


class SessionManager:
    """Owns all terminal sessions, one per account/bot id."""

    def __init__(self):
        self._sessions: dict[str, _BaseSession] = {}

    def get(self, account_id: str) -> Optional[_BaseSession]:
        return self._sessions.get(account_id)

    async def get_or_create(self, account_id: str, *, path=None, login=None,
                            password=None, server=None, sim_equity=10_000.0,
                            is_master=False) -> _BaseSession:
        existing = self._sessions.get(account_id)
        if existing and existing.connected:
            return existing
        if MT5_AVAILABLE:
            sess: _BaseSession = WorkerSession(account_id, path, login, password,
                                               server, sim_equity)
        else:
            sess = SimSession(account_id, sim_equity)
        await sess.initialize()
        self._sessions[account_id] = sess
        return sess

    def create_sim(self, account_id: str, sim_equity: float = 10_000.0) -> SimSession:
        """A never-failing sim session (used for the standalone-bot default terminal
        in sim mode, and as a fallback)."""
        sess = SimSession(account_id, sim_equity)
        sess.connected = True
        self._sessions[account_id] = sess
        return sess

    async def remove(self, account_id: str):
        sess = self._sessions.pop(account_id, None)
        if sess:
            await sess.shutdown()

    async def shutdown_all(self):
        for sess in list(self._sessions.values()):
            try:
                await sess.shutdown()
            except Exception:
                pass
        self._sessions.clear()
