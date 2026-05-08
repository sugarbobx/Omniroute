"""
main.py — OmniRoute Bridge v2.1
New: Telegram notification endpoints + toggle
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import database as db
import notifier
from models import (
    AccountRole, AddAccountRequest, LinkRequest,
    MasterAccount, SlaveAccount, TradeSignal, UnlinkRequest,
)
from router import CopyRouter
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bridge.log", encoding="utf-8")],
)
logger = logging.getLogger("main")

router = CopyRouter()


class WSManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept(); self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        if ws in self.active: self.active.remove(ws)
    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try: await ws.send_text(json.dumps(data, default=str))
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

ws_manager = WSManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 OmniRoute Bridge starting...")
    db.init_db()
    await router.startup()
    async def _push():
        while True:
            await asyncio.sleep(2)
            if ws_manager.active:
                await ws_manager.broadcast({"type": "status", "data": router.get_full_status()})
    asyncio.create_task(_push())
    yield
    logger.info("🛑 OmniRoute shutting down...")
    await router.shutdown()


app = FastAPI(title="OmniRoute Bridge", version="2.1.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    resp.headers["X-Latency-Ms"] = f"{(time.perf_counter()-t0)*1000:.2f}"
    return resp


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "app": "OmniRoute", "masters": len(router.masters), "slaves": len(router.slaves), "timestamp": datetime.utcnow().isoformat()}


# ── Trade signals ─────────────────────────────────────────────────────────────

@app.post("/trade-signal", tags=["Trading"])
async def trade_signal(signal: TradeSignal, background_tasks: BackgroundTasks):
    t0 = time.perf_counter()
    if signal.magic_number not in router._magic_index:
        raise HTTPException(404, f"No master for magic_number={signal.magic_number}")
    background_tasks.add_task(router.route_signal, signal, t0)
    return {"status": "accepted", "signal_id": signal.signal_id, "magic_number": signal.magic_number, "master_id": router._magic_index.get(signal.magic_number), "bridge_latency_ms": round((time.perf_counter()-t0)*1000, 2)}


@app.post("/trade-close", tags=["Trading"])
async def trade_close(magic_number: int, symbol: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(router.route_close, magic_number, symbol)
    return {"status": "close_dispatched", "magic_number": magic_number, "symbol": symbol}


# ── Unified account ───────────────────────────────────────────────────────────

@app.post("/account", tags=["Accounts"])
async def add_account(req: AddAccountRequest):
    if req.role == AccountRole.MASTER:
        if req.magic_number is None:
            raise HTTPException(400, "magic_number required for master")
        account = MasterAccount(label=req.label, login=req.login, password=req.password, server=req.server, terminal_path=req.terminal_path, magic_number=req.magic_number, symbol_map=req.symbol_map)
        return await router.add_master(account)
    else:
        account = SlaveAccount(label=req.label, login=req.login, password=req.password, server=req.server, terminal_path=req.terminal_path, lot_sizing_mode=req.lot_sizing_mode, fixed_lot=req.fixed_lot, multiplier=req.multiplier, max_lot=req.max_lot, min_lot=req.min_lot, max_open_trades=req.max_open_trades)
        return await router.add_slave(account)


# ── Masters ───────────────────────────────────────────────────────────────────

@app.get("/masters", tags=["Masters"])
async def list_masters(): return router.get_master_statuses()

@app.post("/masters", tags=["Masters"])
async def add_master(account: MasterAccount): return await router.add_master(account)

@app.delete("/masters/{master_id}", tags=["Masters"])
async def remove_master(master_id: str):
    result = router.remove_master(master_id)
    if result["status"] == "not_found": raise HTTPException(404)
    return result


# ── Slaves ────────────────────────────────────────────────────────────────────

@app.get("/slaves", tags=["Slaves"])
async def list_slaves(): return router.get_slave_statuses()

@app.post("/slaves", tags=["Slaves"])
async def add_slave(account: SlaveAccount): return await router.add_slave(account)

@app.delete("/slaves/{account_id}", tags=["Slaves"])
async def remove_slave(account_id: str):
    result = router.remove_slave(account_id)
    if result["status"] == "not_found": raise HTTPException(404)
    return result


# ── Links ─────────────────────────────────────────────────────────────────────

@app.post("/link", tags=["Relations"])
async def link_accounts(req: LinkRequest):
    result = router.link(req.master_id, req.account_id)
    if "not_found" in result.get("status", ""): raise HTTPException(404, result["status"])
    return result

@app.post("/unlink", tags=["Relations"])
async def unlink_accounts(req: UnlinkRequest):
    return router.unlink(req.master_id, req.account_id)


# ── Symbol map ────────────────────────────────────────────────────────────────

@app.get("/symbol-map", tags=["Config"])
async def get_symbol_map(): return {"global": router.global_symbol_map}

@app.post("/symbol-map", tags=["Config"])
async def update_symbol_map(mapping: dict[str, str]):
    router.global_symbol_map.update(mapping)
    return {"status": "updated", "global_symbol_map": router.global_symbol_map}


# ── Telegram ──────────────────────────────────────────────────────────────────

@app.get("/telegram/config", tags=["Telegram"])
async def get_telegram_config():
    """Return current Telegram config (token masked)."""
    token = settings.telegram_bot_token
    return {
        "enabled":    settings.telegram_enabled,
        "configured": bool(token and settings.telegram_chat_id),
        "bot_token_set": bool(token),
        "chat_id_set":   bool(settings.telegram_chat_id),
        "token_preview": f"...{token[-8:]}" if token else "not set",
    }


@app.patch("/telegram/config", tags=["Telegram"])
async def update_telegram_config(payload: dict):
    """
    Update Telegram settings at runtime.
    Accepted keys: enabled (bool), bot_token (str), chat_id (str)
    """
    if "enabled" in payload:
        settings.telegram_enabled = bool(payload["enabled"])
    if "bot_token" in payload and payload["bot_token"]:
        settings.telegram_bot_token = str(payload["bot_token"])
    if "chat_id" in payload and payload["chat_id"]:
        settings.telegram_chat_id = str(payload["chat_id"])
    return {"status": "updated", "enabled": settings.telegram_enabled}


@app.post("/telegram/test", tags=["Telegram"])
async def test_telegram():
    """Send a test message to verify the Telegram integration."""
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        raise HTTPException(400, "Bot token and chat ID must be configured before testing")
    success = await notifier.send_test_message()
    if success:
        return {"status": "sent", "message": "Test message delivered to Telegram ✓"}
    else:
        raise HTTPException(500, "Failed to send test message — check bot token and chat ID")


@app.patch("/telegram/toggle", tags=["Telegram"])
async def toggle_telegram(enabled: bool):
    """Quick toggle endpoint for the dashboard switch."""
    settings.telegram_enabled = enabled
    return {"status": "updated", "telegram_enabled": settings.telegram_enabled}


# ── Status / logs ─────────────────────────────────────────────────────────────

@app.get("/status", tags=["Monitoring"])
async def get_status(): return router.get_full_status()

@app.get("/logs", tags=["Monitoring"])
async def get_logs(limit: int = 200): return router.get_recent_logs(limit)


# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/status")
async def ws_status(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        await ws.send_text(json.dumps({"type": "status", "data": router.get_full_status()}, default=str))
        while True:
            await asyncio.sleep(30)
            await ws.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False, workers=1)
