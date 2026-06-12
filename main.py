"""
main.py — OmniRoute v2.2
New endpoints:
  POST /trade-modify          — SL/TP sync from Master EA
  GET/PUT /slaves/{id}/protection — per-slave protection config
  POST /slaves/{id}/protection/preset — apply a named preset
  GET  /protection/presets    — list all built-in risk presets
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import ast
import uuid

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, BackgroundTasks, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional as Opt

import database as db
import notifier
import strategy_loader
from bot import BotEngine
from models import (
    AccountRole, AddAccountRequest, ConnectionStatus, LinkRequest, MasterAccount,
    ModifySignal, SlaveAccount, TradeProtection, TradeSignal, UnlinkRequest,
)
from protection import RISK_PRESETS
from router import CopyRouter, MasterState
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bridge.log", encoding="utf-8")],
)
logger = logging.getLogger("main")

router = CopyRouter()
bot_engine = BotEngine(router)


class WSManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    async def connect(self, ws):
        await ws.accept(); self.active.append(ws)
    def disconnect(self, ws):
        if ws in self.active: self.active.remove(ws)
    async def broadcast(self, data):
        dead = []
        for ws in self.active:
            try: await ws.send_text(json.dumps(data, default=str))
            except: dead.append(ws)
        for ws in dead: self.disconnect(ws)

ws_manager = WSManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 OmniRoute v2.3 starting...")
    db.init_db()
    await router.startup()
    await bot_engine.reload_and_synchronize()
    async def _push():
        while True:
            await asyncio.sleep(2)
            if ws_manager.active:
                await ws_manager.broadcast({"type": "status", "data": router.get_full_status()})
    # Store the task to prevent garbage collection
    push_task = asyncio.create_task(_push())
    try:
        yield
    finally:
        push_task.cancel()
        for bot_id in list(bot_engine.active_tasks.keys()):
            await bot_engine.kill_bot_task(bot_id)
        await router.shutdown()


app = FastAPI(title="OmniRoute Bridge", version="2.2.0", lifespan=lifespan)
# allow_credentials=True is incompatible with allow_origins=["*"] per the CORS spec.
# Use explicit origins (from env) or allow all without credentials.
_cors_origins = [o.strip() for o in (settings.cors_origins or "").split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    resp.headers["X-Latency-Ms"] = f"{(time.perf_counter()-t0)*1000:.2f}"
    return resp


@app.middleware("http")
async def api_auth(request: Request, call_next):
    secret = settings.api_secret
    if secret:
        # Skip auth for health check and docs
        if request.url.path not in ("/health", "/docs", "/openapi.json", "/redoc"):
            token = request.headers.get("X-API-Key") or request.query_params.get("api_key")
            if token != secret:
                from fastapi.responses import JSONResponse
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "app": "OmniRoute", "version": "2.2.0",
            "masters": len(router.masters), "slaves": len(router.slaves)}


# ── Trade signals ─────────────────────────────────────────────────────────────

@app.post("/trade-signal", tags=["Trading"])
async def trade_signal(signal: TradeSignal, background_tasks: BackgroundTasks):
    t0 = time.perf_counter()
    if signal.magic_number not in router._magic_index:
        raise HTTPException(404, f"No master for magic_number={signal.magic_number}")
    background_tasks.add_task(router.route_signal, signal, t0)
    return {
        "status": "accepted", "signal_id": signal.signal_id,
        "magic_number": signal.magic_number,
        "master_id": router._magic_index.get(signal.magic_number),
        "bridge_latency_ms": round((time.perf_counter()-t0)*1000, 2),
    }


@app.post("/trade-close", tags=["Trading"])
async def trade_close(magic_number: int, symbol: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(router.route_close, magic_number, symbol)
    return {"status": "close_dispatched", "magic_number": magic_number, "symbol": symbol}


@app.post("/trade-modify", tags=["Trading"])
async def trade_modify(modify: ModifySignal, background_tasks: BackgroundTasks):
    """
    Called by the Master EA when a position's SL or TP is changed.
    Routes the modification to all linked slaves, applying per-slave
    SL/TP scaling and offset transformations.

    MQL5 snippet to call this endpoint:
      OnTradeTransaction → TRADE_TRANSACTION_POSITION
      payload: {magic_number, symbol, new_sl, new_tp, master_price}
    """
    if modify.magic_number not in router._magic_index:
        raise HTTPException(404, f"No master for magic_number={modify.magic_number}")
    background_tasks.add_task(router.route_modify, modify)
    return {
        "status": "modify_dispatched",
        "magic_number": modify.magic_number,
        "symbol": modify.symbol,
        "new_sl": modify.new_sl,
        "new_tp": modify.new_tp,
    }


# ── Accounts ──────────────────────────────────────────────────────────────────

@app.post("/account", tags=["Accounts"])
async def add_account(req: AddAccountRequest):
    if req.role == AccountRole.MASTER:
        if req.magic_number is None:
            raise HTTPException(400, "magic_number required for master")
        account = MasterAccount(
            label=req.label, login=req.login, password=req.password, server=req.server,
            terminal_path=req.terminal_path, magic_number=req.magic_number, symbol_map=req.symbol_map,
        )
        return await router.add_master(account)
    else:
        account = SlaveAccount(
            label=req.label, login=req.login, password=req.password, server=req.server,
            terminal_path=req.terminal_path, lot_sizing_mode=req.lot_sizing_mode,
            fixed_lot=req.fixed_lot, multiplier=req.multiplier, max_lot=req.max_lot,
            min_lot=req.min_lot, max_open_trades=req.max_open_trades, protection=req.protection,
        )
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


# ── Protection endpoints ──────────────────────────────────────────────────────

@app.get("/slaves/{account_id}/protection", tags=["Protection"])
async def get_protection(account_id: str):
    if account_id not in router.slaves:
        raise HTTPException(404, f"Slave {account_id} not found")
    prot = router.slaves[account_id].account.protection
    return {"account_id": account_id, "protection": prot.model_dump()}


@app.put("/slaves/{account_id}/protection", tags=["Protection"])
async def update_protection(account_id: str, protection: TradeProtection):
    """Update full protection config for a slave."""
    result = router.update_protection(account_id, protection)
    if result["status"] == "not_found":
        raise HTTPException(404)
    return result


@app.patch("/slaves/{account_id}/protection", tags=["Protection"])
async def patch_protection(account_id: str, updates: dict):
    """
    Partial update — only send the fields you want to change.
    Example: {"risk_multiplier": 0.5, "slippage_max": 2.0}
    """
    if account_id not in router.slaves:
        raise HTTPException(404)
    current = router.slaves[account_id].account.protection.model_dump()
    current.update(updates)
    new_prot = TradeProtection.model_validate(current)
    result = router.update_protection(account_id, new_prot)
    return result


@app.post("/slaves/{account_id}/protection/preset", tags=["Protection"])
async def apply_preset(account_id: str, preset_name: str):
    """
    Apply a named risk preset. Available: ultra_safe, conservative, default, aggressive, no_protection
    """
    if account_id not in router.slaves:
        raise HTTPException(404, f"Slave {account_id} not found")
    if preset_name not in RISK_PRESETS:
        raise HTTPException(400, f"Unknown preset '{preset_name}'. Available: {list(RISK_PRESETS.keys())}")
    preset = RISK_PRESETS[preset_name]
    result = router.update_protection(account_id, preset)
    return {"status": "preset_applied", "preset": preset_name, "account_id": account_id, "protection": preset.model_dump()}


@app.get("/protection/presets", tags=["Protection"])
async def list_presets():
    """List all built-in risk profile presets."""
    return {name: p.model_dump() for name, p in RISK_PRESETS.items()}


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
    token = settings.telegram_bot_token
    return {"enabled": settings.telegram_enabled, "configured": bool(token and settings.telegram_chat_id),
            "bot_token_set": bool(token), "chat_id_set": bool(settings.telegram_chat_id),
            "token_preview": f"...{token[-8:]}" if token else "not set"}

@app.patch("/telegram/config", tags=["Telegram"])
async def update_telegram_config(payload: dict):
    if "enabled"   in payload: settings.telegram_enabled    = bool(payload["enabled"])
    if "bot_token" in payload and payload["bot_token"]: settings.telegram_bot_token = str(payload["bot_token"])
    if "chat_id"   in payload and payload["chat_id"]:   settings.telegram_chat_id   = str(payload["chat_id"])
    return {"status": "updated", "enabled": settings.telegram_enabled}

@app.post("/telegram/test", tags=["Telegram"])
async def test_telegram():
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        raise HTTPException(400, "Configure bot token and chat ID first")
    success = await notifier.send_test_message()
    if success: return {"status": "sent"}
    raise HTTPException(500, "Failed to send — check token and chat ID")

@app.patch("/telegram/toggle", tags=["Telegram"])
async def toggle_telegram(enabled: bool):
    settings.telegram_enabled = enabled
    return {"telegram_enabled": settings.telegram_enabled}


# ── Status / Logs ─────────────────────────────────────────────────────────────

@app.get("/status", tags=["Monitoring"])
async def get_status(): return router.get_full_status()

@app.get("/logs", tags=["Monitoring"])
async def get_logs(limit: int = 200): return router.get_recent_logs(limit)


# ── Bot Engine API ────────────────────────────────────────────────────────────

bot_api = APIRouter(prefix="/api/v1/bots", tags=["Bots"])


class BotCreateRequest(BaseModel):
    label: str
    symbol: str
    timeframe: str = "M5"
    magic_number: int
    base_volume: float = Field(0.1, gt=0)
    mode: str = "standalone"           # standalone | connected
    forward_test: bool = False
    enabled: bool = True


def _register_virtual_master(bot: dict):
    """Expose the bot in the copier's master registry so connected-mode signals
    route through router.route_signal and the bot shows on the dashboard."""
    acct = MasterAccount(
        master_id=bot["bot_id"], label=f"🤖 {bot['label']}", login=0, password="-",
        server="virtual", terminal_path="virtual", magic_number=bot["magic_number"],
    )
    state = MasterState(acct)
    state.status = ConnectionStatus.CONNECTED
    router.masters[bot["bot_id"]] = state
    router._magic_index[bot["magic_number"]] = bot["bot_id"]


def _bot_summary(b: dict) -> dict:
    strat = db.get_strategy_for_bot(b["bot_id"])
    return {**b,
            "strategy_id": strat["strategy_id"] if strat else None,
            "strategy_name": strat["name"] if strat else None,
            **bot_engine.get_bot_status(b["bot_id"])}


@bot_api.get("")
async def list_bots():
    return [_bot_summary(b) for b in db.get_all_virtual_bots()]


@bot_api.post("")
async def create_bot(req: BotCreateRequest):
    if req.mode not in ("standalone", "connected"):
        raise HTTPException(400, "mode must be 'standalone' or 'connected'")
    if req.timeframe not in ("M1", "M5", "M15", "H1"):
        raise HTTPException(400, "timeframe must be one of M1, M5, M15, H1")
    if req.magic_number in router._magic_index:
        raise HTTPException(409, f"magic_number {req.magic_number} already in use")
    bot = {
        "bot_id": str(uuid.uuid4())[:8], "label": req.label, "symbol": req.symbol.upper(),
        "timeframe": req.timeframe, "magic_number": req.magic_number,
        "base_volume": req.base_volume, "mode": req.mode,
        "forward_test": req.forward_test, "enabled": req.enabled, "strategy_name": None,
    }
    db.save_virtual_bot(bot)
    _register_virtual_master(bot)
    await bot_engine.reload_and_synchronize()
    return {"status": "created", **_bot_summary(bot)}


@bot_api.patch("/{bot_id}")
async def update_bot(bot_id: str, updates: dict):
    if not db.get_virtual_bot(bot_id):
        raise HTTPException(404, f"Bot {bot_id} not found")
    allowed = {"label", "symbol", "timeframe", "base_volume", "magic_number",
               "mode", "forward_test", "enabled"}
    updates = {k: v for k, v in updates.items() if k in allowed}
    if "symbol" in updates:
        updates["symbol"] = str(updates["symbol"]).upper()
    if "magic_number" in updates:
        owner = router._magic_index.get(updates["magic_number"])
        if owner and owner != bot_id:
            raise HTTPException(409, f"magic_number {updates['magic_number']} already in use")
    db.update_virtual_bot(bot_id, updates)
    bot = db.get_virtual_bot(bot_id)
    # keep the copier registry in step
    old_state = router.masters.get(bot_id)
    if old_state:
        router._magic_index.pop(old_state.account.magic_number, None)
    _register_virtual_master(bot)
    await bot_engine.kill_bot_task(bot_id)
    await bot_engine.reload_and_synchronize()
    return {"status": "updated", **_bot_summary(bot)}


@bot_api.delete("/{bot_id}")
async def delete_bot(bot_id: str):
    bot = db.get_virtual_bot(bot_id)
    if not bot:
        raise HTTPException(404, f"Bot {bot_id} not found")
    await bot_engine.kill_bot_task(bot_id)
    bot_engine.execution_states.pop(bot_id, None)
    db.delete_virtual_bot(bot_id)
    state = router.masters.pop(bot_id, None)
    if state:
        router._magic_index.pop(state.account.magic_number, None)
    return {"status": "deleted", "bot_id": bot_id}


@bot_api.post("/{bot_id}/strategy")
async def assign_strategy(bot_id: str, payload: dict):
    strategy_id = payload.get("strategy_id")
    bot = db.get_virtual_bot(bot_id)
    if not bot:
        raise HTTPException(404, f"Bot {bot_id} not found")
    strat = db.get_strategy(strategy_id) if strategy_id else None
    if strategy_id and not strat:
        raise HTTPException(404, f"Strategy {strategy_id} not found")
    db.assign_strategy_to_bot(strategy_id, bot_id if strategy_id else None)
    db.update_virtual_bot(bot_id, {"strategy_name": strat["name"] if strat else None})
    await bot_engine.kill_bot_task(bot_id)
    await bot_engine.reload_and_synchronize()
    return {"status": "assigned", "bot_id": bot_id, "strategy_id": strategy_id}


@bot_api.post("/sync")
async def sync_bots():
    await bot_engine.reload_and_synchronize()
    return {"status": "synchronized", "running": list(bot_engine.active_tasks.keys())}


@bot_api.get("/{bot_id}/results")
async def bot_results(bot_id: str, limit: int = 200):
    if not db.get_virtual_bot(bot_id):
        raise HTTPException(404, f"Bot {bot_id} not found")
    return db.get_strategy_results(bot_id, limit)


app.include_router(bot_api)


# ── Strategy API ──────────────────────────────────────────────────────────────

strategy_api = APIRouter(prefix="/api/v1/strategies", tags=["Strategies"])


class StrategyCreateRequest(BaseModel):
    name: str
    mode: str                      # visual | code
    symbol: str
    timeframe: str
    blocks: Opt[dict] = None       # visual mode
    source_code: Opt[str] = None   # code mode


def _validate_strategy_payload(req: StrategyCreateRequest):
    if req.mode == "visual":
        if not req.blocks or "entry" not in req.blocks:
            raise HTTPException(400, "Visual strategy requires a 'blocks' object with an 'entry' block")
    elif req.mode == "code":
        if not req.source_code:
            raise HTTPException(400, "Code strategy requires 'source_code'")
        try:
            tree = ast.parse(req.source_code)
        except SyntaxError as e:
            raise HTTPException(400, f"Python syntax error: {e}")
        fn_names = [n.name for n in ast.walk(tree)
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        if "evaluate" not in fn_names:
            raise HTTPException(400, "Code strategy must define evaluate(market_data)")
    else:
        raise HTTPException(400, "mode must be 'visual' or 'code'")


def _write_strategy_file(strategy_id: str, req: StrategyCreateRequest) -> str:
    path = strategy_loader.strategy_file_path(strategy_id, req.mode)
    if req.mode == "visual":
        path.write_text(json.dumps(req.blocks, indent=2), encoding="utf-8")
    else:
        path.write_text(req.source_code, encoding="utf-8")
    return str(path)


def _strategy_with_content(row: dict) -> dict:
    out = dict(row)
    try:
        text = open(row["file_path"], encoding="utf-8").read()
        if row["mode"] == "visual":
            out["blocks"] = json.loads(text)
        else:
            out["source_code"] = text
    except OSError:
        out["file_missing"] = True
    return out


@strategy_api.get("")
async def list_strategies():
    return [_strategy_with_content(r) for r in db.get_all_strategies()]


@strategy_api.post("")
async def create_strategy(req: StrategyCreateRequest):
    _validate_strategy_payload(req)
    strategy_id = str(uuid.uuid4())[:8]
    file_path = _write_strategy_file(strategy_id, req)
    db.save_strategy({
        "strategy_id": strategy_id, "name": req.name, "mode": req.mode,
        "symbol": req.symbol.upper(), "timeframe": req.timeframe, "file_path": file_path,
    })
    return {"status": "created", "strategy_id": strategy_id}


@strategy_api.get("/{strategy_id}")
async def get_strategy(strategy_id: str):
    row = db.get_strategy(strategy_id)
    if not row:
        raise HTTPException(404, f"Strategy {strategy_id} not found")
    return _strategy_with_content(row)


@strategy_api.put("/{strategy_id}")
async def update_strategy(strategy_id: str, req: StrategyCreateRequest):
    row = db.get_strategy(strategy_id)
    if not row:
        raise HTTPException(404, f"Strategy {strategy_id} not found")
    _validate_strategy_payload(req)
    file_path = _write_strategy_file(strategy_id, req)
    db.save_strategy({
        "strategy_id": strategy_id, "name": req.name, "mode": req.mode,
        "symbol": req.symbol.upper(), "timeframe": req.timeframe, "file_path": file_path,
        "assigned_bot_id": row["assigned_bot_id"],
    })
    if row["assigned_bot_id"]:  # hot-reload the bot running this strategy
        await bot_engine.kill_bot_task(row["assigned_bot_id"])
        await bot_engine.reload_and_synchronize()
    return {"status": "updated", "strategy_id": strategy_id}


@strategy_api.delete("/{strategy_id}")
async def delete_strategy(strategy_id: str):
    row = db.get_strategy(strategy_id)
    if not row:
        raise HTTPException(404, f"Strategy {strategy_id} not found")
    if row["assigned_bot_id"]:
        await bot_engine.kill_bot_task(row["assigned_bot_id"])
        db.update_virtual_bot(row["assigned_bot_id"], {"strategy_name": None})
    db.delete_strategy(strategy_id)
    try:
        from pathlib import Path
        Path(row["file_path"]).unlink(missing_ok=True)
    except OSError:
        pass
    return {"status": "deleted", "strategy_id": strategy_id}


app.include_router(strategy_api)


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
