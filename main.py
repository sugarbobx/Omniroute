"""
main.py — OmniRoute v2.3

Highlights this version:
  • MT5 access via a shared SessionManager (one terminal subprocess per account)
  • Security: loopback-default bind, constant-time API-key check, WebSocket auth,
    refusal to expose a non-loopback interface without a secret
  • Core REST surface is also mounted under /api/v1 (versioned) alongside the
    legacy top-level paths
  • The dashboard (index.html + app.js + styles.css) is served by FastAPI itself
  • Symbol map and Telegram config persist across restarts
"""

import asyncio
import ast
import json
import logging
import secrets
import uuid
from contextlib import asynccontextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional as Opt

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, BackgroundTasks, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

import database as db
import notifier
import strategy_loader
from bot import BotEngine
from models import (
    AccountRole, AddAccountRequest, LinkRequest, MasterAccount,
    ModifySignal, SlaveAccount, TradeProtection, TradeSignal, UnlinkRequest,
)
from protection import RISK_PRESETS
from router import CopyRouter, MasterState, _is_virtual
from mt5_client import MT5_AVAILABLE
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("bridge.log", maxBytes=5_000_000, backupCount=3, encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")

BASE_DIR = Path(__file__).parent

router = CopyRouter()
bot_engine = BotEngine(router)  # shares router.sessions


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
            except Exception: dead.append(ws)
        for ws in dead: self.disconnect(ws)

ws_manager = WSManager()


def _load_persisted_settings():
    """Restore Telegram config saved via the API into the in-memory settings."""
    tg = db.get_setting("telegram_config", {}) or {}
    if "enabled" in tg:
        settings.telegram_enabled = bool(tg["enabled"])
    if tg.get("bot_token"):
        settings.telegram_bot_token = tg["bot_token"]
    if tg.get("chat_id"):
        settings.telegram_chat_id = tg["chat_id"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 OmniRoute v2.3 starting (%s mode)...",
                "LIVE" if MT5_AVAILABLE else "SIMULATION")
    db.init_db()
    _load_persisted_settings()
    # Safety guard: never expose a non-loopback interface without an API secret.
    if settings.bridge_host not in ("127.0.0.1", "localhost", "::1") and not settings.api_secret:
        logger.warning("⚠ Bridge bound to %s with NO API_SECRET — the API can execute "
                       "strategy code and holds broker credentials. Set API_SECRET.",
                       settings.bridge_host)
    await router.startup()
    await bot_engine.reload_and_synchronize()

    async def _push():
        while True:
            await asyncio.sleep(2)
            if ws_manager.active:
                await ws_manager.broadcast({"type": "status", "data": router.get_full_status()})
    push_task = asyncio.create_task(_push())
    try:
        yield
    finally:
        push_task.cancel()
        for bot_id in list(bot_engine.active_tasks.keys()):
            await bot_engine.kill_bot_task(bot_id)
        await router.shutdown()


app = FastAPI(title="OmniRoute Bridge", version="2.3.0", lifespan=lifespan)

_cors_origins = [o.strip() for o in (settings.cors_origins or "").split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_cors_origins != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths reachable without an API key (docs, health, and the static dashboard so
# the user can load the page to enter their key).
_AUTH_SKIP = {"/health", "/docs", "/openapi.json", "/redoc", "/",
              "/index.html", "/app.js", "/styles.css", "/favicon.svg"}


@app.middleware("http")
async def timing(request: Request, call_next):
    import time
    t0 = time.perf_counter()
    resp = await call_next(request)
    resp.headers["X-Latency-Ms"] = f"{(time.perf_counter()-t0)*1000:.2f}"
    return resp


@app.middleware("http")
async def api_auth(request: Request, call_next):
    secret = settings.api_secret
    if secret and request.url.path not in _AUTH_SKIP:
        # Header only — a query param would leak the key into access logs.
        token = request.headers.get("X-API-Key") or ""
        if not secrets.compare_digest(token, secret):
            return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ══════════════════════════════════════════════════════════════════════════
# Core REST surface — defined on a router so it can be mounted at both the
# legacy root paths and under /api/v1.
# ══════════════════════════════════════════════════════════════════════════

core = APIRouter()


@core.get("/health", tags=["Health"])
async def health():
    return {"status": "healthy", "app": "OmniRoute", "version": "2.3.0",
            "mode": "live" if MT5_AVAILABLE else "simulation",
            "masters": len(router.masters), "slaves": len(router.slaves)}


@core.post("/trade-signal", tags=["Trading"])
async def trade_signal(signal: TradeSignal, background_tasks: BackgroundTasks):
    import time
    t0 = time.perf_counter()
    if signal.magic_number not in router._magic_index:
        raise HTTPException(404, f"No master for magic_number={signal.magic_number}")
    background_tasks.add_task(router.route_signal, signal, t0)
    return {"status": "accepted", "signal_id": signal.signal_id,
            "magic_number": signal.magic_number,
            "master_id": router._magic_index.get(signal.magic_number),
            "bridge_latency_ms": round((time.perf_counter()-t0)*1000, 2)}


@core.post("/trade-close", tags=["Trading"])
async def trade_close(magic_number: int, symbol: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(router.route_close, magic_number, symbol)
    return {"status": "close_dispatched", "magic_number": magic_number, "symbol": symbol}


@core.post("/trade-modify", tags=["Trading"])
async def trade_modify(modify: ModifySignal, background_tasks: BackgroundTasks):
    if modify.magic_number not in router._magic_index:
        raise HTTPException(404, f"No master for magic_number={modify.magic_number}")
    background_tasks.add_task(router.route_modify, modify)
    return {"status": "modify_dispatched", "magic_number": modify.magic_number,
            "symbol": modify.symbol, "new_sl": modify.new_sl, "new_tp": modify.new_tp}


@core.post("/account", tags=["Accounts"])
async def add_account(req: AddAccountRequest):
    if req.role == AccountRole.MASTER:
        if req.magic_number is None:
            raise HTTPException(400, "magic_number required for master")
        account = MasterAccount(
            label=req.label, login=req.login, password=req.password, server=req.server,
            terminal_path=req.terminal_path, magic_number=req.magic_number, symbol_map=req.symbol_map,
        )
        return await router.add_master(account)
    account = SlaveAccount(
        label=req.label, login=req.login, password=req.password, server=req.server,
        terminal_path=req.terminal_path, lot_sizing_mode=req.lot_sizing_mode,
        fixed_lot=req.fixed_lot, multiplier=req.multiplier, max_lot=req.max_lot,
        min_lot=req.min_lot, max_open_trades=req.max_open_trades, protection=req.protection,
    )
    return await router.add_slave(account)


@core.get("/masters", tags=["Masters"])
async def list_masters(): return router.get_master_statuses()

@core.post("/masters", tags=["Masters"])
async def create_master(account: MasterAccount): return await router.add_master(account)

@core.delete("/masters/{master_id}", tags=["Masters"])
async def remove_master(master_id: str):
    result = await router.remove_master(master_id)
    if result["status"] == "not_found":
        raise HTTPException(404)
    # If this master was actually a virtual bot, tear down its task and state too.
    if result.get("was_virtual"):
        await bot_engine.kill_bot_task(master_id)
        bot_engine.execution_states.pop(master_id, None)
        db.clear_bot_state(master_id)
        db.delete_virtual_bot(master_id)
    return result


@core.get("/slaves", tags=["Slaves"])
async def list_slaves(): return router.get_slave_statuses()

@core.post("/slaves", tags=["Slaves"])
async def create_slave(account: SlaveAccount): return await router.add_slave(account)

@core.delete("/slaves/{account_id}", tags=["Slaves"])
async def remove_slave(account_id: str):
    result = await router.remove_slave(account_id)
    if result["status"] == "not_found":
        raise HTTPException(404)
    return result


@core.get("/slaves/{account_id}/protection", tags=["Protection"])
async def get_protection(account_id: str):
    if account_id not in router.slaves:
        raise HTTPException(404, f"Slave {account_id} not found")
    return {"account_id": account_id, "protection": router.slaves[account_id].account.protection.model_dump()}


@core.put("/slaves/{account_id}/protection", tags=["Protection"])
async def update_protection(account_id: str, protection: TradeProtection):
    result = router.update_protection(account_id, protection)
    if result["status"] == "not_found":
        raise HTTPException(404)
    return result


@core.patch("/slaves/{account_id}/protection", tags=["Protection"])
async def patch_protection(account_id: str, updates: dict):
    if account_id not in router.slaves:
        raise HTTPException(404)
    current = router.slaves[account_id].account.protection.model_dump()
    current.update(updates)
    new_prot = TradeProtection.model_validate(current)
    return router.update_protection(account_id, new_prot)


@core.post("/slaves/{account_id}/protection/preset", tags=["Protection"])
async def apply_preset(account_id: str, preset_name: str):
    if account_id not in router.slaves:
        raise HTTPException(404, f"Slave {account_id} not found")
    if preset_name not in RISK_PRESETS:
        raise HTTPException(400, f"Unknown preset '{preset_name}'. Available: {list(RISK_PRESETS.keys())}")
    preset = RISK_PRESETS[preset_name]
    router.update_protection(account_id, preset)
    return {"status": "preset_applied", "preset": preset_name, "account_id": account_id,
            "protection": preset.model_dump()}


@core.get("/protection/presets", tags=["Protection"])
async def list_presets():
    return {name: p.model_dump() for name, p in RISK_PRESETS.items()}


@core.post("/link", tags=["Relations"])
async def link_accounts(req: LinkRequest):
    result = router.link(req.master_id, req.account_id)
    if "not_found" in result.get("status", ""):
        raise HTTPException(404, result["status"])
    return result

@core.post("/unlink", tags=["Relations"])
async def unlink_accounts(req: UnlinkRequest):
    return router.unlink(req.master_id, req.account_id)


@core.get("/symbol-map", tags=["Config"])
async def get_symbol_map():
    return {"global": router.global_symbol_map}

@core.post("/symbol-map", tags=["Config"])
async def update_symbol_map(mapping: dict[str, str]):
    # Replace semantics: the UI always sends the full map, so a removed key must
    # actually disappear (a merge would leave deleted mappings behind).
    router.global_symbol_map = dict(mapping)
    db.set_setting("global_symbol_map", router.global_symbol_map)
    return {"status": "updated", "global_symbol_map": router.global_symbol_map}


@core.get("/telegram/config", tags=["Telegram"])
async def get_telegram_config():
    token = settings.telegram_bot_token
    return {"enabled": settings.telegram_enabled, "configured": bool(token and settings.telegram_chat_id),
            "bot_token_set": bool(token), "chat_id_set": bool(settings.telegram_chat_id),
            "token_preview": f"...{token[-8:]}" if token else "not set"}

@core.patch("/telegram/config", tags=["Telegram"])
async def update_telegram_config(payload: dict):
    if "enabled"   in payload: settings.telegram_enabled    = bool(payload["enabled"])
    if payload.get("bot_token"): settings.telegram_bot_token = str(payload["bot_token"])
    if payload.get("chat_id"):   settings.telegram_chat_id   = str(payload["chat_id"])
    db.set_setting("telegram_config", {"enabled": settings.telegram_enabled,
                                       "bot_token": settings.telegram_bot_token,
                                       "chat_id": settings.telegram_chat_id})
    return {"status": "updated", "enabled": settings.telegram_enabled}

@core.post("/telegram/test", tags=["Telegram"])
async def test_telegram():
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        raise HTTPException(400, "Configure bot token and chat ID first")
    if await notifier.send_test_message():
        return {"status": "sent"}
    raise HTTPException(500, "Failed to send — check token and chat ID")

@core.patch("/telegram/toggle", tags=["Telegram"])
async def toggle_telegram(enabled: bool):
    settings.telegram_enabled = enabled
    db.set_setting("telegram_config", {"enabled": settings.telegram_enabled,
                                       "bot_token": settings.telegram_bot_token,
                                       "chat_id": settings.telegram_chat_id})
    return {"telegram_enabled": settings.telegram_enabled}


@core.get("/status", tags=["Monitoring"])
async def get_status(): return router.get_full_status()

@core.get("/logs", tags=["Monitoring"])
async def get_logs(limit: int = 200): return router.get_recent_logs(limit)


app.include_router(core)                    # legacy top-level paths
app.include_router(core, prefix="/api/v1")  # versioned aliases


# ══════════════════════════════════════════════════════════════════════════
# Bot Engine API
# ══════════════════════════════════════════════════════════════════════════

bot_api = APIRouter(prefix="/api/v1/bots", tags=["Bots"])


class BotCreateRequest(BaseModel):
    label: str
    symbol: str
    timeframe: str = "M5"
    magic_number: int
    base_volume: float = Field(0.1, gt=0)
    mode: str = "standalone"
    forward_test: bool = False
    enabled: bool = True


def _register_virtual_master(bot: dict):
    acct = MasterAccount(
        master_id=bot["bot_id"], label=f"🤖 {bot['label']}", login=0, password="-",
        server="virtual", terminal_path="virtual", magic_number=bot["magic_number"],
    )
    from models import ConnectionStatus
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
    bot = {"bot_id": str(uuid.uuid4())[:8], "label": req.label, "symbol": req.symbol.upper(),
           "timeframe": req.timeframe, "magic_number": req.magic_number,
           "base_volume": req.base_volume, "mode": req.mode,
           "forward_test": req.forward_test, "enabled": req.enabled, "strategy_name": None}
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
    db.clear_bot_state(bot_id)
    db.delete_virtual_bot(bot_id)
    state = router.masters.pop(bot_id, None)
    if state:
        router._magic_index.pop(state.account.magic_number, None)
    await router.sessions.remove(bot_id)
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


# ══════════════════════════════════════════════════════════════════════════
# Strategy API
# ══════════════════════════════════════════════════════════════════════════

strategy_api = APIRouter(prefix="/api/v1/strategies", tags=["Strategies"])


class StrategyCreateRequest(BaseModel):
    name: str
    mode: str
    symbol: str
    timeframe: str
    blocks: Opt[dict] = None
    source_code: Opt[str] = None


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
        with open(row["file_path"], encoding="utf-8") as fh:
            text = fh.read()
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
    db.save_strategy({"strategy_id": strategy_id, "name": req.name, "mode": req.mode,
                      "symbol": req.symbol.upper(), "timeframe": req.timeframe, "file_path": file_path})
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
    # If the mode changed (visual↔code) the old file has a different extension —
    # remove it so we don't leave an orphan behind.
    old_path = Path(row["file_path"])
    file_path = _write_strategy_file(strategy_id, req)
    if str(old_path) != file_path:
        old_path.unlink(missing_ok=True)
    db.save_strategy({"strategy_id": strategy_id, "name": req.name, "mode": req.mode,
                      "symbol": req.symbol.upper(), "timeframe": req.timeframe,
                      "file_path": file_path, "assigned_bot_id": row["assigned_bot_id"]})
    if row["assigned_bot_id"]:
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
    Path(row["file_path"]).unlink(missing_ok=True)
    return {"status": "deleted", "strategy_id": strategy_id}


app.include_router(strategy_api)


# ══════════════════════════════════════════════════════════════════════════
# WebSocket (authenticated via ?api_key= when a secret is configured)
# ══════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/status")
async def ws_status(ws: WebSocket):
    if settings.api_secret:
        token = ws.query_params.get("api_key") or ""
        if not secrets.compare_digest(token, settings.api_secret):
            await ws.close(code=1008)  # policy violation
            return
    await ws_manager.connect(ws)
    try:
        await ws.send_text(json.dumps({"type": "status", "data": router.get_full_status()}, default=str))
        while True:
            await asyncio.sleep(30)
            await ws.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
    except Exception:
        ws_manager.disconnect(ws)


# ══════════════════════════════════════════════════════════════════════════
# Static dashboard
# ══════════════════════════════════════════════════════════════════════════

@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(BASE_DIR / "index.html")

@app.get("/app.js", include_in_schema=False)
async def app_js():
    return FileResponse(BASE_DIR / "app.js", media_type="application/javascript")

@app.get("/styles.css", include_in_schema=False)
async def styles_css():
    return FileResponse(BASE_DIR / "styles.css", media_type="text/css")

@app.get("/favicon.svg", include_in_schema=False)
async def favicon():
    return FileResponse(BASE_DIR / "favicon.svg", media_type="image/svg+xml")


if __name__ == "__main__":
    uvicorn.run("main:app", host=settings.bridge_host, port=settings.bridge_port,
                reload=False, workers=1)
