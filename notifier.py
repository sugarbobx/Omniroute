"""
notifier.py — OmniRoute Telegram Notification System

Sends rich HTML-formatted alerts to a Telegram chat via Bot API.
All calls are fire-and-forget async (never block the trade path).

Events covered:
  • Trade detected on Master
  • Trade successfully copied to Slave (with lot size, ticket, latency)
  • Trade copy failed (with error reason)
  • Slave connected / disconnected
  • Master connected / disconnected
  • Bridge startup / shutdown

Configuration (via .env):
  TELEGRAM_BOT_TOKEN = 7xxxxxxxxx:AAF...
  TELEGRAM_CHAT_ID   = -100xxxxxxxxxx   (group) or 123456789 (personal)
  TELEGRAM_ENABLED   = true
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx

from config import settings

logger = logging.getLogger("notifier")

# ── Singleton HTTP client (reused across calls) ──────────────────────────────
_client: Optional[httpx.AsyncClient] = None

def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=8.0)
    return _client


# ── Core send function ───────────────────────────────────────────────────────

async def _send(html: str) -> bool:
    """
    Send an HTML-formatted message to Telegram.
    Returns True on success, False on failure.
    Never raises — always safe to await.
    """
    if not settings.telegram_enabled:
        return False
    if not settings.telegram_bot_token or not settings.telegram_chat_id:
        logger.debug("Telegram not configured — skipping notification")
        return False

    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id":    settings.telegram_chat_id,
        "text":       html,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        client = _get_client()
        r = await client.post(url, json=payload)
        if r.status_code != 200:
            logger.warning(f"Telegram API error {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as exc:
        logger.warning(f"Telegram send failed: {exc}")
        return False


def _ts() -> str:
    """UTC timestamp string for message footers."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fire(coro):
    """Schedule a coroutine without blocking the caller."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(coro)
        else:
            loop.run_until_complete(coro)
    except Exception as exc:
        logger.debug(f"Notifier fire error: {exc}")


# ════════════════════════════════════════════════════════════════════════════
# PUBLIC NOTIFICATION FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════

def notify_trade_detected(
    master_label: str,
    magic_number: int,
    symbol: str,
    trade_type: str,
    volume: float,
    price: float,
    sl: float,
    tp: float,
    signal_id: str,
):
    """Fired when a signal arrives from a Master EA."""
    direction = "🟢 BUY" if "buy" in trade_type.lower() else "🔴 SELL"
    sl_str = f"{sl:.5f}" if sl else "—"
    tp_str = f"{tp:.5f}" if tp else "—"

    msg = (
        f"📡 <b>OmniRoute — Signal Detected</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Master:</b>  {master_label}\n"
        f"<b>Magic:</b>   <code>{magic_number}</code>\n"
        f"<b>Signal:</b>  <code>{signal_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{direction}  <b>{symbol}</b>\n"
        f"<b>Volume:</b>  <code>{volume:.2f} lots</code>\n"
        f"<b>Price:</b>   <code>{price:.5f}</code>\n"
        f"<b>SL:</b>      <code>{sl_str}</code>\n"
        f"<b>TP:</b>      <code>{tp_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_trade_copied(
    master_label: str,
    slave_label: str,
    slave_id: str,
    symbol: str,
    trade_type: str,
    volume: float,
    price: float,
    ticket: int,
    latency_ms: float,
    signal_id: str,
):
    """Fired when a Slave successfully executes a copied trade."""
    direction = "🟢" if "buy" in trade_type.lower() else "🔴"
    lat_emoji = "⚡" if latency_ms < 50 else "🕐" if latency_ms < 100 else "🐢"

    msg = (
        f"✅ <b>OmniRoute — Trade Copied</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Slave:</b>   {slave_label}\n"
        f"<b>Account:</b> <code>{slave_id}</code>\n"
        f"<b>Master:</b>  {master_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{direction} {trade_type.upper()}  <b>{symbol}</b>\n"
        f"<b>Volume:</b>  <code>{volume:.2f} lots</code>\n"
        f"<b>Price:</b>   <code>{price:.5f}</code>\n"
        f"<b>Ticket:</b>  <code>#{ticket}</code>\n"
        f"<b>Signal:</b>  <code>{signal_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{lat_emoji} Latency: <code>{latency_ms:.1f}ms</code>\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_trade_failed(
    master_label: str,
    slave_label: str,
    slave_id: str,
    symbol: str,
    trade_type: str,
    error_message: str,
    error_code: Optional[int],
    signal_id: str,
):
    """Fired when a Slave fails to copy a trade."""
    code_str = f" (code <code>{error_code}</code>)" if error_code else ""

    msg = (
        f"❌ <b>OmniRoute — Copy Failed</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Slave:</b>   {slave_label}\n"
        f"<b>Account:</b> <code>{slave_id}</code>\n"
        f"<b>Master:</b>  {master_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Symbol:</b>  {symbol}\n"
        f"<b>Action:</b>  {trade_type.upper()}\n"
        f"<b>Signal:</b>  <code>{signal_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ <b>Error:</b> {error_message}{code_str}\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_slave_connected(slave_label: str, slave_id: str, server: str, equity: float):
    """Fired when a Slave terminal connects successfully."""
    msg = (
        f"🔗 <b>OmniRoute — Slave Connected</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Slave:</b>   {slave_label}\n"
        f"<b>Account:</b> <code>{slave_id}</code>\n"
        f"<b>Server:</b>  {server}\n"
        f"<b>Equity:</b>  <code>${equity:,.2f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_slave_error(slave_label: str, slave_id: str, error: str):
    """Fired when a Slave fails to connect or encounters a terminal error."""
    msg = (
        f"🔴 <b>OmniRoute — Slave Error</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Slave:</b>   {slave_label}\n"
        f"<b>Account:</b> <code>{slave_id}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ {error}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Action required — check MT5 terminal on VPS</i>\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_master_connected(master_label: str, magic: int, server: str, equity: float):
    """Fired when a Master terminal connects."""
    msg = (
        f"🟦 <b>OmniRoute — Master Connected</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Master:</b>  {master_label}\n"
        f"<b>Magic:</b>   <code>{magic}</code>\n"
        f"<b>Server:</b>  {server}\n"
        f"<b>Equity:</b>  <code>${equity:,.2f}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_master_error(master_label: str, magic: int, error: str):
    """Fired when a Master fails to connect."""
    msg = (
        f"🔴 <b>OmniRoute — Master Error</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Master:</b>  {master_label}\n"
        f"<b>Magic:</b>   <code>{magic}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚠️ {error}\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_bridge_started(masters: int, slaves: int):
    """Fired when the bridge starts up."""
    msg = (
        f"🚀 <b>OmniRoute Bridge Started</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Masters loaded:</b> {masters}\n"
        f"<b>Slaves loaded:</b>  {slaves}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


def notify_bridge_stopped():
    """Fired on clean shutdown."""
    msg = (
        f"🛑 <b>OmniRoute Bridge Stopped</b>\n"
        f"<i>{_ts()}</i>"
    )
    _fire(_send(msg))


async def send_test_message() -> bool:
    """Send a test message and return True if successful. Used by the /telegram/test endpoint."""
    msg = (
        f"✅ <b>OmniRoute — Test Message</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Telegram notifications are working correctly.\n"
        f"<b>Bot Token:</b> <code>...{settings.telegram_bot_token[-8:] if settings.telegram_bot_token else 'not set'}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>{_ts()}</i>"
    )
    return await _send(msg)


async def close_client():
    """Call on app shutdown to cleanly close the HTTP client."""
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None
