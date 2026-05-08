"""
config.py — OmniRoute bridge settings.
Now includes Telegram notification configuration.
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Bridge
    bridge_host:  str   = "0.0.0.0"
    bridge_port:  int   = 8000
    api_secret:   str   = ""

    # Master reference equity fallback
    master_equity: float = 10_000.0

    # Execution
    slippage_tolerance: int = 3

    # Database
    db_path: str = "copybridge.db"

    # Logging
    log_level: str = "INFO"

    # ── Telegram ────────────────────────────────────────────────────────────
    telegram_bot_token: str  = ""
    telegram_chat_id:   str  = ""
    telegram_enabled:   bool = True   # Master toggle — can also be flipped via API

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
