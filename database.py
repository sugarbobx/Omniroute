"""
database.py — OmniRoute v2.3
Added: protection_json column on slaves, modify_log table
v2.3: bot columns on masters, strategies + strategy_results tables
"""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from models import MasterAccount, SlaveAccount, TradeProtection

logger = logging.getLogger("database")
DB_PATH = Path("copybridge.db")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS masters (
                master_id       TEXT PRIMARY KEY,
                label           TEXT NOT NULL,
                login           INTEGER NOT NULL,
                password        TEXT NOT NULL,
                server          TEXT NOT NULL,
                terminal_path   TEXT NOT NULL,
                magic_number    INTEGER NOT NULL UNIQUE,
                symbol_map_json TEXT NOT NULL DEFAULT '{}',
                enabled         INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slaves (
                account_id          TEXT PRIMARY KEY,
                label               TEXT NOT NULL,
                login               INTEGER NOT NULL,
                password            TEXT NOT NULL,
                server              TEXT NOT NULL,
                terminal_path       TEXT NOT NULL,
                lot_sizing_mode     TEXT NOT NULL DEFAULT 'equity_ratio',
                fixed_lot           REAL NOT NULL DEFAULT 0.01,
                multiplier          REAL NOT NULL DEFAULT 1.0,
                max_lot             REAL NOT NULL DEFAULT 10.0,
                min_lot             REAL NOT NULL DEFAULT 0.01,
                symbol_map_json     TEXT NOT NULL DEFAULT '{}',
                max_open_trades     INTEGER NOT NULL DEFAULT 20,
                slippage_override   INTEGER,
                protection_json     TEXT NOT NULL DEFAULT '{}',
                enabled             INTEGER NOT NULL DEFAULT 1,
                created_at          TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS slave_master_links (
                account_id  TEXT NOT NULL,
                master_id   TEXT NOT NULL,
                linked_at   TEXT NOT NULL,
                PRIMARY KEY (account_id, master_id),
                FOREIGN KEY (account_id) REFERENCES slaves(account_id) ON DELETE CASCADE,
                FOREIGN KEY (master_id)  REFERENCES masters(master_id) ON DELETE CASCADE
            );

            -- Tracks slave tickets so we can find them for SL/TP modify
            CREATE TABLE IF NOT EXISTS slave_positions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id   TEXT NOT NULL,
                master_id    TEXT NOT NULL,
                magic_number INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                ticket       INTEGER NOT NULL,
                open_price   REAL NOT NULL,
                trade_type   TEXT NOT NULL,
                opened_at    TEXT NOT NULL,
                UNIQUE(account_id, ticket)
            );

            -- Log of all SL/TP modify operations
            CREATE TABLE IF NOT EXISTS modify_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id   TEXT NOT NULL,
                ticket       INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                old_sl       REAL,
                old_tp       REAL,
                new_sl       REAL,
                new_tp       REAL,
                success      INTEGER NOT NULL,
                error        TEXT,
                modified_at  TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_slave_positions_lookup
                ON slave_positions(account_id, magic_number, symbol);

            CREATE TABLE IF NOT EXISTS strategies (
                strategy_id     TEXT PRIMARY KEY,
                name            TEXT NOT NULL,
                mode            TEXT NOT NULL CHECK(mode IN ('visual', 'code')),
                symbol          TEXT NOT NULL,
                timeframe       TEXT NOT NULL,
                file_path       TEXT NOT NULL,
                assigned_bot_id TEXT,
                created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(assigned_bot_id) REFERENCES masters(master_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS strategy_results (
                result_id        TEXT PRIMARY KEY,
                strategy_id      TEXT NOT NULL,
                bot_id           TEXT NOT NULL,
                signal_direction TEXT,
                executed_at      TEXT,
                entry_price      REAL,
                exit_price       REAL,
                pnl              REAL,
                mode             TEXT CHECK(mode IN ('forward_test', 'live')),
                FOREIGN KEY(strategy_id) REFERENCES strategies(strategy_id) ON DELETE CASCADE
            );
        """)
        # SQLite has no ALTER TABLE ... ADD COLUMN IF NOT EXISTS — check pragma first
        _ensure_column(conn, "masters", "is_virtual_bot", "INTEGER DEFAULT 0")
        _ensure_column(conn, "masters", "bot_symbol", "TEXT")
        _ensure_column(conn, "masters", "bot_timeframe", "TEXT DEFAULT 'M5'")
        _ensure_column(conn, "masters", "base_volume", "REAL DEFAULT 0.1")
        _ensure_column(conn, "masters", "strategy_name", "TEXT")
        _ensure_column(conn, "masters", "forward_test", "INTEGER DEFAULT 0")
        _ensure_column(conn, "masters", "bot_mode", "TEXT DEFAULT 'standalone'")
    logger.info(f"Database ready: {DB_PATH.resolve()}")


def _ensure_column(conn, table: str, col: str, decl: str):
    existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        logger.info(f"Migration: added {table}.{col}")


# ── Masters ──────────────────────────────────────────────────────────────────

def save_master(m: MasterAccount):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO masters
              (master_id,label,login,password,server,terminal_path,magic_number,
               symbol_map_json,enabled,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(master_id) DO UPDATE SET
              label=excluded.label, login=excluded.login,
              password=excluded.password, server=excluded.server,
              terminal_path=excluded.terminal_path,
              magic_number=excluded.magic_number,
              symbol_map_json=excluded.symbol_map_json,
              enabled=excluded.enabled
        """, (
            m.master_id, m.label, m.login, m.password, m.server,
            m.terminal_path, m.magic_number, json.dumps(m.symbol_map),
            int(m.enabled), m.created_at.isoformat(),
        ))


def load_all_masters() -> list[MasterAccount]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM masters").fetchall()
    return [_row_to_master(r) for r in rows]


def delete_master(master_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM masters WHERE master_id=?", (master_id,))


def _row_to_master(row) -> MasterAccount:
    return MasterAccount(
        master_id=row["master_id"], label=row["label"], login=row["login"],
        password=row["password"], server=row["server"],
        terminal_path=row["terminal_path"], magic_number=row["magic_number"],
        symbol_map=json.loads(row["symbol_map_json"]),
        enabled=bool(row["enabled"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


# ── Slaves ───────────────────────────────────────────────────────────────────

def save_slave(s: SlaveAccount):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO slaves
              (account_id,label,login,password,server,terminal_path,lot_sizing_mode,
               fixed_lot,multiplier,max_lot,min_lot,symbol_map_json,max_open_trades,
               slippage_override,protection_json,enabled,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
              label=excluded.label, login=excluded.login,
              password=excluded.password, server=excluded.server,
              terminal_path=excluded.terminal_path,
              lot_sizing_mode=excluded.lot_sizing_mode,
              fixed_lot=excluded.fixed_lot, multiplier=excluded.multiplier,
              max_lot=excluded.max_lot, min_lot=excluded.min_lot,
              symbol_map_json=excluded.symbol_map_json,
              max_open_trades=excluded.max_open_trades,
              slippage_override=excluded.slippage_override,
              protection_json=excluded.protection_json,
              enabled=excluded.enabled
        """, (
            s.account_id, s.label, s.login, s.password, s.server, s.terminal_path,
            s.lot_sizing_mode.value, s.fixed_lot, s.multiplier, s.max_lot, s.min_lot,
            json.dumps(s.symbol_map), s.max_open_trades, s.slippage_override,
            s.protection.model_dump_json(), int(s.enabled), s.created_at.isoformat(),
        ))


def update_slave_protection(account_id: str, protection: TradeProtection):
    """Targeted update — only touches the protection column."""
    with get_conn() as conn:
        conn.execute(
            "UPDATE slaves SET protection_json=? WHERE account_id=?",
            (protection.model_dump_json(), account_id),
        )


def load_all_slaves() -> list[SlaveAccount]:
    with get_conn() as conn:
        rows  = conn.execute("SELECT * FROM slaves").fetchall()
        links = conn.execute("SELECT account_id, master_id FROM slave_master_links").fetchall()
    slave_masters: dict[str, list[str]] = {}
    for lnk in links:
        slave_masters.setdefault(lnk["account_id"], []).append(lnk["master_id"])
    result = []
    for row in rows:
        s = _row_to_slave(row)
        s.master_ids = slave_masters.get(s.account_id, [])
        result.append(s)
    return result


def delete_slave(account_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM slaves WHERE account_id=?", (account_id,))
        conn.execute("DELETE FROM slave_positions WHERE account_id=?", (account_id,))


def _row_to_slave(row) -> SlaveAccount:
    prot_raw = row["protection_json"] if "protection_json" in row.keys() else "{}"
    try:
        prot = TradeProtection.model_validate_json(prot_raw)
    except Exception:
        prot = TradeProtection()
    return SlaveAccount(
        account_id=row["account_id"], label=row["label"], login=row["login"],
        password=row["password"], server=row["server"], terminal_path=row["terminal_path"],
        lot_sizing_mode=row["lot_sizing_mode"], fixed_lot=row["fixed_lot"],
        multiplier=row["multiplier"], max_lot=row["max_lot"], min_lot=row["min_lot"],
        symbol_map=json.loads(row["symbol_map_json"]),
        max_open_trades=row["max_open_trades"],
        slippage_override=row["slippage_override"],
        protection=prot,
        enabled=bool(row["enabled"]),
        created_at=datetime.fromisoformat(row["created_at"]),
    )


# ── Links ────────────────────────────────────────────────────────────────────

def link_slave_to_master(account_id: str, master_id: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO slave_master_links (account_id,master_id,linked_at) VALUES (?,?,?)",
            (account_id, master_id, datetime.utcnow().isoformat()),
        )


def unlink_slave_from_master(account_id: str, master_id: str):
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM slave_master_links WHERE account_id=? AND master_id=?",
            (account_id, master_id),
        )


def get_slaves_for_master(master_id: str) -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT account_id FROM slave_master_links WHERE master_id=?", (master_id,)
        ).fetchall()
    return [r["account_id"] for r in rows]


def get_masters_for_slave(account_id: str) -> list[str]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT master_id FROM slave_master_links WHERE account_id=?", (account_id,)
        ).fetchall()
    return [r["master_id"] for r in rows]


# ── Position tracking (for SL/TP modify) ────────────────────────────────────

def record_slave_position(account_id: str, master_id: str, magic_number: int,
                           symbol: str, ticket: int, open_price: float, trade_type: str):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO slave_positions
              (account_id,master_id,magic_number,symbol,ticket,open_price,trade_type,opened_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (account_id, master_id, magic_number, symbol, ticket, open_price, trade_type,
              datetime.utcnow().isoformat()))


def get_slave_tickets(account_id: str, magic_number: int, symbol: str) -> list[dict]:
    """Return all open slave tickets for a given magic+symbol combination."""
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT ticket, open_price, trade_type FROM slave_positions
            WHERE account_id=? AND magic_number=? AND symbol=?
        """, (account_id, magic_number, symbol)).fetchall()
    return [{"ticket": r["ticket"], "open_price": r["open_price"], "trade_type": r["trade_type"]} for r in rows]


def remove_slave_position(account_id: str, ticket: int):
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM slave_positions WHERE account_id=? AND ticket=?",
            (account_id, ticket),
        )


def log_modify(account_id: str, ticket: int, symbol: str,
               old_sl: float, old_tp: float, new_sl: float, new_tp: float,
               success: bool, error: Optional[str] = None):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO modify_log
              (account_id,ticket,symbol,old_sl,old_tp,new_sl,new_tp,success,error,modified_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (account_id, ticket, symbol, old_sl, old_tp, new_sl, new_tp,
              int(success), error, datetime.utcnow().isoformat()))


# ── Virtual bots (rows in masters with is_virtual_bot=1) ────────────────────

_BOT_FIELDS = ("is_virtual_bot", "bot_symbol", "bot_timeframe", "base_volume",
               "strategy_name", "forward_test", "bot_mode")


def _row_to_bot(row) -> dict:
    return {
        "bot_id":        row["master_id"],
        "label":         row["label"],
        "magic_number":  row["magic_number"],
        "enabled":       bool(row["enabled"]),
        "symbol":        row["bot_symbol"],
        "timeframe":     row["bot_timeframe"] or "M5",
        "base_volume":   row["base_volume"] if row["base_volume"] is not None else 0.1,
        "strategy_name": row["strategy_name"],
        "forward_test":  bool(row["forward_test"]),
        "mode":          row["bot_mode"] or "standalone",
        "created_at":    row["created_at"],
    }


def get_all_virtual_bots() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM masters WHERE is_virtual_bot=1").fetchall()
    return [_row_to_bot(r) for r in rows]


def get_virtual_bot(bot_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM masters WHERE master_id=? AND is_virtual_bot=1", (bot_id,)
        ).fetchone()
    return _row_to_bot(row) if row else None


def save_virtual_bot(bot: dict):
    """Insert a virtual bot as a masters row. Standard master columns get
    placeholder values — a virtual bot has no real MT5 master terminal."""
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO masters
              (master_id,label,login,password,server,terminal_path,magic_number,
               symbol_map_json,enabled,created_at,
               is_virtual_bot,bot_symbol,bot_timeframe,base_volume,strategy_name,forward_test,bot_mode)
            VALUES (?,?,0,'','virtual','virtual',?,'{}',?,?,1,?,?,?,?,?,?)
        """, (
            bot["bot_id"], bot["label"], bot["magic_number"], int(bot.get("enabled", True)),
            datetime.utcnow().isoformat(),
            bot["symbol"], bot.get("timeframe", "M5"), bot.get("base_volume", 0.1),
            bot.get("strategy_name"), int(bot.get("forward_test", False)),
            bot.get("mode", "standalone"),
        ))


def update_virtual_bot(bot_id: str, updates: dict):
    col_map = {
        "label": "label", "magic_number": "magic_number", "enabled": "enabled",
        "symbol": "bot_symbol", "timeframe": "bot_timeframe", "base_volume": "base_volume",
        "strategy_name": "strategy_name", "forward_test": "forward_test", "mode": "bot_mode",
    }
    sets, vals = [], []
    for key, col in col_map.items():
        if key in updates:
            v = updates[key]
            if isinstance(v, bool):
                v = int(v)
            sets.append(f"{col}=?")
            vals.append(v)
    if not sets:
        return
    vals.append(bot_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE masters SET {', '.join(sets)} WHERE master_id=? AND is_virtual_bot=1", vals)


def delete_virtual_bot(bot_id: str):
    with get_conn() as conn:
        conn.execute("UPDATE strategies SET assigned_bot_id=NULL WHERE assigned_bot_id=?", (bot_id,))
        conn.execute("DELETE FROM masters WHERE master_id=? AND is_virtual_bot=1", (bot_id,))


# ── Strategies ───────────────────────────────────────────────────────────────

def get_strategy(strategy_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE strategy_id=?", (strategy_id,)).fetchone()
    return dict(row) if row else None


def get_all_strategies() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM strategies ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def save_strategy(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO strategies
              (strategy_id,name,mode,symbol,timeframe,file_path,assigned_bot_id,created_at)
            VALUES (?,?,?,?,?,?,?,COALESCE((SELECT created_at FROM strategies WHERE strategy_id=?),CURRENT_TIMESTAMP))
        """, (data["strategy_id"], data["name"], data["mode"], data["symbol"],
              data["timeframe"], data["file_path"], data.get("assigned_bot_id"),
              data["strategy_id"]))


def delete_strategy(strategy_id: str):
    with get_conn() as conn:
        conn.execute("DELETE FROM strategies WHERE strategy_id=?", (strategy_id,))


def assign_strategy_to_bot(strategy_id: str, bot_id: Optional[str]):
    with get_conn() as conn:
        # one strategy per bot — unassign anything previously on this bot
        if bot_id:
            conn.execute("UPDATE strategies SET assigned_bot_id=NULL WHERE assigned_bot_id=?", (bot_id,))
        conn.execute("UPDATE strategies SET assigned_bot_id=? WHERE strategy_id=?", (bot_id, strategy_id))


def get_strategy_for_bot(bot_id: str) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM strategies WHERE assigned_bot_id=?", (bot_id,)).fetchone()
    return dict(row) if row else None


def log_strategy_result(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO strategy_results
              (result_id,strategy_id,bot_id,signal_direction,executed_at,entry_price,exit_price,pnl,mode)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (data["result_id"], data["strategy_id"], data["bot_id"],
              data.get("signal_direction"), data.get("executed_at", datetime.utcnow().isoformat()),
              data.get("entry_price"), data.get("exit_price"), data.get("pnl"),
              data.get("mode", "forward_test")))


def get_strategy_results(bot_id: str, limit: int = 200) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM strategy_results WHERE bot_id=?
            ORDER BY executed_at DESC LIMIT ?
        """, (bot_id, limit)).fetchall()
    return [dict(r) for r in rows]
