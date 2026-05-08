"""
database.py — OmniRoute v2.2
Added: protection_json column on slaves, modify_log table
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
        """)
    logger.info(f"Database ready: {DB_PATH.resolve()}")


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
