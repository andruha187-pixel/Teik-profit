"""
Лёгкое SQLite-хранилище: сигналы (для последующего бэктеста стратегии) и
сделки (для учёта PnL). Никакой внешней БД не нужно для старта.
"""
from __future__ import annotations
import os
import sqlite3
import time
from contextlib import contextmanager

from config import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    current_price REAL,
    strike_price REAL,
    direction TEXT,
    entry_price REAL,
    safety_score REAL,
    minutes_left REAL,
    distance_atr REAL,
    should_enter INTEGER,
    reasons TEXT
);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    market_slug TEXT NOT NULL,
    condition_id TEXT,
    direction TEXT,
    entry_price REAL,
    size_usdc REAL,
    order_id TEXT,
    status TEXT,
    outcome TEXT,
    pnl_usdc REAL,
    dry_run INTEGER
);

CREATE TABLE IF NOT EXISTS bot_settings (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(settings.DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(settings.DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with _conn() as conn:
        conn.executescript(_SCHEMA)


def log_signal(market_slug: str, current_price: float, strike_price: float, decision) -> None:
    with _conn() as conn:
        conn.execute(
            """INSERT INTO signals
               (ts, market_slug, current_price, strike_price, direction, entry_price,
                safety_score, minutes_left, distance_atr, should_enter, reasons)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(time.time()), market_slug, current_price, strike_price, decision.direction,
                decision.entry_price, decision.safety_score, decision.minutes_left,
                decision.distance_atr, int(decision.should_enter), "; ".join(decision.reasons),
            ),
        )


def log_trade(market_slug: str, condition_id: str, direction: str, entry_price: float,
              size_usdc: float, order_id: str, status: str, dry_run: bool) -> int:
    with _conn() as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ts, market_slug, condition_id, direction, entry_price, size_usdc,
                order_id, status, outcome, pnl_usdc, dry_run)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)""",
            (int(time.time()), market_slug, condition_id, direction, entry_price,
             size_usdc, order_id, status, int(dry_run)),
        )
        return cur.lastrowid


def settle_trade(trade_id: int, outcome: str, pnl_usdc: float) -> None:
    with _conn() as conn:
        conn.execute(
            "UPDATE trades SET outcome = ?, pnl_usdc = ? WHERE id = ?",
            (outcome, pnl_usdc, trade_id),
        )


def get_open_trade_for_market(market_slug: str):
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, condition_id, direction, entry_price, size_usdc FROM trades "
            "WHERE market_slug = ? AND outcome IS NULL", (market_slug,),
        )
        return cur.fetchone()


def get_unsettled_trades():
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run "
            "FROM trades WHERE outcome IS NULL",
        )
        return cur.fetchall()


def get_pnl_summary(since_ts: int = 0):
    with _conn() as conn:
        cur = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(pnl_usdc), 0), "
            "SUM(CASE WHEN pnl_usdc > 0 THEN 1 ELSE 0 END) "
            "FROM trades WHERE outcome IS NOT NULL AND ts >= ?", (since_ts,),
        )
        count, total_pnl, wins = cur.fetchone()
        return {"trades": count or 0, "pnl_usdc": total_pnl or 0.0, "wins": wins or 0}


# --- Настройки, управляемые из Telegram (переживают рестарт процесса) ---

def get_all_settings() -> dict[str, str]:
    with _conn() as conn:
        cur = conn.execute("SELECT key, value FROM bot_settings")
        return {k: v for k, v in cur.fetchall()}


def set_setting(key: str, value) -> None:
    with _conn() as conn:
        conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
