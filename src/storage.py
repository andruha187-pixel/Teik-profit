"""
Лёгкое SQLite-хранилище: сигналы (для последующего бэктеста стратегии) и
сделки (для учёта PnL). Никакой внешней БД не нужно для старта.

Таблица signals пишет КАЖДЫЙ тик, вошёл бот или нет — это основной
датасет для анализа: раз в 15 минут рынок резолвится, и мы можем
подписать (label_signals_outcome) все тики этого рынка исходом,
получая полноценные признаки + метку для последующего подбора формулы.
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

# Колонки, добавленные уже после первого релиза — через ALTER TABLE, чтобы
# не терять историю на уже задеплоенных базах. (column_name, sql_type)
_SIGNALS_MIGRATIONS = [
    ("atr", "REAL"),
    ("atr_ratio_to_avg", "REAL"),
    ("ema_fast", "REAL"),
    ("ema_slow", "REAL"),
    ("ema_fast_slope", "REAL"),
    ("trend_up", "INTEGER"),
    ("time_score", "REAL"),
    ("distance_score", "REAL"),
    ("trend_score", "REAL"),
    ("vol_score", "REAL"),
    ("liq_score", "REAL"),
    ("ask_liquidity_usdc", "REAL"),
    ("up_best_ask", "REAL"),
    ("down_best_ask", "REAL"),
    ("book_source", "TEXT"),
    ("outcome", "TEXT"),
    ("asset", "TEXT"),
    ("timeframe", "TEXT"),
]

_TRADES_MIGRATIONS = [
    ("token_id", "TEXT"),
]


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
        existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
        for col, sql_type in _SIGNALS_MIGRATIONS:
            if col not in existing:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {sql_type}")
        existing_trades = {row[1] for row in conn.execute("PRAGMA table_info(trades)")}
        for col, sql_type in _TRADES_MIGRATIONS:
            if col not in existing_trades:
                conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {sql_type}")


def log_signal(market_slug: str, current_price: float, strike_price: float, decision,
               indicators: dict | None = None, up_book=None, down_book=None) -> None:
    """
    indicators/up_book/down_book необязательны (обратная совместимость), но
    без них отчёт для анализа будет неполным — main.py всегда должен их
    передавать.
    """
    indicators = indicators or {}
    asset, timeframe_label = parse_market_slug(market_slug)
    with _conn() as conn:
        conn.execute(
            """INSERT INTO signals
               (ts, market_slug, current_price, strike_price, direction, entry_price,
                safety_score, minutes_left, distance_atr, should_enter, reasons,
                atr, atr_ratio_to_avg, ema_fast, ema_slow, ema_fast_slope, trend_up,
                time_score, distance_score, trend_score, vol_score, liq_score,
                ask_liquidity_usdc, up_best_ask, down_best_ask, book_source, asset, timeframe)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                int(time.time()), market_slug, current_price, strike_price, decision.direction,
                decision.entry_price, decision.safety_score, decision.minutes_left,
                decision.distance_atr, int(decision.should_enter), "; ".join(decision.reasons),
                indicators.get("atr"), indicators.get("atr_ratio_to_avg"),
                indicators.get("ema_fast"), indicators.get("ema_slow"), indicators.get("ema_fast_slope"),
                int(indicators.get("trend_up")) if indicators.get("trend_up") is not None else None,
                decision.time_score, decision.distance_score, decision.trend_score,
                decision.vol_score, decision.liq_score,
                (up_book.ask_liquidity_usdc if decision.direction == "UP" else down_book.ask_liquidity_usdc)
                if (up_book and down_book) else None,
                up_book.best_ask if up_book else None,
                down_book.best_ask if down_book else None,
                (up_book.source if decision.direction == "UP" else down_book.source)
                if (up_book and down_book) else None,
                asset, timeframe_label,
            ),
        )


def label_signals_outcome(market_slug: str, outcome: str) -> int:
    """Проставляет исход рынка всем ещё не подписанным тикам этого рынка.
    Возвращает число обновлённых строк."""
    with _conn() as conn:
        cur = conn.execute(
            "UPDATE signals SET outcome = ? WHERE market_slug = ? AND outcome IS NULL",
            (outcome, market_slug),
        )
        return cur.rowcount


def get_markets_needing_outcome(exclude_slugs: set[str] | None, limit: int = 50) -> list[str]:
    """Слаги рынков, у которых есть сигналы без проставленного исхода —
    кандидаты на то, чтобы спросить Gamma API, не зарезолвились ли они."""
    exclude_slugs = exclude_slugs or set()
    with _conn() as conn:
        cur = conn.execute(
            "SELECT DISTINCT market_slug FROM signals WHERE outcome IS NULL ORDER BY ts ASC LIMIT ?",
            (limit + len(exclude_slugs),),
        )
        rows = [row[0] for row in cur.fetchall() if row[0] not in exclude_slugs]
        return rows[:limit]


def log_trade(market_slug: str, condition_id: str, direction: str, entry_price: float,
              size_usdc: float, order_id: str, status: str, dry_run: bool, token_id: str = "") -> int:
    with _conn() as conn:
        cur = conn.execute(
            """INSERT INTO trades
               (ts, market_slug, condition_id, direction, entry_price, size_usdc,
                order_id, status, outcome, pnl_usdc, dry_run, token_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)""",
            (int(time.time()), market_slug, condition_id, direction, entry_price,
             size_usdc, order_id, status, int(dry_run), token_id),
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


def count_open_trades() -> int:
    """Общее число сейчас открытых позиций по ВСЕМ активам/таймфреймам —
    используется для общего лимита MAX_OPEN_POSITIONS (см. executor.py)."""
    with _conn() as conn:
        cur = conn.execute("SELECT COUNT(*) FROM trades WHERE outcome IS NULL")
        return cur.fetchone()[0]


def parse_market_slug(slug: str) -> tuple[str, str]:
    """'sol-updown-1h-1789270200' -> ('sol', '1h'). Если формат неожиданный,
    возвращает ('unknown', 'unknown') вместо падения — отчёты не должны
    рушиться из-за одного странного слага."""
    try:
        asset, rest = slug.split("-updown-", 1)
        label = rest.split("-", 1)[0]
        return asset, label
    except (ValueError, AttributeError):
        return "unknown", "unknown"


def get_unsettled_trades():
    with _conn() as conn:
        cur = conn.execute(
            "SELECT id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id "
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


def get_pnl_by_asset(since_ts: int = 0) -> dict[str, dict]:
    """PnL/винрейт по каждому активу отдельно (суммируя все таймфреймы этого
    актива) — для кнопки 'Статистика' в Telegram."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT market_slug, pnl_usdc FROM trades WHERE outcome IS NOT NULL AND ts >= ?",
            (since_ts,),
        )
        rows = cur.fetchall()

    by_asset: dict[str, dict] = {}
    for slug, pnl in rows:
        asset, _label = parse_market_slug(slug)
        bucket = by_asset.setdefault(asset, {"trades": 0, "pnl_usdc": 0.0, "wins": 0})
        bucket["trades"] += 1
        bucket["pnl_usdc"] += pnl or 0.0
        if (pnl or 0.0) > 0:
            bucket["wins"] += 1
    return by_asset


def get_pnl_by_timeframe(since_ts: int = 0) -> dict[str, dict]:
    """То же самое, но сгруппировано по таймфрейму (15m/1h) вместо актива."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT market_slug, pnl_usdc FROM trades WHERE outcome IS NOT NULL AND ts >= ?",
            (since_ts,),
        )
        rows = cur.fetchall()

    by_tf: dict[str, dict] = {}
    for slug, pnl in rows:
        _asset, label = parse_market_slug(slug)
        bucket = by_tf.setdefault(label, {"trades": 0, "pnl_usdc": 0.0, "wins": 0})
        bucket["trades"] += 1
        bucket["pnl_usdc"] += pnl or 0.0
        if (pnl or 0.0) > 0:
            bucket["wins"] += 1
    return by_tf


# --- Экспорт для периодических отчётов (см. src/reporting.py) ---

SIGNALS_COLUMNS = [
    "id", "ts", "market_slug", "asset", "timeframe", "current_price", "strike_price", "direction", "entry_price",
    "safety_score", "minutes_left", "distance_atr", "should_enter", "reasons",
    "atr", "atr_ratio_to_avg", "ema_fast", "ema_slow", "ema_fast_slope", "trend_up",
    "time_score", "distance_score", "trend_score", "vol_score", "liq_score",
    "ask_liquidity_usdc", "up_best_ask", "down_best_ask", "book_source", "outcome",
]

TRADES_COLUMNS = [
    "id", "ts", "market_slug", "condition_id", "direction", "entry_price", "size_usdc",
    "order_id", "status", "outcome", "pnl_usdc", "dry_run", "token_id",
]


def get_signals_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(SIGNALS_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM signals WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()


def get_trades_since(since_ts: int) -> list[tuple]:
    with _conn() as conn:
        cols = ", ".join(TRADES_COLUMNS)
        cur = conn.execute(f"SELECT {cols} FROM trades WHERE ts >= ? ORDER BY ts ASC", (since_ts,))
        return cur.fetchall()


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
