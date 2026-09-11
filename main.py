import os
import re
import json
import time
import math
import asyncio
import sqlite3
import hashlib
import logging
from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from pathlib import Path
from datetime import datetime, timezone
from collections import OrderedDict, defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
from dotenv import load_dotenv

try:
    from polymarket import AsyncSecureClient, RelayerApiKey
    from polymarket._internal.actions.orders.place import (
        post_order_with_allowance_recovery as sdk_post_order_with_allowance_recovery,
    )
except ImportError:
    AsyncSecureClient = None
    RelayerApiKey = None
    sdk_post_order_with_allowance_recovery = None

load_dotenv()

# ============================================================
# POLYMARKET ULTRAFAST WALLET COPY BOT
# ============================================================
# Fast path:
#   Polymarket RTDS activity/orders_matched + activity/trades
#   -> in-memory wallet filter + dedupe
#   -> immediate signed FAK at target trade price +/- user slippage
#
# Important latency rule:
#   LIVE fast path does NOT wait for REST orderbook before first submission.
#   REST /trades is only a fallback for missed/reconnect events.
#
# Controls are persisted in SQLite and exposed through Telegram buttons.
# Fresh install: STOP + PAPER. LIVE requires LIVE_MASTER_ENABLE=1 and a
# Telegram confirmation before the mode changes to LIVE.
# ============================================================

VERSION = "1.2-ultrafast-rtds-copy-audit"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    p = DATA_DIR / ".copybot_write_test"
    p.write_text("ok", encoding="utf-8")
    p.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "ultrafast_copybot.db"

RTDS_URL = os.getenv("RTDS_URL", "wss://ws-live-data.polymarket.com").strip()
DATA_API = "https://data-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
POLYMARKET_WALLET_ADDRESS = os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip()
POLYMARKET_RELAYER_API_KEY = os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip()
POLYMARKET_RELAYER_API_KEY_ADDRESS = os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()
LIVE_MASTER_ENABLE = os.getenv("LIVE_MASTER_ENABLE", "0").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_COPY_USDC = float(os.getenv("COPY_USDC", "5"))
DEFAULT_SIZE_MODE = os.getenv("COPY_SIZE_MODE", "FIXED").strip().upper()
DEFAULT_SCALE_PCT = float(os.getenv("COPY_SCALE_PCT", "100"))
DEFAULT_MAX_COPY_USDC = float(os.getenv("MAX_COPY_USDC", "100"))
DEFAULT_SLIPPAGE = float(os.getenv("COPY_SLIPPAGE", "0.05"))
DEFAULT_PAPER_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
DEFAULT_SELL_MODE = os.getenv("COPY_SELL_MODE", "PROPORTIONAL").strip().upper()

MIN_ORDER_SHARES = float(os.getenv("MIN_ORDER_SHARES", "5"))
MAX_ORDER_SHARES = float(os.getenv("MAX_ORDER_SHARES", "10000"))
LIVE_PRICE_TICK_FALLBACK = float(os.getenv("LIVE_PRICE_TICK_FALLBACK", "0.01"))

REST_FALLBACK_ENABLE = os.getenv("REST_FALLBACK_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
REST_FALLBACK_INTERVAL = max(0.5, float(os.getenv("REST_FALLBACK_INTERVAL", "1.0")))
REST_MAX_COPY_AGE_SEC = max(1.0, float(os.getenv("REST_MAX_COPY_AGE_SEC", "4")))
MAX_WALLETS = max(1, min(50, int(os.getenv("MAX_WALLETS", "20"))))
SEEN_CACHE_SIZE = max(1000, int(os.getenv("SEEN_CACHE_SIZE", "20000")))

LIVE_PREWARM_ENABLE = os.getenv("LIVE_PREWARM_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
LIVE_PREWARM_INTERVAL_SEC = max(5.0, float(os.getenv("LIVE_PREWARM_INTERVAL_SEC", "20")))
LIVE_NO_MATCH_RETRIES = max(0, min(2, int(os.getenv("LIVE_NO_MATCH_RETRIES", "1"))))
LIVE_NO_MATCH_RETRY_DELAY_MS = max(0, int(os.getenv("LIVE_NO_MATCH_RETRY_DELAY_MS", "25")))
LIVE_SELL_BALANCE_RETRY_MS = max(100, int(os.getenv("LIVE_SELL_BALANCE_RETRY_MS", "600")))

TELEGRAM_NOTIFY_TRADES_DEFAULT = os.getenv("TELEGRAM_NOTIFY_TRADES", "1").strip().lower() in {"1", "true", "yes", "on"}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ultrafast-copybot")

session: Optional[aiohttp.ClientSession] = None
live_client = None
live_client_ready = False
live_client_error = ""
live_prewarm_last_ms = 0
live_prewarm_lock = asyncio.Lock()

# Hot-path state kept in memory. DB is updated after the detection decision so
# SQLite never sits in front of a LIVE order submission.
watched_wallets = {}  # lower-address -> {address,label,enabled}
settings_cache = {}
shadow_cache = {}       # (wallet, asset) -> target shares
position_cache = {}     # (wallet, asset, mode) -> bot-tracked position dict
seen_hot = OrderedDict()
asset_locks = defaultdict(asyncio.Lock)
wallet_rate = defaultdict(lambda: deque(maxlen=200))
pending_input = {"type": None}

rt_stats = {
    "connected": False,
    "last_message_ms": 0,
    "messages": 0,
    "matched_wallet_raw_events": 0,
    "matched_wallet_events": 0,
    "duplicate_events": 0,
    "reconnects": 0,
    "last_error": "",
}

# Trade-notification queue keeps Telegram completely off the execution hot path
# while preserving FOUND -> RESULT message order.
trade_notice_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ms():
    return int(time.time() * 1000)


def utc_iso(ts=None):
    if ts is None:
        ts = time.time()
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def sf(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def si(v, default=0):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def clamp(v, lo, hi):
    return max(lo, min(hi, float(v)))


def jd(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def normalize_address(v):
    s = str(v or "").strip()
    return s.lower() if ADDRESS_RE.match(s) else ""


def short_addr(addr):
    s = str(addr or "")
    return f"{s[:6]}…{s[-4:]}" if len(s) >= 12 else s


def safe_label(label, addr):
    x = str(label or "").strip()
    return x[:40] if x else short_addr(addr)


def source_trade_usdc(payload):
    # Data API activity may expose an exact usdcSize; RTDS Trade currently
    # documents price+size, so fall back to price*shares without adding a REST hop.
    exact = sf((payload or {}).get("usdcSize"), -1.0)
    if exact >= 0:
        return exact
    return max(0.0, sf((payload or {}).get("price"))) * max(0.0, sf((payload or {}).get("size")))


def percentile(values, p):
    vals = sorted(float(x) for x in values if x is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    k = (len(vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return vals[int(k)]
    return vals[f] * (c - k) + vals[c] * (k - f)


def fee_estimate_generic(shares, price):
    # Fees differ by market/category and CLOB V2 fee curve. We deliberately do
    # not invent a universal fee here. Reported copy PnL is bot-tracked GROSS.
    return 0.0


def is_definite_fak_no_match_error(exc):
    text = f"{type(exc).__name__}: {exc}".lower()
    return "no orders found to match with fak order" in text and "fak" in text


def is_definite_balance_allowance_rejection(exc):
    text = f"{type(exc).__name__}: {exc}".lower()
    return (
        "not enough balance" in text
        or "not enough balance / allowance" in text
        or "insufficient balance" in text
    )


def response_json(obj):
    try:
        if hasattr(obj, "model_dump"):
            return jd(obj.model_dump())
        if hasattr(obj, "__dict__"):
            return jd(obj.__dict__)
        return jd(str(obj))
    except Exception:
        return "{}"


def event_key(payload):
    wallet = normalize_address(
        payload.get("proxyWallet")
        or payload.get("proxy_wallet")
        or ((payload.get("trader") or {}).get("address") if isinstance(payload.get("trader"), dict) else "")
    )
    tx = str(payload.get("transactionHash") or payload.get("transaction_hash") or "").lower()
    asset = str(payload.get("asset") or payload.get("asset_id") or "")
    side = str(payload.get("side") or "").upper()
    if tx:
        # One target-wallet/asset/side action in the same on-chain transaction is
        # copied once even if RTDS emits it through both orders_matched and trades.
        raw = f"{wallet}|{tx}|{asset}|{side}"
    else:
        raw = "|".join([
            wallet, asset, side,
            str(payload.get("price") or ""),
            str(payload.get("size") or ""),
            str(payload.get("timestamp") or ""),
            str(payload.get("conditionId") or payload.get("condition_id") or ""),
        ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def hot_seen_add(key):
    if key in seen_hot:
        seen_hot.move_to_end(key)
        return False
    seen_hot[key] = now_ms()
    if len(seen_hot) > SEEN_CACHE_SIZE:
        seen_hot.popitem(last=False)
    return True


# ============================================================
# SQLITE
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS watched_wallets(
            address TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            added_ms INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS seen_events(
            event_key TEXT PRIMARY KEY,
            wallet TEXT NOT NULL,
            source TEXT NOT NULL,
            observed_ms INTEGER NOT NULL,
            target_timestamp INTEGER,
            tx_hash TEXT,
            asset TEXT,
            side TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_seen_observed ON seen_events(observed_ms);

        CREATE TABLE IF NOT EXISTS target_shadow(
            wallet TEXT NOT NULL,
            asset TEXT NOT NULL,
            shares REAL NOT NULL DEFAULT 0,
            updated_ms INTEGER NOT NULL,
            PRIMARY KEY(wallet, asset)
        );

        CREATE TABLE IF NOT EXISTS copy_positions(
            wallet TEXT NOT NULL,
            asset TEXT NOT NULL,
            mode TEXT NOT NULL,
            condition_id TEXT,
            title TEXT,
            slug TEXT,
            outcome TEXT,
            shares REAL NOT NULL DEFAULT 0,
            total_cost REAL NOT NULL DEFAULT 0,
            realized_pnl REAL NOT NULL DEFAULT 0,
            updated_ms INTEGER NOT NULL,
            PRIMARY KEY(wallet, asset, mode)
        );

        CREATE TABLE IF NOT EXISTS copy_orders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL,
            wallet TEXT NOT NULL,
            wallet_label TEXT,
            source TEXT NOT NULL,
            detected_ms INTEGER NOT NULL,
            target_timestamp INTEGER,
            target_tx_hash TEXT,
            condition_id TEXT,
            asset TEXT NOT NULL,
            title TEXT,
            slug TEXT,
            outcome TEXT,
            target_side TEXT NOT NULL,
            target_price REAL,
            target_size REAL,
            mode TEXT NOT NULL,
            copy_amount_usdc REAL,
            size_mode TEXT,
            source_amount_usdc REAL,
            scale_pct REAL,
            max_copy_usdc REAL,
            size_capped INTEGER NOT NULL DEFAULT 0,
            slippage REAL,
            requested_shares REAL,
            limit_price REAL,
            build_sign_ms INTEGER,
            detect_to_submit_ms INTEGER,
            api_ms INTEGER,
            total_reaction_ms INTEGER,
            status TEXT NOT NULL,
            filled_shares REAL NOT NULL DEFAULT 0,
            avg_price REAL,
            gross_amount REAL NOT NULL DEFAULT 0,
            fee_estimate REAL NOT NULL DEFAULT 0,
            order_id TEXT,
            response_json TEXT,
            error TEXT,
            created_ms INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_orders_wallet ON copy_orders(wallet, created_ms);
        CREATE INDEX IF NOT EXISTS idx_orders_event ON copy_orders(event_key);
        """)
        # v1.1 migration for databases created by v1.0.
        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(copy_orders)").fetchall()}
        for col, ddl in {
            "size_mode": "TEXT",
            "source_amount_usdc": "REAL",
            "scale_pct": "REAL",
            "max_copy_usdc": "REAL",
            "size_capped": "INTEGER NOT NULL DEFAULT 0",
        }.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE copy_orders ADD COLUMN {col} {ddl}")

        defaults = {
            "running": "0",
            "mode": "PAPER",
            "copy_usdc": str(DEFAULT_COPY_USDC),
            "size_mode": DEFAULT_SIZE_MODE if DEFAULT_SIZE_MODE in {"FIXED", "SAME_USD", "SAME_SHARES", "SCALE"} else "FIXED",
            "scale_pct": str(DEFAULT_SCALE_PCT),
            "max_copy_usdc": str(DEFAULT_MAX_COPY_USDC),
            "slippage": str(DEFAULT_SLIPPAGE),
            "paper_cash": str(DEFAULT_PAPER_BALANCE),
            "sell_mode": DEFAULT_SELL_MODE if DEFAULT_SELL_MODE in {"PROPORTIONAL", "FULL", "OFF"} else "PROPORTIONAL",
            "notify_trades": "1" if TELEGRAM_NOTIFY_TRADES_DEFAULT else "0",
        }
        for k, v in defaults.items():
            conn.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))
        conn.commit()


def load_runtime_caches():
    settings_cache.clear()
    shadow_cache.clear()
    position_cache.clear()
    with db() as conn:
        for r in conn.execute("SELECT key,value FROM settings"):
            settings_cache[r["key"]] = r["value"]
        for r in conn.execute("SELECT wallet,asset,shares FROM target_shadow"):
            shadow_cache[(r["wallet"], r["asset"])] = max(0.0, sf(r["shares"]))
        for r in conn.execute("SELECT * FROM copy_positions"):
            position_cache[(r["wallet"], r["asset"], r["mode"])] = dict(r)


def setting(key, default=""):
    # Hot-path reads are memory-only. Telegram changes persist to SQLite and
    # update this cache atomically in the same event loop.
    return settings_cache.get(key, default)


def set_setting(key, value):
    value = str(value)
    settings_cache[key] = value
    with db() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def bot_running():
    return setting("running", "0") == "1"


def copy_mode():
    m = setting("mode", "PAPER").upper()
    return m if m in {"PAPER", "LIVE"} else "PAPER"


def copy_usdc():
    return clamp(sf(setting("copy_usdc", DEFAULT_COPY_USDC), DEFAULT_COPY_USDC), 0.10, 100000.0)


def copy_size_mode():
    m = setting("size_mode", DEFAULT_SIZE_MODE).upper()
    return m if m in {"FIXED", "SAME_USD", "SAME_SHARES", "SCALE"} else "FIXED"


def copy_scale_pct():
    return clamp(sf(setting("scale_pct", DEFAULT_SCALE_PCT), DEFAULT_SCALE_PCT), 1.0, 10000.0)


def max_copy_usdc():
    return clamp(sf(setting("max_copy_usdc", DEFAULT_MAX_COPY_USDC), DEFAULT_MAX_COPY_USDC), 0.10, 1000000.0)


def copy_slippage():
    return clamp(sf(setting("slippage", DEFAULT_SLIPPAGE), DEFAULT_SLIPPAGE), 0.0, 0.50)


def sell_mode():
    x = setting("sell_mode", "PROPORTIONAL").upper()
    return x if x in {"PROPORTIONAL", "FULL", "OFF"} else "PROPORTIONAL"


def notify_trades():
    return setting("notify_trades", "1") == "1"


def load_wallets():
    watched_wallets.clear()
    with db() as conn:
        for r in conn.execute("SELECT * FROM watched_wallets ORDER BY added_ms"):
            watched_wallets[r["address"].lower()] = dict(r)


def load_seen_hot():
    seen_hot.clear()
    with db() as conn:
        rows = conn.execute(
            "SELECT event_key, observed_ms FROM seen_events ORDER BY observed_ms DESC LIMIT ?",
            (SEEN_CACHE_SIZE,),
        ).fetchall()
    for r in reversed(rows):
        seen_hot[r["event_key"]] = r["observed_ms"]


def persist_seen(key, wallet, source, payload, observed_ms):
    try:
        with db() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO seen_events(
                    event_key,wallet,source,observed_ms,target_timestamp,tx_hash,asset,side
                ) VALUES(?,?,?,?,?,?,?,?)""",
                (
                    key, wallet, source, observed_ms,
                    si(payload.get("timestamp"), 0),
                    str(payload.get("transactionHash") or payload.get("transaction_hash") or ""),
                    str(payload.get("asset") or payload.get("asset_id") or ""),
                    str(payload.get("side") or "").upper(),
                ),
            )
            conn.commit()
    except Exception:
        log.exception("persist_seen failed")


def shadow_get(wallet, asset):
    return max(0.0, sf(shadow_cache.get((wallet, asset), 0.0)))


def _shadow_persist(wallet, asset, shares, updated):
    try:
        with db() as conn:
            conn.execute(
                """INSERT INTO target_shadow(wallet,asset,shares,updated_ms) VALUES(?,?,?,?)
                   ON CONFLICT(wallet,asset) DO UPDATE SET shares=excluded.shares,updated_ms=excluded.updated_ms""",
                (wallet, asset, shares, updated),
            )
            conn.commit()
    except Exception:
        log.exception("shadow persist failed")


def shadow_set(wallet, asset, shares):
    # Memory first: a source inventory update must never put SQLite in front of
    # the LIVE FAK. Persistence runs after the hot-path state change.
    shares = max(0.0, sf(shares))
    shadow_cache[(wallet, asset)] = shares
    updated = now_ms()
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(_shadow_persist, wallet, asset, shares, updated))
    except RuntimeError:
        _shadow_persist(wallet, asset, shares, updated)


def position_get(wallet, asset, mode):
    p = position_cache.get((wallet, asset, mode))
    return dict(p) if p else None


def position_apply_buy(wallet, asset, mode, payload, filled, avg_price, gross):
    if filled <= 1e-12:
        return
    with db() as conn:
        r = conn.execute(
            "SELECT shares,total_cost,realized_pnl FROM copy_positions WHERE wallet=? AND asset=? AND mode=?",
            (wallet, asset, mode),
        ).fetchone()
        old_sh = sf(r["shares"]) if r else 0.0
        old_cost = sf(r["total_cost"]) if r else 0.0
        realized = sf(r["realized_pnl"]) if r else 0.0
        new_sh = old_sh + filled
        new_cost = old_cost + gross
        conn.execute(
            """INSERT INTO copy_positions(
                wallet,asset,mode,condition_id,title,slug,outcome,shares,total_cost,realized_pnl,updated_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(wallet,asset,mode) DO UPDATE SET
                condition_id=excluded.condition_id,title=excluded.title,slug=excluded.slug,
                outcome=excluded.outcome,shares=excluded.shares,total_cost=excluded.total_cost,
                realized_pnl=excluded.realized_pnl,updated_ms=excluded.updated_ms""",
            (
                wallet, asset, mode,
                str(payload.get("conditionId") or payload.get("condition_id") or ""),
                str(payload.get("title") or ""), str(payload.get("slug") or payload.get("marketSlug") or ""),
                str(payload.get("outcome") or ""), new_sh, new_cost, realized, now_ms(),
            ),
        )
        conn.commit()
        position_cache[(wallet, asset, mode)] = {
            "wallet": wallet, "asset": asset, "mode": mode,
            "condition_id": str(payload.get("conditionId") or payload.get("condition_id") or ""),
            "title": str(payload.get("title") or ""),
            "slug": str(payload.get("slug") or payload.get("marketSlug") or ""),
            "outcome": str(payload.get("outcome") or ""),
            "shares": new_sh, "total_cost": new_cost, "realized_pnl": realized,
            "updated_ms": now_ms(),
        }


def position_apply_sell(wallet, asset, mode, sold, proceeds):
    if sold <= 1e-12:
        return 0.0
    realized_delta = 0.0
    with db() as conn:
        r = conn.execute(
            "SELECT shares,total_cost,realized_pnl FROM copy_positions WHERE wallet=? AND asset=? AND mode=?",
            (wallet, asset, mode),
        ).fetchone()
        if not r:
            return 0.0
        old_sh = max(0.0, sf(r["shares"]))
        old_cost = max(0.0, sf(r["total_cost"]))
        old_realized = sf(r["realized_pnl"])
        actual_sold = min(old_sh, max(0.0, sold))
        if old_sh <= 1e-12 or actual_sold <= 1e-12:
            return 0.0
        cost_out = old_cost * (actual_sold / old_sh)
        realized_delta = proceeds - cost_out
        new_sh = max(0.0, old_sh - actual_sold)
        new_cost = max(0.0, old_cost - cost_out)
        conn.execute(
            """UPDATE copy_positions SET shares=?,total_cost=?,realized_pnl=?,updated_ms=?
               WHERE wallet=? AND asset=? AND mode=?""",
            (new_sh, new_cost, old_realized + realized_delta, now_ms(), wallet, asset, mode),
        )
        conn.commit()
        cached = position_cache.get((wallet, asset, mode), {})
        cached = dict(cached)
        cached.update({
            "wallet": wallet, "asset": asset, "mode": mode,
            "shares": new_sh, "total_cost": new_cost,
            "realized_pnl": old_realized + realized_delta, "updated_ms": now_ms(),
        })
        position_cache[(wallet, asset, mode)] = cached
    return realized_delta


def record_order(row):
    cols = [
        "event_key","wallet","wallet_label","source","detected_ms","target_timestamp","target_tx_hash",
        "condition_id","asset","title","slug","outcome","target_side","target_price","target_size",
        "mode","copy_amount_usdc","size_mode","source_amount_usdc","scale_pct","max_copy_usdc","size_capped",
        "slippage","requested_shares","limit_price","build_sign_ms",
        "detect_to_submit_ms","api_ms","total_reaction_ms","status","filled_shares","avg_price",
        "gross_amount","fee_estimate","order_id","response_json","error","created_ms",
    ]
    vals = [row.get(c) for c in cols]
    with db() as conn:
        conn.execute(
            f"INSERT INTO copy_orders({','.join(cols)}) VALUES({','.join('?' for _ in cols)})",
            vals,
        )
        conn.commit()


# ============================================================
# HTTP / LIVE CLIENT
# ============================================================

async def get_json(url, params=None, timeout=8):
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
            if r.status != 200:
                return None
            return await r.json(content_type=None)
    except Exception:
        return None


async def init_live_client():
    global live_client, live_client_ready, live_client_error
    live_client_ready = False
    live_client_error = ""
    if AsyncSecureClient is None:
        live_client_error = "polymarket-client is not installed"
        return False
    if not POLYMARKET_PRIVATE_KEY:
        live_client_error = "POLYMARKET_PRIVATE_KEY not configured"
        return False
    try:
        api_key = None
        if POLYMARKET_RELAYER_API_KEY and POLYMARKET_RELAYER_API_KEY_ADDRESS:
            api_key = RelayerApiKey(
                key=POLYMARKET_RELAYER_API_KEY,
                address=POLYMARKET_RELAYER_API_KEY_ADDRESS,
            )
        live_client = await AsyncSecureClient.create(
            private_key=POLYMARKET_PRIVATE_KEY,
            wallet=POLYMARKET_WALLET_ADDRESS or None,
            api_key=api_key,
        )
        live_client_ready = True
        log.info(
            "LIVE wallet ready | wallet=%s | signer=%s | master=%s",
            str(getattr(live_client, "wallet", POLYMARKET_WALLET_ADDRESS)),
            str(getattr(live_client, "signer", "")),
            "ON" if LIVE_MASTER_ENABLE else "OFF",
        )
        return True
    except Exception as e:
        live_client = None
        live_client_error = f"{type(e).__name__}: {e}"
        log.exception("LIVE client init failed")
        return False


async def close_live_client():
    global live_client, live_client_ready
    c = live_client
    live_client = None
    live_client_ready = False
    if c is not None:
        try:
            await c.close()
        except Exception:
            pass


async def live_collateral_balance():
    if not live_client_ready or live_client is None:
        return None
    try:
        b = await live_client.get_balance_allowance(asset_type="COLLATERAL")
        return sf(getattr(b, "balance", 0)) / 1_000_000.0
    except Exception:
        return None


async def prewarm_loop():
    global live_prewarm_last_ms
    while True:
        await asyncio.sleep(1)
        if not LIVE_PREWARM_ENABLE or not live_client_ready or live_client is None:
            continue
        if now_ms() - live_prewarm_last_ms < int(LIVE_PREWARM_INTERVAL_SEC * 1000):
            continue
        async with live_prewarm_lock:
            if now_ms() - live_prewarm_last_ms < int(LIVE_PREWARM_INTERVAL_SEC * 1000):
                continue
            started = now_ms()
            try:
                await live_client.get_balance_allowance(asset_type="COLLATERAL")
                live_prewarm_last_ms = now_ms()
                log.debug("LIVE transport prewarm OK %dms", live_prewarm_last_ms - started)
            except Exception as e:
                live_prewarm_last_ms = now_ms()
                log.debug("LIVE prewarm failed: %s", e)


def normalize_limit(price, side):
    tick = Decimal(str(LIVE_PRICE_TICK_FALLBACK))
    px = Decimal(str(price))
    rounding = ROUND_FLOOR if str(side).upper() == "BUY" else ROUND_CEILING
    units = (px / tick).to_integral_value(rounding=rounding)
    out = units * tick
    # CLOB prices must be inside (0,1).
    lo = tick
    hi = Decimal("1") - tick
    out = min(hi, max(lo, out))
    return out.normalize()


def build_limit_from_target(target_price, slippage, side):
    # Use Decimal from the textual JSON value. Float subtraction such as
    # 0.54-0.05 can become 0.49000000000000005 and a SELL ceiling would then
    # incorrectly jump to 0.50. Exact decimal arithmetic prevents that.
    p = Decimal(str(target_price))
    s = max(Decimal("0"), Decimal(str(slippage)))
    raw = p + s if str(side).upper() == "BUY" else p - s
    tick = Decimal(str(LIVE_PRICE_TICK_FALLBACK))
    raw = min(Decimal("1") - tick, max(tick, raw))
    return normalize_limit(raw, side)


def buy_copy_sizing(target_price, target_size, limit_price, size_mode=None, fixed_usdc=None, scale_pct=None, max_usdc=None, source_usdc_override=None):
    """Return (requested_shares, planned_usdc, source_usdc, mode, capped).

    SAME_USD and SCALE treat source notional as a spend *ceiling* at our slippage
    limit, so a faster/better fill can spend slightly less. SAME_SHARES mirrors the
    source shares exactly unless MAX COPY USD trims it. FIXED uses COPY_USDC.
    This is intentionally book-free so sizing adds no REST round-trip to the hot path.
    """
    tp = max(0.0, sf(target_price))
    ts = max(0.0, sf(target_size))
    lp = max(1e-9, sf(limit_price))
    mode = str(size_mode or copy_size_mode()).upper()
    if mode not in {"FIXED", "SAME_USD", "SAME_SHARES", "SCALE"}:
        mode = "FIXED"
    fixed = copy_usdc() if fixed_usdc is None else max(0.0, sf(fixed_usdc))
    scale = copy_scale_pct() if scale_pct is None else clamp(sf(scale_pct), 1.0, 10000.0)
    cap = max_copy_usdc() if max_usdc is None else max(0.10, sf(max_usdc))
    source_usdc = tp * ts if source_usdc_override is None else max(0.0, sf(source_usdc_override))
    capped = False

    if mode == "SAME_SHARES":
        requested = ts
        if requested * lp > cap + 1e-12:
            requested = cap / lp
            capped = True
        planned_usdc = requested * lp
    else:
        if mode == "SAME_USD":
            desired_usdc = source_usdc
        elif mode == "SCALE":
            desired_usdc = source_usdc * (scale / 100.0)
        else:
            desired_usdc = fixed
        planned_usdc = min(desired_usdc, cap)
        capped = desired_usdc > cap + 1e-12
        requested = planned_usdc / lp

    requested = math.floor(max(0.0, requested) * 10000) / 10000.0
    planned_usdc = min(cap, requested * lp)
    return requested, planned_usdc, source_usdc, mode, capped


async def submit_live_fak(asset, side, shares, limit_price, detected_ms):
    """Fast CLOB FAK. No REST orderbook round-trip before the first submission."""
    if not LIVE_MASTER_ENABLE:
        return {"ok": False, "status": "LIVE_MASTER_OFF", "filled": 0.0, "error": "LIVE_MASTER_ENABLE=0"}
    if not live_client_ready or live_client is None:
        return {"ok": False, "status": "WALLET_NOT_READY", "filled": 0.0, "error": live_client_error or "wallet_not_ready"}
    if shares < MIN_ORDER_SHARES - 1e-12 or shares > MAX_ORDER_SHARES + 1e-12:
        return {"ok": False, "status": "INVALID_SIZE", "filled": 0.0, "error": f"shares {shares:.4f} outside {MIN_ORDER_SHARES:g}..{MAX_ORDER_SHARES:g}"}

    side = str(side).upper()
    limit_dec = normalize_limit(limit_price, side)
    limit_str = format(limit_dec, "f")
    size_str = format(Decimal(str(round(shares, 4))), "f")

    build_start = now_ms()
    try:
        signed = await live_client.create_limit_order(
            token_id=str(asset),
            price=limit_str,
            size=size_str,
            side=side,
            post_only=False,
        )
        fak_order = replace(signed, order_type="FAK", post_only=False)
    except Exception as e:
        return {
            "ok": False, "status": "REJECTED_LOCAL", "filled": 0.0,
            "error": f"{type(e).__name__}: {e}",
            "build_sign_ms": now_ms() - build_start,
            "detect_to_submit_ms": None, "api_ms": None,
        }

    build_end = now_ms()
    attempts = 0
    last = None
    while attempts <= LIVE_NO_MATCH_RETRIES:
        if attempts and LIVE_NO_MATCH_RETRY_DELAY_MS:
            await asyncio.sleep(LIVE_NO_MATCH_RETRY_DELAY_MS / 1000.0)
            # New nonce/order hash for deterministic FAK retry.
            build_start_retry = now_ms()
            try:
                signed = await live_client.create_limit_order(
                    token_id=str(asset), price=limit_str, size=size_str, side=side, post_only=False,
                )
                fak_order = replace(signed, order_type="FAK", post_only=False)
                build_end = now_ms()
                build_start = build_start_retry
            except Exception as e:
                return {
                    "ok": False, "status": "REJECTED_LOCAL", "filled": 0.0,
                    "error": f"{type(e).__name__}: {e}",
                    "build_sign_ms": build_end - build_start,
                    "detect_to_submit_ms": None, "api_ms": None,
                }

        submit_ms = now_ms()
        try:
            if sdk_post_order_with_allowance_recovery is not None:
                response = await sdk_post_order_with_allowance_recovery(live_client, fak_order)
            else:
                response = await live_client.post_order(fak_order)
            response_ms = now_ms()

            ok = bool(getattr(response, "ok", False))
            if not ok:
                error = f"{getattr(response, 'code', 'rejected')}: {getattr(response, 'message', '')}".strip()
                if is_definite_fak_no_match_error(error):
                    last = {
                        "ok": False, "status": "REJECTED_NO_MATCH", "filled": 0.0, "error": error,
                        "build_sign_ms": build_end - build_start,
                        "detect_to_submit_ms": submit_ms - detected_ms,
                        "api_ms": response_ms - submit_ms,
                        "response_json": response_json(response),
                    }
                    attempts += 1
                    if attempts <= LIVE_NO_MATCH_RETRIES:
                        continue
                    return last
                if side == "SELL" and is_definite_balance_allowance_rejection(error):
                    return {
                        "ok": False, "status": "REJECTED_BALANCE_ALLOWANCE", "filled": 0.0, "error": error,
                        "build_sign_ms": build_end - build_start,
                        "detect_to_submit_ms": submit_ms - detected_ms,
                        "api_ms": response_ms - submit_ms,
                        "response_json": response_json(response),
                    }
                return {
                    "ok": False, "status": "REJECTED", "filled": 0.0, "error": error,
                    "build_sign_ms": build_end - build_start,
                    "detect_to_submit_ms": submit_ms - detected_ms,
                    "api_ms": response_ms - submit_ms,
                    "response_json": response_json(response),
                }

            making = sf(getattr(response, "making_amount", 0))
            taking = sf(getattr(response, "taking_amount", 0))
            if side == "BUY":
                filled = taking
                gross = making
            else:
                filled = making
                gross = taking
            avg = gross / filled if filled > 1e-12 else 0.0
            status = str(getattr(response, "status", ""))
            stored = status or "OK"
            if filled <= 1e-12 and stored.lower() in {"delayed", "live", "matched"}:
                stored = "DELAYED_AMBIGUOUS"
            return {
                "ok": True, "status": stored, "filled": filled, "avg": avg, "gross": gross,
                "fee": fee_estimate_generic(filled, avg),
                "order_id": str(getattr(response, "order_id", "")),
                "response_json": response_json(response), "error": "",
                "build_sign_ms": build_end - build_start,
                "detect_to_submit_ms": submit_ms - detected_ms,
                "api_ms": response_ms - submit_ms,
            }
        except Exception as e:
            response_ms = now_ms()
            error = f"{type(e).__name__}: {e}"
            if is_definite_fak_no_match_error(e):
                last = {
                    "ok": False, "status": "REJECTED_NO_MATCH", "filled": 0.0, "error": error,
                    "build_sign_ms": build_end - build_start,
                    "detect_to_submit_ms": submit_ms - detected_ms,
                    "api_ms": response_ms - submit_ms,
                    "response_json": "{}",
                }
                attempts += 1
                if attempts <= LIVE_NO_MATCH_RETRIES:
                    continue
                return last
            if side == "SELL" and is_definite_balance_allowance_rejection(e):
                return {
                    "ok": False, "status": "REJECTED_BALANCE_ALLOWANCE", "filled": 0.0, "error": error,
                    "build_sign_ms": build_end - build_start,
                    "detect_to_submit_ms": submit_ms - detected_ms,
                    "api_ms": response_ms - submit_ms,
                    "response_json": "{}",
                }
            # Fail closed: after POST starts, an unknown transport/API exception can
            # mean the order was accepted. Never duplicate it automatically.
            return {
                "ok": False, "status": "AMBIGUOUS", "filled": 0.0, "error": error,
                "build_sign_ms": build_end - build_start,
                "detect_to_submit_ms": submit_ms - detected_ms,
                "api_ms": response_ms - submit_ms,
                "response_json": "{}",
            }

    return last or {"ok": False, "status": "REJECTED_NO_MATCH", "filled": 0.0, "error": "no_match"}


# ============================================================
# PAPER FAK SIMULATION
# ============================================================

def parse_levels(data, side_key):
    out = []
    for x in (data or {}).get(side_key, []) or []:
        if isinstance(x, dict):
            p = sf(x.get("price"))
            q = sf(x.get("size"))
        elif isinstance(x, (list, tuple)) and len(x) >= 2:
            p, q = sf(x[0]), sf(x[1])
        else:
            continue
        if p > 0 and q > 0:
            out.append((p, q))
    return out


async def paper_fak(asset, side, shares, limit_price):
    started = now_ms()
    data = await get_json(f"{CLOB_API}/book", params={"token_id": str(asset)}, timeout=4)
    if not isinstance(data, dict):
        return {"ok": False, "status": "PAPER_NO_BOOK", "filled": 0.0, "error": "book_unavailable", "api_ms": now_ms() - started}
    side = side.upper()
    levels = parse_levels(data, "asks" if side == "BUY" else "bids")
    levels.sort(key=lambda x: x[0], reverse=(side == "SELL"))
    remaining = shares
    gross = 0.0
    filled = 0.0
    for price, qty in levels:
        if side == "BUY" and price > float(limit_price) + 1e-12:
            break
        if side == "SELL" and price < float(limit_price) - 1e-12:
            break
        take = min(remaining, qty)
        if take > 0:
            filled += take
            gross += take * price
            remaining -= take
        if remaining <= 1e-12:
            break
    if filled <= 1e-12:
        return {"ok": False, "status": "PAPER_NO_MATCH", "filled": 0.0, "error": "no_visible_liquidity", "api_ms": now_ms() - started}
    avg = gross / filled
    return {
        "ok": True, "status": "PAPER_FILLED" if filled + 1e-9 >= shares else "PAPER_PARTIAL",
        "filled": filled, "avg": avg, "gross": gross, "fee": fee_estimate_generic(filled, avg),
        "order_id": "PAPER", "response_json": "{}", "error": "", "api_ms": now_ms() - started,
        "build_sign_ms": 0,
    }


# ============================================================
# TRADE AUDIT NOTIFICATIONS
# ============================================================

def queue_trade_notice(text):
    if not notify_trades():
        return
    try:
        trade_notice_queue.put_nowait(str(text)[:4090])
    except asyncio.QueueFull:
        log.warning("Trade notice queue full; notification dropped")


async def trade_notice_loop():
    while True:
        text = await trade_notice_queue.get()
        try:
            await tg_send(text)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Trade notice send failed")
        finally:
            trade_notice_queue.task_done()


def compact_tx(payload):
    tx = str(payload.get("transactionHash") or payload.get("transaction_hash") or "")
    return f"{tx[:10]}…{tx[-6:]}" if len(tx) > 20 else tx


def source_detect_message(wallet, info, payload, source):
    side = str(payload.get("side") or "").upper()
    asset = str(payload.get("asset") or payload.get("asset_id") or "")
    before = shadow_cache.get((wallet, asset))
    if side == "BUY":
        if before is not None and sf(before) <= 1e-12:
            head = "🆕 НОВАЯ ПОЗИЦИЯ НАЙДЕНА"
        elif before is not None:
            head = "➕ ДОБОР ПОЗИЦИИ НАЙДЕН"
        else:
            head = "🔎 BUY СДЕЛКА НАЙДЕНА"
    else:
        head = "➖ SELL ПОЗИЦИИ НАЙДЕН"
    label = safe_label(info.get("label"), wallet)
    price = sf(payload.get("price"))
    size = sf(payload.get("size"))
    usdc = source_trade_usdc(payload)
    title = payload.get("title") or payload.get("slug") or asset
    outcome = payload.get("outcome") or ""
    tx = compact_tx(payload)
    fallback = "\n⚠️ Найдено через REST fallback — RTDS не был первым источником этой сделки." if str(source).startswith("rest:") else ""
    return (
        f"{head}\n"
        f"👛 {label} ({short_addr(wallet)})\n"
        f"{title}\n{outcome}\n"
        f"Источник: {side} {size:.4f}sh @ {price:.4f} ≈ ${usdc:.2f}\n"
        f"feed: {source}"
        + (f" | tx {tx}" if tx else "")
        + fallback
    )


def human_copy_reason(status, error=""):
    st = str(status or "").upper()
    er = str(error or "")
    if st == "REJECTED_NO_MATCH":
        return "FAK NO_MATCH: по допустимой цене не нашлось исполняемого объёма; рынок мог уйти быстрее нашего slippage-limit."
    if st == "PAPER_NO_MATCH":
        return "PAPER NO_MATCH: в текущем стакане не было объёма по нашей limit-цене или лучше."
    if st == "PAPER_NO_BOOK":
        return "Не удалось получить стакан для PAPER-проверки."
    if st == "PAPER_INSUFFICIENT_CASH":
        return "Недостаточно PAPER-баланса."
    if st == "REJECTED_BALANCE_ALLOWANCE":
        return "Недостаточно balance/allowance для SELL после безопасной повторной попытки."
    if st == "LIVE_MASTER_OFF":
        return "LIVE_MASTER_ENABLE=0."
    if st == "WALLET_NOT_READY":
        return "LIVE-кошелёк/подпись не готовы."
    if st == "INVALID_SIZE":
        return "Размер ордера вне разрешённого диапазона."
    if st == "REJECTED_LOCAL":
        return "Ордер не удалось собрать/подписать локально."
    if st == "AMBIGUOUS" or st == "DELAYED_AMBIGUOUS":
        return "Ответ после отправки неоднозначен; бот fail-closed и не дублирует ордер вслепую."
    if st == "REJECTED":
        return f"CLOB отклонил ордер: {er}" if er else "CLOB отклонил ордер."
    if st == "SKIPPED":
        if er == "BOT_STOPPED":
            return "Сделка источника увидена, но COPY был STOP."
        if er == "RATE_GUARD":
            return "Сработала защита от аномально частого потока (>120 source actions/min)."
        if er == "SELL_COPY_OFF":
            return "SELL-копирование отключено."
        if er == "NO_COPIED_POSITION":
            return "У бота нет своей скопированной позиции для этого SELL."
        if er == "LIVE_NOT_READY":
            return "LIVE запрошен, но master/кошелёк не готовы."
        if er.startswith("ORDER_TOO_SMALL"):
            return f"Расчётный размер меньше минимального ордера: {er.split(':',1)[-1]}."
        return er or "Сделка пропущена правилами бота."
    return er or st or "Причина не определена."


# ============================================================
# WALLET SHADOW / COPY ENGINE
# ============================================================

async def warm_target_positions(wallet):
    """Seed target inventory for proportional SELL copying. Existing target
    positions are NOT copied; they are only used as the denominator for exits."""
    data = await get_json(
        f"{DATA_API}/positions",
        params={"user": wallet, "limit": 500, "sizeThreshold": 0},
        timeout=10,
    )
    if not isinstance(data, list):
        return 0
    count = 0
    for p in data:
        asset = str(p.get("asset") or "")
        sh = max(0.0, sf(p.get("size")))
        if asset:
            shadow_set(wallet, asset, sh)
            count += 1
    return count


def target_wallet_from_payload(payload):
    addr = payload.get("proxyWallet") or payload.get("proxy_wallet")
    if not addr and isinstance(payload.get("trader"), dict):
        addr = payload["trader"].get("address")
    return normalize_address(addr)


def valid_trade_payload(payload):
    if not isinstance(payload, dict):
        return False
    if not target_wallet_from_payload(payload):
        return False
    if str(payload.get("side") or "").upper() not in {"BUY", "SELL"}:
        return False
    if not str(payload.get("asset") or payload.get("asset_id") or ""):
        return False
    if sf(payload.get("price"), -1) <= 0 or sf(payload.get("price"), -1) >= 1:
        return False
    if sf(payload.get("size"), 0) <= 0:
        return False
    return True


def wallet_rate_allowed(wallet):
    # Hard anti-loop/flood safety; 120 copied source actions/minute/wallet is far
    # above normal human/strategy activity and still protects a malformed feed.
    t = time.time()
    q = wallet_rate[wallet]
    while q and t - q[0] > 60:
        q.popleft()
    if len(q) >= 120:
        return False
    q.append(t)
    return True


async def ingest_trade(payload, source="rtds"):
    if not valid_trade_payload(payload):
        return
    wallet = target_wallet_from_payload(payload)
    info = watched_wallets.get(wallet)
    if not info or not si(info.get("enabled"), 1):
        return

    rt_stats["matched_wallet_raw_events"] += 1
    key = event_key(payload)
    if not hot_seen_add(key):
        rt_stats["duplicate_events"] += 1
        return

    detected_ms = now_ms()
    rt_stats["matched_wallet_events"] += 1
    # FOUND notification is queued immediately but Telegram is never awaited here,
    # so it cannot delay signing/submission.
    queue_trade_notice(source_detect_message(wallet, info, payload, source))
    # Persistence is intentionally off the critical path.
    asyncio.create_task(asyncio.to_thread(persist_seen, key, wallet, source, payload, detected_ms))

    if not bot_running():
        asyncio.create_task(log_skipped(
            key, wallet, info, payload, source, detected_ms, copy_mode(), copy_usdc(), copy_slippage(), "BOT_STOPPED"
        ))
        return
    if not wallet_rate_allowed(wallet):
        asyncio.create_task(log_skipped(
            key, wallet, info, payload, source, detected_ms, copy_mode(), copy_usdc(), copy_slippage(), "RATE_GUARD"
        ))
        return

    asyncio.create_task(copy_trade(key, wallet, info, payload, source, detected_ms))


async def copy_trade(key, wallet, info, payload, source, detected_ms):
    asset = str(payload.get("asset") or payload.get("asset_id") or "")
    async with asset_locks[(wallet, asset)]:
        side = str(payload.get("side") or "").upper()
        target_price = sf(payload.get("price"))
        target_size = sf(payload.get("size"))
        mode = copy_mode()
        amount = copy_usdc()
        size_mode = copy_size_mode()
        scale_pct = copy_scale_pct()
        max_usdc = max_copy_usdc()
        slip = copy_slippage()
        smode = sell_mode()
        limit_dec = build_limit_from_target(target_price, slip, side)
        limit_price = float(limit_dec)
        source_usdc = source_trade_usdc(payload)
        planned_usdc = 0.0
        size_capped = False

        shadow_before = shadow_get(wallet, asset)

        if side == "BUY":
            requested, planned_usdc, source_usdc, size_mode, size_capped = buy_copy_sizing(
                target_price, target_size, limit_price, size_mode=size_mode,
                fixed_usdc=amount, scale_pct=scale_pct, max_usdc=max_usdc,
                source_usdc_override=source_usdc,
            )
            shadow_after = shadow_before + target_size
            shadow_set(wallet, asset, shadow_after)
        else:
            pos = position_get(wallet, asset, mode)
            copied_before = max(0.0, sf(pos.get("shares"))) if pos else 0.0
            shadow_after = max(0.0, shadow_before - target_size)
            shadow_set(wallet, asset, shadow_after)
            if smode == "OFF":
                await log_skipped(key, wallet, info, payload, source, detected_ms, mode, amount, slip, "SELL_COPY_OFF")
                return
            if copied_before <= 1e-12:
                await log_skipped(key, wallet, info, payload, source, detected_ms, mode, amount, slip, "NO_COPIED_POSITION")
                return
            if smode == "FULL" or shadow_before <= 1e-12:
                requested = copied_before
            else:
                ratio = clamp(target_size / shadow_before, 0.0, 1.0)
                requested = copied_before * ratio
            requested = math.floor(requested * 10000) / 10000.0

        effective_amount = planned_usdc if side == "BUY" else amount
        if requested < MIN_ORDER_SHARES - 1e-12:
            await log_skipped(
                key, wallet, info, payload, source, detected_ms, mode, effective_amount, slip,
                f"ORDER_TOO_SMALL:{requested:.4f}<{MIN_ORDER_SHARES:g}", requested, limit_price,
            )
            return
        if requested > MAX_ORDER_SHARES:
            requested = MAX_ORDER_SHARES
            if side == "BUY":
                size_capped = True
                planned_usdc = requested * limit_price
                effective_amount = planned_usdc

        if mode == "LIVE":
            if not LIVE_MASTER_ENABLE or not live_client_ready:
                await log_skipped(
                    key, wallet, info, payload, source, detected_ms, mode, effective_amount, slip,
                    "LIVE_NOT_READY", requested, limit_price,
                )
                return
            result = await submit_live_fak(asset, side, requested, limit_dec, detected_ms)
            # Explicit balance rejection is safe to retry once on SELL after the
            # short CLOB balance-cache propagation window.
            if side == "SELL" and result.get("status") == "REJECTED_BALANCE_ALLOWANCE":
                await asyncio.sleep(LIVE_SELL_BALANCE_RETRY_MS / 1000.0)
                result = await submit_live_fak(asset, side, requested, limit_dec, detected_ms)
        else:
            # PAPER deliberately checks the actual current book instead of
            # pretending a fill at the target wallet's historical trade price.
            result = await paper_fak(asset, side, requested, limit_dec)
            result["detect_to_submit_ms"] = 0

        filled = max(0.0, sf(result.get("filled")))
        avg = sf(result.get("avg"))
        gross = sf(result.get("gross"))
        if result.get("ok") and filled > 1e-12:
            if mode == "PAPER":
                cash = sf(setting("paper_cash", DEFAULT_PAPER_BALANCE), DEFAULT_PAPER_BALANCE)
                if side == "BUY":
                    if gross > cash + 1e-9:
                        result = {**result, "ok": False, "status": "PAPER_INSUFFICIENT_CASH", "filled": 0.0, "error": "paper balance too low"}
                        filled = 0.0
                        gross = 0.0
                    else:
                        set_setting("paper_cash", cash - gross)
                else:
                    set_setting("paper_cash", cash + gross)

            if filled > 1e-12:
                if side == "BUY":
                    position_apply_buy(wallet, asset, mode, payload, filled, avg, gross)
                else:
                    position_apply_sell(wallet, asset, mode, filled, gross)

        total_reaction = now_ms() - detected_ms
        row = base_order_row(key, wallet, info, payload, source, detected_ms, mode, effective_amount, slip)
        row.update({
            "size_mode": size_mode if side == "BUY" else copy_size_mode(),
            "source_amount_usdc": source_usdc,
            "scale_pct": scale_pct,
            "max_copy_usdc": max_usdc,
            "size_capped": 1 if size_capped else 0,
            "requested_shares": requested,
            "limit_price": limit_price,
            "build_sign_ms": result.get("build_sign_ms"),
            "detect_to_submit_ms": result.get("detect_to_submit_ms"),
            "api_ms": result.get("api_ms"),
            "total_reaction_ms": total_reaction,
            "status": str(result.get("status") or ("FILLED" if filled else "REJECTED")),
            "filled_shares": filled,
            "avg_price": avg if filled > 0 else None,
            "gross_amount": gross,
            "fee_estimate": sf(result.get("fee")),
            "order_id": str(result.get("order_id") or ""),
            "response_json": str(result.get("response_json") or "{}"),
            "error": str(result.get("error") or ""),
            "created_ms": now_ms(),
        })
        await asyncio.to_thread(record_order, row)

        if notify_trades():
            label = safe_label(info.get("label"), wallet)
            ambiguous = str(row["status"]).upper() in {"AMBIGUOUS", "DELAYED_AMBIGUOUS"}
            full_fill = filled > 1e-12 and filled + 1e-9 >= requested
            if full_fill:
                head = "✅ ПОЗИЦИЯ ИСПОЛНЕНА"
            elif filled > 1e-12:
                head = "⚠️ ПОЗИЦИЯ ЧАСТИЧНО ИСПОЛНЕНА"
            elif ambiguous:
                head = "⚠️ РЕЗУЛЬТАТ ОРДЕРА НЕОДНОЗНАЧЕН"
            else:
                head = "❌ ПОЗИЦИЯ НЕ ИСПОЛНЕНА"
            target_line = f"Источник {side} {target_size:.4f}sh @ {target_price:.4f} (${source_usdc:.2f})"
            if side == "BUY":
                sizing_line = f"Размер {size_mode}" + (f" {scale_pct:.0f}%" if size_mode == "SCALE" else "")
                sizing_line += f" | план <=${planned_usdc:.2f}" + (" | MAX cap" if size_capped else "")
            else:
                sizing_line = f"SELL {smode}"
            if filled > 0:
                our_line = f"Наш fill {filled:.4f}/{requested:.4f}sh @ {avg:.4f} (${gross:.2f}) | limit {limit_price:.4f}"
            else:
                our_line = f"Fill 0/{requested:.4f}sh | limit {limit_price:.4f} | {row['status']}"
            latency = []
            if row.get("detect_to_submit_ms") is not None:
                latency.append(f"detect→submit {si(row['detect_to_submit_ms'])}ms")
            if row.get("api_ms") is not None:
                latency.append(f"API {si(row['api_ms'])}ms")
            reason = "" if filled > 0 else "\nПричина: " + human_copy_reason(row["status"], row["error"])
            queue_trade_notice(
                f"{head}\n"
                f"👛 {label}\n"
                f"{payload.get('title') or payload.get('slug') or asset}\n"
                f"{payload.get('outcome') or ''}\n"
                f"{target_line}\n{sizing_line}\n{our_line}{reason}\n"
                f"mode {mode} | source {source}"
                + (f"\n⏱ {' | '.join(latency)}" if latency else "")
            )


def base_order_row(key, wallet, info, payload, source, detected_ms, mode, amount, slip):
    return {
        "event_key": key,
        "wallet": wallet,
        "wallet_label": safe_label(info.get("label"), wallet),
        "source": source,
        "detected_ms": detected_ms,
        "target_timestamp": si(payload.get("timestamp"), 0),
        "target_tx_hash": str(payload.get("transactionHash") or payload.get("transaction_hash") or ""),
        "condition_id": str(payload.get("conditionId") or payload.get("condition_id") or ""),
        "asset": str(payload.get("asset") or payload.get("asset_id") or ""),
        "title": str(payload.get("title") or ""),
        "slug": str(payload.get("slug") or payload.get("marketSlug") or ""),
        "outcome": str(payload.get("outcome") or ""),
        "target_side": str(payload.get("side") or "").upper(),
        "target_price": sf(payload.get("price")),
        "target_size": sf(payload.get("size")),
        "mode": mode,
        "copy_amount_usdc": amount,
        "size_mode": copy_size_mode(),
        "source_amount_usdc": source_trade_usdc(payload),
        "scale_pct": copy_scale_pct(),
        "max_copy_usdc": max_copy_usdc(),
        "size_capped": 0,
        "slippage": slip,
    }


async def log_skipped(key, wallet, info, payload, source, detected_ms, mode, amount, slip, reason, requested=None, limit_price=None):
    row = base_order_row(key, wallet, info, payload, source, detected_ms, mode, amount, slip)
    row.update({
        "requested_shares": requested,
        "limit_price": limit_price,
        "build_sign_ms": None,
        "detect_to_submit_ms": None,
        "api_ms": None,
        "total_reaction_ms": now_ms() - detected_ms,
        "status": "SKIPPED",
        "filled_shares": 0.0,
        "avg_price": None,
        "gross_amount": 0.0,
        "fee_estimate": 0.0,
        "order_id": "",
        "response_json": "{}",
        "error": reason,
        "created_ms": now_ms(),
    })
    await asyncio.to_thread(record_order, row)
    if notify_trades():
        queue_trade_notice(
            f"❌ ПОЗИЦИЯ НЕ ИСПОЛНЕНА\n"
            f"👛 {safe_label(info.get('label'), wallet)}\n"
            f"{payload.get('title') or payload.get('slug') or payload.get('asset') or ''}\n"
            f"{payload.get('side')} {payload.get('outcome') or ''} {sf(payload.get('size')):.4f}sh @ {sf(payload.get('price')):.4f}\n"
            f"Причина: {human_copy_reason('SKIPPED', reason)}\n"
            f"source {source}"
        )


# ============================================================
# RTDS FAST FEED + REST FALLBACK
# ============================================================

def iter_ws_messages(raw):
    if raw is None:
        return []
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if not isinstance(raw, str) or raw.lower() in {"ping", "pong", ""}:
        return []
    try:
        x = json.loads(raw)
    except Exception:
        return []
    return x if isinstance(x, list) else [x]


async def rtds_ping(ws):
    while True:
        await asyncio.sleep(5)
        try:
            await ws.send_str("ping")
        except Exception:
            return


async def rtds_loop():
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
            async with session.ws_connect(RTDS_URL, heartbeat=None, timeout=timeout, max_msg_size=8_000_000) as ws:
                rt_stats["connected"] = True
                rt_stats["reconnects"] += 1
                rt_stats["last_error"] = ""
                await ws.send_str(jd({
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": "activity", "type": "orders_matched"},
                        {"topic": "activity", "type": "trades"},
                    ],
                }))
                ping_task = asyncio.create_task(rtds_ping(ws))
                log.info("RTDS connected: %s", RTDS_URL)
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            rt_stats["last_message_ms"] = now_ms()
                            rt_stats["messages"] += 1
                            for envelope in iter_ws_messages(msg.data):
                                if not isinstance(envelope, dict):
                                    continue
                                topic = str(envelope.get("topic") or "")
                                typ = str(envelope.get("type") or "")
                                if topic and topic != "activity":
                                    continue
                                if typ and typ not in {"trades", "orders_matched"}:
                                    continue
                                payload = envelope.get("payload", envelope.get("data", envelope))
                                if isinstance(payload, list):
                                    for p in payload:
                                        if isinstance(p, dict):
                                            await ingest_trade(p, source=f"rtds:{typ or 'activity'}")
                                elif isinstance(payload, dict):
                                    await ingest_trade(payload, source=f"rtds:{typ or 'activity'}")
                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            break
                finally:
                    ping_task.cancel()
                    rt_stats["connected"] = False
        except asyncio.CancelledError:
            raise
        except Exception as e:
            rt_stats["connected"] = False
            rt_stats["last_error"] = f"{type(e).__name__}: {e}"
            log.warning("RTDS reconnect: %s", e)
            await asyncio.sleep(0.5)


async def rest_fallback_loop():
    primed = set()
    while True:
        try:
            if not REST_FALLBACK_ENABLE or not watched_wallets:
                await asyncio.sleep(1)
                continue
            wallets = list(watched_wallets)
            for wallet in wallets:
                data = await get_json(
                    f"{DATA_API}/trades",
                    params={"user": wallet, "limit": 20, "takerOnly": "false"},
                    timeout=4,
                )
                if not isinstance(data, list):
                    continue
                # Oldest first so shadow/position logic sees source actions in order.
                data = sorted(data, key=lambda x: si(x.get("timestamp"), 0))
                first = wallet not in primed
                for p in data:
                    if not isinstance(p, dict):
                        continue
                    # Data API response should already contain proxyWallet, but
                    # normalize defensively if an endpoint revision omits it.
                    p.setdefault("proxyWallet", wallet)
                    ts = si(p.get("timestamp"), 0)
                    age = time.time() - ts if ts else 999999
                    k = event_key(p)
                    if first and age > REST_MAX_COPY_AGE_SEC:
                        hot_seen_add(k)
                        asyncio.create_task(asyncio.to_thread(persist_seen, k, wallet, "rest:prime", p, now_ms()))
                        continue
                    if age <= REST_MAX_COPY_AGE_SEC:
                        await ingest_trade(p, source="rest:fallback")
                primed.add(wallet)
            await asyncio.sleep(REST_FALLBACK_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("REST fallback loop")
            await asyncio.sleep(1)


# ============================================================
# TELEGRAM UI
# ============================================================

def main_keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "📐 COPY SIZE"}, {"text": "💵 AMOUNT"}],
            [{"text": "📈 SCALE %"}, {"text": "🧱 MAX COPY"}],
            [{"text": "🎚 SLIPPAGE"}, {"text": "👛 WALLETS"}],
            [{"text": "➕ ADD WALLET"}, {"text": "➖ REMOVE WALLET"}],
            [{"text": "📊 REPORT"}, {"text": "💰 BALANCE"}],
            [{"text": "⚙️ MODE"}, {"text": "🔁 SELL MODE"}],
            [{"text": "🔔 NOTIFY"}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


async def tg_api(method, payload=None, timeout=35):
    if not TELEGRAM_BOT_TOKEN:
        return None
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}",
            json=payload or {}, timeout=aiohttp.ClientTimeout(total=timeout),
        ) as r:
            return await r.json(content_type=None)
    except Exception:
        return None


async def tg_send(text, reply_markup=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": str(text)[:4090]}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    await tg_api("sendMessage", payload)


async def tg_answer_callback(callback_id, text=""):
    await tg_api("answerCallbackQuery", {"callback_query_id": callback_id, "text": text[:180]})


def status_text():
    mode = copy_mode()
    live = "READY" if live_client_ready else f"NOT READY ({live_client_error or 'no signer'})"
    return (
        f"⚡ UltraFast CopyBot {VERSION}\n"
        f"Trading: {'START' if bot_running() else 'STOP'} | Mode: {mode}\n"
        f"BUY size: {copy_size_mode()}"
        + (f" {copy_scale_pct():.0f}%" if copy_size_mode() == "SCALE" else "")
        + (f" | fixed ${copy_usdc():.2f}" if copy_size_mode() == "FIXED" else "")
        + f" | MAX ${max_copy_usdc():.2f}\n"
        f"Slippage: {copy_slippage():.3f} | SELL: {sell_mode()} | Wallets: {len(watched_wallets)}/{MAX_WALLETS}\n"
        f"RTDS: {'CONNECTED' if rt_stats['connected'] else 'DISCONNECTED'} | unique {rt_stats['matched_wallet_events']} | raw {rt_stats['matched_wallet_raw_events']} | dup {rt_stats['duplicate_events']}\n"
        f"LIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'} | wallet {live}"
    )


async def show_copy_size_menu():
    mode = copy_size_mode()
    await tg_send(
        f"Current BUY size mode: {mode}\n"
        "FIXED = fixed USD amount.\n"
        "SAME USD = mirror source dollar notional as a max spend at our slippage cap.\n"
        "SAME SHARES = mirror source shares 1:1.\n"
        "SCALE = source dollar notional × selected %.\n"
        f"MAX COPY = ${max_copy_usdc():.2f} per BUY.",
        {"inline_keyboard": [
            [{"text": "FIXED USD", "callback_data": "size:FIXED"}, {"text": "SAME USD 1:1", "callback_data": "size:SAME_USD"}],
            [{"text": "SAME SHARES 1:1", "callback_data": "size:SAME_SHARES"}, {"text": "SCALE %", "callback_data": "size:SCALE"}],
        ]},
    )


async def show_scale_menu():
    vals = [25, 50, 75, 100, 150, 200]
    rows = []
    for i in range(0, len(vals), 3):
        rows.append([{"text": f"{v}%", "callback_data": f"scale:{v}"} for v in vals[i:i+3]])
    rows.append([{"text": "✏️ CUSTOM", "callback_data": "scale:custom"}])
    await tg_send(f"Current SCALE: {copy_scale_pct():.0f}% of source USD notional", {"inline_keyboard": rows})


async def show_max_copy_menu():
    vals = [5, 10, 25, 50, 100, 250]
    rows = []
    for i in range(0, len(vals), 3):
        rows.append([{"text": f"${v}", "callback_data": f"maxcopy:{v}"} for v in vals[i:i+3]])
    rows.append([{"text": "✏️ CUSTOM", "callback_data": "maxcopy:custom"}])
    await tg_send(f"Current MAX COPY: ${max_copy_usdc():.2f} per BUY", {"inline_keyboard": rows})


async def show_amount_menu():
    vals = [1, 2, 5, 10, 20, 50]
    rows = []
    for i in range(0, len(vals), 3):
        rows.append([{"text": f"${v}", "callback_data": f"amt:{v}"} for v in vals[i:i+3]])
    rows.append([{"text": "✏️ CUSTOM", "callback_data": "amt:custom"}])
    await tg_send(f"Current FIXED amount: ${copy_usdc():.2f} per copied BUY\nUsed only when COPY SIZE = FIXED.", {"inline_keyboard": rows})


async def show_slippage_menu():
    vals = [0.01, 0.02, 0.03, 0.05, 0.07, 0.10]
    rows = []
    for i in range(0, len(vals), 3):
        rows.append([{"text": f"{v:.2f}", "callback_data": f"slip:{v}"} for v in vals[i:i+3]])
    rows.append([{"text": "✏️ CUSTOM", "callback_data": "slip:custom"}])
    await tg_send(
        f"Current slippage: {copy_slippage():.3f}\nBUY cap = target price + slippage; SELL floor = target price - slippage.",
        {"inline_keyboard": rows},
    )


async def show_mode_menu():
    await tg_send(
        f"Current mode: {copy_mode()}",
        {"inline_keyboard": [[
            {"text": "📝 PAPER", "callback_data": "mode:paper"},
            {"text": "🔴 LIVE", "callback_data": "mode:live_request"},
        ]]},
    )


async def show_sell_mode_menu():
    await tg_send(
        f"Current SELL mode: {sell_mode()}\nPROPORTIONAL mirrors the target wallet's sold fraction using its tracked inventory.",
        {"inline_keyboard": [[
            {"text": "PROPORTIONAL", "callback_data": "sell:PROPORTIONAL"},
            {"text": "FULL", "callback_data": "sell:FULL"},
            {"text": "OFF", "callback_data": "sell:OFF"},
        ]]},
    )


async def show_wallets(remove=False, report=False):
    if not watched_wallets:
        await tg_send("No watched wallets. Tap ➕ ADD WALLET.")
        return
    lines = ["👛 Watched wallets:"]
    buttons = []
    for i, (addr, info) in enumerate(watched_wallets.items(), 1):
        label = safe_label(info.get("label"), addr)
        lines.append(f"{i}. {label} — {short_addr(addr)}")
        if remove:
            buttons.append([{"text": f"❌ {label}", "callback_data": f"rm:{addr}"}])
        if report:
            buttons.append([{"text": f"📊 {label}", "callback_data": f"rep:{addr}"}])
    await tg_send("\n".join(lines), {"inline_keyboard": buttons} if buttons else None)


async def send_balance():
    live_bal = await live_collateral_balance()
    paper = sf(setting("paper_cash", DEFAULT_PAPER_BALANCE), DEFAULT_PAPER_BALANCE)
    await tg_send(
        f"💰 BALANCE\nPaper cash: ${paper:.2f}\n"
        f"Live collateral: {('$%.2f' % live_bal) if live_bal is not None else 'unavailable'}\n"
        f"Mode: {copy_mode()} | {'START' if bot_running() else 'STOP'}"
    )


def wallet_report_text(wallet):
    info = watched_wallets.get(wallet) or {"label": short_addr(wallet)}
    label = safe_label(info.get("label"), wallet)
    with db() as conn:
        orders = conn.execute(
            "SELECT * FROM copy_orders WHERE wallet=? ORDER BY id DESC LIMIT 5000",
            (wallet,),
        ).fetchall()
        positions = conn.execute(
            "SELECT * FROM copy_positions WHERE wallet=? AND shares>0.000001 ORDER BY updated_ms DESC",
            (wallet,),
        ).fetchall()
    total = len(orders)
    filled_rows = [r for r in orders if sf(r["filled_shares"]) > 0]
    buys = [r for r in filled_rows if r["target_side"] == "BUY"]
    sells = [r for r in filled_rows if r["target_side"] == "SELL"]
    skipped = sum(1 for r in orders if r["status"] == "SKIPPED")
    rejected = sum(1 for r in orders if sf(r["filled_shares"]) <= 0 and r["status"] not in {"SKIPPED"})
    capped_buys = sum(1 for r in orders if r["target_side"] == "BUY" and si(r["size_capped"]) == 1)
    rtds_events = sum(1 for r in orders if str(r["source"] or "").startswith("rtds:"))
    rest_events = sum(1 for r in orders if str(r["source"] or "").startswith("rest:"))
    stopped_seen = sum(1 for r in orders if r["status"] == "SKIPPED" and str(r["error"] or "") == "BOT_STOPPED")
    last_event_ms = max((si(r["created_ms"]) for r in orders), default=0)
    lat_submit = [r["detect_to_submit_ms"] for r in orders if r["detect_to_submit_ms"] is not None and r["mode"] == "LIVE"]
    api = [r["api_ms"] for r in orders if r["api_ms"] is not None and r["mode"] == "LIVE"]
    with db() as conn:
        pnl_rows = conn.execute(
            "SELECT mode,SUM(realized_pnl) p FROM copy_positions WHERE wallet=? GROUP BY mode",
            (wallet,),
        ).fetchall()
    pnl = {r["mode"]: sf(r["p"]) for r in pnl_rows}
    lines = [
        f"📊 {label}",
        wallet,
        f"Events/orders: {total} | fills {len(filled_rows)} | BUY {len(buys)} | SELL {len(sells)}",
        f"Skipped {skipped} | no-fill/rejected {rejected} | MAX-capped BUY {capped_buys}",
        f"Feed audit: RTDS {rtds_events} | REST fallback {rest_events} | seen while STOP {stopped_seen}",
        f"Current sizing: {copy_size_mode()}" + (f" {copy_scale_pct():.0f}%" if copy_size_mode() == "SCALE" else "") + f" | MAX ${max_copy_usdc():.2f}",
        f"Tracked realized GROSS PnL: PAPER {pnl.get('PAPER',0):+.2f} | LIVE {pnl.get('LIVE',0):+.2f}",
    ]
    if last_event_ms:
        lines.append(f"Last detected/copy decision: {utc_iso(last_event_ms / 1000.0)}")
    if lat_submit:
        lines.append(
            f"LIVE detect→submit: avg {sum(lat_submit)/len(lat_submit):.0f}ms | p50 {percentile(lat_submit,.5):.0f} | p95 {percentile(lat_submit,.95):.0f}"
        )
    if api:
        lines.append(
            f"LIVE API: avg {sum(api)/len(api):.0f}ms | p50 {percentile(api,.5):.0f} | p95 {percentile(api,.95):.0f}"
        )
    if positions:
        lines.append("Open bot-tracked allocations:")
        for p in positions[:12]:
            sh = sf(p["shares"])
            cost = sf(p["total_cost"])
            avg = cost / sh if sh > 0 else 0
            title = p["title"] or p["slug"] or p["asset"][-10:]
            lines.append(f"• {p['mode']} {p['outcome'] or ''} {sh:.4f}sh avg {avg:.4f} — {title[:55]}")
        if len(positions) > 12:
            lines.append(f"… +{len(positions)-12} more")
    lines.append("PnL above is bot-tracked gross PnL; exchange fees/rebates are not guessed as one universal rate.")
    return "\n".join(lines)


async def send_overview_report():
    if not watched_wallets:
        await tg_send("No watched wallets.")
        return
    lines = ["📊 COPY REPORT", status_text(), ""]
    with db() as conn:
        for addr, info in watched_wallets.items():
            r = conn.execute(
                """SELECT COUNT(*) n, SUM(CASE WHEN filled_shares>0 THEN 1 ELSE 0 END) fills,
                          AVG(CASE WHEN mode='LIVE' THEN detect_to_submit_ms END) lat
                   FROM copy_orders WHERE wallet=?""",
                (addr,),
            ).fetchone()
            p = conn.execute(
                "SELECT SUM(realized_pnl) p FROM copy_positions WHERE wallet=?",
                (addr,),
            ).fetchone()
            lines.append(
                f"{safe_label(info.get('label'), addr)}: events {si(r['n'])}, fills {si(r['fills'])}, "
                f"PnL {sf(p['p']):+.2f}" + (f", d→s {sf(r['lat']):.0f}ms" if r["lat"] is not None else "")
            )
    buttons = [[{"text": f"📊 {safe_label(info.get('label'), addr)}", "callback_data": f"rep:{addr}"}]
               for addr, info in watched_wallets.items()]
    await tg_send("\n".join(lines), {"inline_keyboard": buttons})


async def handle_callback(q):
    cid = str(q.get("id") or "")
    data = str(q.get("data") or "")
    await tg_answer_callback(cid)
    if data.startswith("size:"):
        v = data.split(":", 1)[1].upper()
        if v in {"FIXED", "SAME_USD", "SAME_SHARES", "SCALE"}:
            set_setting("size_mode", v)
            await tg_send(f"✅ BUY copy size mode = {copy_size_mode()}" + (f" ({copy_scale_pct():.0f}%)" if v == "SCALE" else ""))
    elif data.startswith("scale:"):
        v = data.split(":", 1)[1]
        if v == "custom":
            pending_input["type"] = "scale_pct"
            await tg_send("Send SCALE percent, for example: 50 or 125")
        else:
            set_setting("scale_pct", clamp(sf(v), 1, 10000))
            set_setting("size_mode", "SCALE")
            await tg_send(f"✅ SCALE = {copy_scale_pct():.0f}% and BUY size mode = SCALE")
    elif data.startswith("maxcopy:"):
        v = data.split(":", 1)[1]
        if v == "custom":
            pending_input["type"] = "max_copy_usdc"
            await tg_send("Send MAX COPY USD per BUY, for example: 100")
        else:
            set_setting("max_copy_usdc", clamp(sf(v), 0.10, 1000000))
            await tg_send(f"✅ MAX COPY = ${max_copy_usdc():.2f} per BUY")
    elif data.startswith("amt:"):
        v = data.split(":", 1)[1]
        if v == "custom":
            pending_input["type"] = "amount"
            await tg_send("Send custom COPY amount in USD, for example: 7.5")
        else:
            set_setting("copy_usdc", sf(v))
            await tg_send(f"✅ COPY amount = ${copy_usdc():.2f}")
    elif data.startswith("slip:"):
        v = data.split(":", 1)[1]
        if v == "custom":
            pending_input["type"] = "slippage"
            await tg_send("Send absolute price slippage, for example: 0.04")
        else:
            set_setting("slippage", sf(v))
            await tg_send(f"✅ Slippage = {copy_slippage():.3f}")
    elif data == "mode:paper":
        set_setting("running", "0")
        set_setting("mode", "PAPER")
        await tg_send("📝 Mode = PAPER. Bot was STOPPED; press START when ready.")
    elif data == "mode:live_request":
        if not LIVE_MASTER_ENABLE:
            await tg_send("⛔ LIVE_MASTER_ENABLE=0 in server variables.")
        elif not live_client_ready:
            await tg_send(f"⛔ LIVE wallet not ready: {live_client_error}")
        else:
            await tg_send(
                "⚠️ LIVE will submit REAL FAK orders when watched wallets trade. Confirm?",
                {"inline_keyboard": [[{"text": "🔴 CONFIRM LIVE", "callback_data": "mode:live_confirm"}]]},
            )
    elif data == "mode:live_confirm":
        set_setting("running", "0")
        set_setting("mode", "LIVE")
        await tg_send("🔴 Mode = LIVE. Bot remains STOPPED. Press START to begin real copying.")
    elif data.startswith("sell:"):
        v = data.split(":", 1)[1].upper()
        if v in {"PROPORTIONAL", "FULL", "OFF"}:
            set_setting("sell_mode", v)
            await tg_send(f"✅ SELL mode = {v}")
    elif data.startswith("rm:"):
        addr = normalize_address(data.split(":", 1)[1])
        if addr and addr in watched_wallets:
            label = safe_label(watched_wallets[addr].get("label"), addr)
            with db() as conn:
                conn.execute("DELETE FROM watched_wallets WHERE address=?", (addr,))
                conn.commit()
            load_wallets()
            await tg_send(f"🗑 Removed {label} ({short_addr(addr)}). Existing bot-tracked positions/history were kept for reporting.")
    elif data.startswith("rep:"):
        addr = normalize_address(data.split(":", 1)[1])
        if addr:
            await tg_send(wallet_report_text(addr))


async def handle_text(text):
    raw = str(text or "").strip()
    upper = raw.upper()

    # Pending custom input / wallet entry.
    if pending_input.get("type") == "scale_pct":
        pending_input["type"] = None
        v = sf(raw, -1)
        if v < 1 or v > 10000:
            await tg_send("Invalid SCALE %. Use 1..10000")
        else:
            set_setting("scale_pct", v)
            set_setting("size_mode", "SCALE")
            await tg_send(f"✅ SCALE = {copy_scale_pct():.0f}% and BUY size mode = SCALE")
        return
    if pending_input.get("type") == "max_copy_usdc":
        pending_input["type"] = None
        v = sf(raw, -1)
        if v < 0.10 or v > 1000000:
            await tg_send("Invalid MAX COPY. Use $0.10..$1,000,000")
        else:
            set_setting("max_copy_usdc", v)
            await tg_send(f"✅ MAX COPY = ${max_copy_usdc():.2f} per BUY")
        return
    if pending_input.get("type") == "amount":
        pending_input["type"] = None
        v = sf(raw, -1)
        if v <= 0:
            await tg_send("Invalid amount.")
        else:
            set_setting("copy_usdc", clamp(v, 0.10, 100000))
            await tg_send(f"✅ COPY amount = ${copy_usdc():.2f}")
        return
    if pending_input.get("type") == "slippage":
        pending_input["type"] = None
        v = sf(raw, -1)
        if v < 0 or v > 0.50:
            await tg_send("Invalid slippage. Use 0.00..0.50")
        else:
            set_setting("slippage", v)
            await tg_send(f"✅ Slippage = {copy_slippage():.3f}")
        return
    if pending_input.get("type") == "add_wallet":
        pending_input["type"] = None
        parts = raw.split(maxsplit=1)
        addr = normalize_address(parts[0] if parts else "")
        label = parts[1].strip() if len(parts) > 1 else ""
        if not addr:
            await tg_send("Invalid address. Format: 0x... Label")
            return
        if addr == normalize_address(POLYMARKET_WALLET_ADDRESS):
            await tg_send("⛔ You cannot watch/copy your own execution wallet (self-copy loop protection).")
            return
        if addr not in watched_wallets and len(watched_wallets) >= MAX_WALLETS:
            await tg_send(f"Wallet limit reached: {MAX_WALLETS}")
            return
        label = safe_label(label, addr)
        with db() as conn:
            conn.execute(
                """INSERT INTO watched_wallets(address,label,enabled,added_ms) VALUES(?,?,1,?)
                   ON CONFLICT(address) DO UPDATE SET label=excluded.label,enabled=1""",
                (addr, label, now_ms()),
            )
            conn.commit()
        load_wallets()
        await tg_send(f"✅ Added {label}\n{addr}\nExisting positions will NOT be copied. I am only seeding target inventory for proportional future SELLs.")
        asyncio.create_task(_warm_wallet_notify(addr, label))
        return

    if upper in {"/START", "▶️ START", "START"}:
        if copy_mode() == "LIVE" and (not LIVE_MASTER_ENABLE or not live_client_ready):
            await tg_send(f"⛔ LIVE cannot start. Master={'ON' if LIVE_MASTER_ENABLE else 'OFF'}; wallet={live_client_error or 'not ready'}")
            return
        if not watched_wallets:
            await tg_send("Add at least one wallet first.")
            return
        set_setting("running", "1")
        await tg_send("▶️ COPY STARTED\n" + status_text(), main_keyboard())
    elif upper in {"⏹ STOP", "STOP", "/STOP", "EMERGENCY STOP", "/EMERGENCY"}:
        set_setting("running", "0")
        await tg_send("⏹ COPY STOPPED. New source trades will not create orders.", main_keyboard())
    elif upper in {"📐 COPY SIZE", "COPY SIZE", "COPYSIZE", "/COPYSIZE"}:
        await show_copy_size_menu()
    elif upper in {"💵 AMOUNT", "AMOUNT", "/AMOUNT"}:
        await show_amount_menu()
    elif upper in {"📈 SCALE %", "SCALE %", "SCALE", "/SCALE"}:
        await show_scale_menu()
    elif upper in {"🧱 MAX COPY", "MAX COPY", "MAXCOPY", "/MAXCOPY"}:
        await show_max_copy_menu()
    elif upper in {"🎚 SLIPPAGE", "SLIPPAGE", "/SLIPPAGE"}:
        await show_slippage_menu()
    elif upper in {"👛 WALLETS", "WALLETS", "/WALLETS"}:
        await show_wallets()
    elif upper in {"➕ ADD WALLET", "ADD WALLET", "/ADDWALLET"}:
        pending_input["type"] = "add_wallet"
        await tg_send("Send wallet address and optional name:\n0x1234... Powerwinner")
    elif upper in {"➖ REMOVE WALLET", "REMOVE WALLET", "/REMOVEWALLET"}:
        await show_wallets(remove=True)
    elif upper in {"📊 REPORT", "REPORT", "/REPORT"}:
        await send_overview_report()
    elif upper in {"💰 BALANCE", "BALANCE", "/BALANCE"}:
        await send_balance()
    elif upper in {"⚙️ MODE", "MODE", "/MODE"}:
        await show_mode_menu()
    elif upper in {"🔁 SELL MODE", "SELL MODE", "SELLMODE", "/SELLMODE"}:
        await show_sell_mode_menu()
    elif upper in {"🔔 NOTIFY", "NOTIFY", "/NOTIFY"}:
        new = "0" if notify_trades() else "1"
        set_setting("notify_trades", new)
        await tg_send(f"🔔 Trade notifications: {'ON' if new == '1' else 'OFF'}")
    elif upper in {"STATUS", "/STATUS", "MENU", "/MENU", "HELP", "/HELP"}:
        await tg_send(status_text(), main_keyboard())
    else:
        await tg_send(status_text(), main_keyboard())


async def _warm_wallet_notify(addr, label):
    n = await warm_target_positions(addr)
    await tg_send(f"👛 {label}: target inventory seeded for {n} open assets. Only NEW trades are eligible for copying.")


async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        while True:
            await asyncio.sleep(3600)
    offset = 0
    await tg_send(status_text(), main_keyboard())
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                data = await r.json(content_type=None)
            for upd in data.get("result", []):
                offset = max(offset, si(upd.get("update_id")) + 1)
                cq = upd.get("callback_query")
                if cq:
                    msg = cq.get("message") or {}
                    chat = (msg.get("chat") or {}).get("id", "")
                    if str(chat) == str(TELEGRAM_CHAT_ID):
                        await handle_callback(cq)
                    continue
                msg = upd.get("message") or {}
                if str((msg.get("chat") or {}).get("id", "")) != str(TELEGRAM_CHAT_ID):
                    continue
                if msg.get("text"):
                    await handle_text(msg["text"])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Telegram polling: %s", e)
            await asyncio.sleep(1)


# ============================================================
# HEALTH / STARTUP
# ============================================================

async def health(request):
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "running": bot_running(),
        "mode": copy_mode(),
        "copy_usdc": copy_usdc(),
        "copy_size_mode": copy_size_mode(),
        "copy_scale_pct": copy_scale_pct(),
        "max_copy_usdc": max_copy_usdc(),
        "slippage": copy_slippage(),
        "sell_mode": sell_mode(),
        "wallets": len(watched_wallets),
        "rtds": rt_stats,
        "live_master": LIVE_MASTER_ENABLE,
        "live_client_ready": live_client_ready,
        "live_client_error": live_client_error,
        "db": str(DB_PATH),
    })


async def start_http():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    return runner


async def seed_all_wallet_shadows():
    for addr, info in list(watched_wallets.items()):
        try:
            n = await warm_target_positions(addr)
            log.info("Seeded %s target positions: %d", safe_label(info.get("label"), addr), n)
        except Exception:
            log.exception("Seed target positions failed %s", addr)


async def main():
    global session
    init_db()
    load_runtime_caches()
    load_wallets()
    load_seen_hot()
    # Every process restart is fail-safe STOP. This is deliberately not persisted
    # as START across redeploys; the user must press START after inspecting status.
    set_setting("running", "0")

    connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300, keepalive_timeout=60)
    session = aiohttp.ClientSession(connector=connector)
    runner = await start_http()
    await init_live_client()

    tasks = [
        asyncio.create_task(rtds_loop(), name="rtds"),
        asyncio.create_task(rest_fallback_loop(), name="rest-fallback"),
        asyncio.create_task(telegram_loop(), name="telegram"),
        asyncio.create_task(trade_notice_loop(), name="trade-notices"),
        asyncio.create_task(prewarm_loop(), name="prewarm"),
        asyncio.create_task(seed_all_wallet_shadows(), name="seed-shadows"),
    ]
    log.info(
        "START %s | wallets=%d | mode=%s | live_master=%s | live_wallet=%s | RTDS=%s",
        VERSION, len(watched_wallets), copy_mode(), LIVE_MASTER_ENABLE, live_client_ready, RTDS_URL,
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()
        await close_live_client()
        await runner.cleanup()
        if session is not None:
            await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
