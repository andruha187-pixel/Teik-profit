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

VERSION = "2.3-ultrafast-delivery-recovery"

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
GAMMA_API = "https://gamma-api.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

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
REST_FALLBACK_INTERVAL = max(0.25, float(os.getenv("REST_FALLBACK_INTERVAL", "0.5")))
# A new record is considered NEW by event-key appearance, not by its exchange timestamp.
# Timestamp age only decides whether a late REST discovery is still safe to copy.
REST_MAX_COPY_AGE_SEC = max(1.0, float(os.getenv("REST_MAX_COPY_AGE_SEC", "30")))
REST_AUDIT_LOOKBACK_SEC = max(REST_MAX_COPY_AGE_SEC, float(os.getenv("REST_AUDIT_LOOKBACK_SEC", "300")))
REST_ACTIVITY_ENABLE = os.getenv("REST_ACTIVITY_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
REST_ACTIVITY_INTERVAL = max(0.5, float(os.getenv("REST_ACTIVITY_INTERVAL", "1.0")))
MAX_WALLETS = max(1, min(50, int(os.getenv("MAX_WALLETS", "20"))))
SEEN_CACHE_SIZE = max(1000, int(os.getenv("SEEN_CACHE_SIZE", "20000")))

LIVE_PREWARM_ENABLE = os.getenv("LIVE_PREWARM_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
LIVE_PREWARM_INTERVAL_SEC = max(5.0, float(os.getenv("LIVE_PREWARM_INTERVAL_SEC", "20")))
LIVE_NO_MATCH_RETRIES = max(0, min(2, int(os.getenv("LIVE_NO_MATCH_RETRIES", "1"))))
LIVE_NO_MATCH_RETRY_DELAY_MS = max(0, int(os.getenv("LIVE_NO_MATCH_RETRY_DELAY_MS", "25")))
LIVE_SELL_BALANCE_RETRY_MS = max(100, int(os.getenv("LIVE_SELL_BALANCE_RETRY_MS", "600")))

# Persistent delivery recovery. The first LIVE copy remains the same ultra-fast FAK.
# If that FAK is a deterministic NO_MATCH, partial fill, or AMBIGUOUS result, a
# durable recovery job keeps trying the REMAINING size at the SAME user slippage
# cap. AMBIGUOUS is reconciled against our own wallet trade history before any
# new real order is allowed, to avoid blind duplicate orders.
RECOVERY_ENABLE = os.getenv("RECOVERY_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
RECOVERY_BOOK_POLL_MS = max(25, int(os.getenv("RECOVERY_BOOK_POLL_MS", "50")))
RECOVERY_FAK_RETRY_MS = max(100, int(os.getenv("RECOVERY_FAK_RETRY_MS", "250")))
RECOVERY_AMBIGUOUS_GRACE_MS = max(500, int(os.getenv("RECOVERY_AMBIGUOUS_GRACE_MS", "2500")))
RECOVERY_RECONCILE_INTERVAL_MS = max(100, int(os.getenv("RECOVERY_RECONCILE_INTERVAL_MS", "250")))
RECOVERY_MAX_AGE_SEC = max(30, int(os.getenv("RECOVERY_MAX_AGE_SEC", "900")))
RECOVERY_OWN_TRADES_LIMIT = max(20, min(500, int(os.getenv("RECOVERY_OWN_TRADES_LIMIT", "200"))))

TELEGRAM_NOTIFY_TRADES_DEFAULT = os.getenv("TELEGRAM_NOTIFY_TRADES", "1").strip().lower() in {"1", "true", "yes", "on"}

# BTC 15-minute acceleration. This is NOT pre-copy: no order is posted until a
# watched-wallet trade is actually detected. We only keep current/next BTC15
# outcome tokens, market books, authenticated transport and local signer path warm.
BTC15_FASTLANE_ENABLE = os.getenv("BTC15_FASTLANE_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
BTC15_SLUG_PREFIX = os.getenv("BTC15_SLUG_PREFIX", "btc-updown-15m").strip().lower()
BTC15_DISCOVERY_INTERVAL_SEC = max(0.5, float(os.getenv("BTC15_DISCOVERY_INTERVAL_SEC", "2")))
BTC15_BOOK_MAX_AGE_MS = max(50, int(os.getenv("BTC15_BOOK_MAX_AGE_MS", "1500")))
BTC15_SIGNER_PREWARM_ENABLE = os.getenv("BTC15_SIGNER_PREWARM_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
BTC15_SIGNER_PREWARM_SIZE = max(MIN_ORDER_SHARES, float(os.getenv("BTC15_SIGNER_PREWARM_SIZE", "5")))
BTC15_SIGNER_PREWARM_PRICE = min(0.95, max(0.05, float(os.getenv("BTC15_SIGNER_PREWARM_PRICE", "0.50"))))
BTC15_WS_MAX_AGE_SEC = max(30, int(os.getenv("BTC15_WS_MAX_AGE_SEC", "240")))

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
recovery_asset_locks = defaultdict(asyncio.Lock)
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
    "invalid_messages": 0,
}

feed_stats = {
    "rest_trades_last_ms": 0,
    "rest_trades_polls": 0,
    "rest_trades_errors": 0,
    "rest_activity_last_ms": 0,
    "rest_activity_polls": 0,
    "rest_activity_errors": 0,
    "rest_late_detected": 0,
    "rest_raw_events": 0,
    "rest_unique_events": 0,
}

recovery_stats = {
    "scheduled": 0,
    "active": 0,
    "completed": 0,
    "reconciled": 0,
    "retries": 0,
    "expired": 0,
    "blocked": 0,
    "last_error": "",
}

# Trade-notification queue keeps Telegram completely off the execution hot path
# while preserving FOUND -> RESULT message order.
trade_notice_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)

# BTC15 fast-lane hot state. Nothing here is required for correctness of generic
# copy trading; it only removes discovery/book/signer cold-start work for BTC15.
btc15_markets = {}          # condition_id -> market dict
btc15_asset_meta = {}       # token_id -> market/outcome metadata
btc15_books = {}            # token_id -> {bids,asks,received_ms,tick_size}
btc15_assets = set()
btc15_ws_send_queue: asyncio.Queue = asyncio.Queue()
btc15_prewarm_lock = asyncio.Lock()
btc15_signer_warmed = set()
btc15_signer_warm_ms = {}
btc15_stats = {
    "ws_connected": False, "ws_messages": 0, "ws_reconnects": 0,
    "discovered_markets": 0, "warmed_assets": 0, "fastlane_events": 0,
    "last_error": "", "last_book_ms": 0,
}

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


def parse_jsonish(v):
    if isinstance(v, list):
        return v
    if v is None:
        return []
    try:
        x = json.loads(v)
        return x if isinstance(x, list) else []
    except Exception:
        return []


def level_map(rows):
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            price = sf(row.get("price") if row.get("price") is not None else row.get("price_level"), math.nan)
            size = sf(row.get("size") if row.get("size") is not None else row.get("new_quantity"), 0)
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price, size = sf(row[0], math.nan), sf(row[1], 0)
        else:
            continue
        if not math.isnan(price) and size > 0:
            out[price] = size
    return out


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


def source_timestamp_ms(payload):
    raw = sf((payload or {}).get("timestamp"), 0.0)
    if raw <= 0:
        return 0
    # Data API timestamps are normally seconds; tolerate millisecond feeds too.
    return int(raw if raw > 10_000_000_000 else raw * 1000.0)


def estimated_source_age_ms(payload, detected_ms):
    tms = source_timestamp_ms(payload)
    if not tms:
        return None
    # The public activity timestamp is commonly only second-resolution, so this
    # is deliberately labelled an estimate in Telegram/reporting.
    return max(0, int(detected_ms) - int(tms))


def recovery_deadline_ms(payload, detected_ms):
    slug = str((payload or {}).get("slug") or (payload or {}).get("marketSlug") or "").lower()
    slot = btc15_slot_from_slug(slug) if slug.startswith(BTC15_SLUG_PREFIX + "-") else None
    if slot:
        # Do not keep sending after the 15m market's trading window is over.
        return min(int(detected_ms) + RECOVERY_MAX_AGE_SEC * 1000, (slot + 900) * 1000 - 500)
    return int(detected_ms) + RECOVERY_MAX_AGE_SEC * 1000


def transient_recovery_status(status, error=""):
    st = str(status or "").upper()
    if st in {"REJECTED_NO_MATCH", "AMBIGUOUS", "DELAYED_AMBIGUOUS"}:
        return True
    text = str(error or "").lower()
    return st == "REJECTED" and any(x in text for x in (
        "429", "rate limit", "tempor", "timeout", "timed out", "internal error", "service unavailable", "502", "503", "504"
    ))


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
            source_age_ms_est INTEGER,
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
            build_sign_us REAL,
            detect_to_submit_ms INTEGER,
            detect_to_submit_us REAL,
            fast_lane INTEGER NOT NULL DEFAULT 0,
            fast_book_age_ms INTEGER,
            fast_book_price REAL,
            fast_signer_warm INTEGER NOT NULL DEFAULT 0,
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

        CREATE TABLE IF NOT EXISTS recovery_orders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL,
            wallet TEXT NOT NULL,
            wallet_label TEXT,
            source TEXT NOT NULL,
            asset TEXT NOT NULL,
            side TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            requested_shares REAL NOT NULL,
            accounted_shares REAL NOT NULL DEFAULT 0,
            accounted_gross REAL NOT NULL DEFAULT 0,
            limit_price REAL NOT NULL,
            mode TEXT NOT NULL,
            initial_status TEXT,
            initial_error TEXT,
            state TEXT NOT NULL DEFAULT 'PENDING',
            attempts INTEGER NOT NULL DEFAULT 0,
            ambiguous_since_ms INTEGER,
            detected_ms INTEGER NOT NULL,
            deadline_ms INTEGER NOT NULL,
            next_try_ms INTEGER NOT NULL,
            last_error TEXT,
            created_ms INTEGER NOT NULL,
            updated_ms INTEGER NOT NULL,
            UNIQUE(event_key, wallet, asset, side, mode)
        );
        CREATE INDEX IF NOT EXISTS idx_recovery_due ON recovery_orders(state,next_try_ms);
        """)
        # v1.1 migration for databases created by v1.0.
        existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(copy_orders)").fetchall()}
        for col, ddl in {
            "size_mode": "TEXT",
            "source_amount_usdc": "REAL",
            "scale_pct": "REAL",
            "max_copy_usdc": "REAL",
            "size_capped": "INTEGER NOT NULL DEFAULT 0",
            "build_sign_us": "REAL",
            "detect_to_submit_us": "REAL",
            "fast_lane": "INTEGER NOT NULL DEFAULT 0",
            "fast_book_age_ms": "INTEGER",
            "fast_book_price": "REAL",
            "fast_signer_warm": "INTEGER NOT NULL DEFAULT 0",
            "source_age_ms_est": "INTEGER",
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


def shadow_reset_wallet(wallet):
    """Clear every cached/persisted source-position shadow for one wallet."""
    wallet = normalize_address(wallet)
    if not wallet:
        return
    stale = [key for key in list(shadow_cache) if key[0] == wallet]
    for key in stale:
        shadow_cache.pop(key, None)
    try:
        with db() as conn:
            conn.execute("DELETE FROM target_shadow WHERE wallet=?", (wallet,))
            conn.commit()
    except Exception:
        log.exception("shadow reset failed %s", wallet)


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
        "event_key","wallet","wallet_label","source","detected_ms","target_timestamp","source_age_ms_est","target_tx_hash",
        "condition_id","asset","title","slug","outcome","target_side","target_price","target_size",
        "mode","copy_amount_usdc","size_mode","source_amount_usdc","scale_pct","max_copy_usdc","size_capped",
        "slippage","requested_shares","limit_price","build_sign_ms","build_sign_us",
        "detect_to_submit_ms","detect_to_submit_us","fast_lane","fast_book_age_ms","fast_book_price","fast_signer_warm","api_ms","total_reaction_ms","status","filled_shares","avg_price",
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
# DURABLE LIVE DELIVERY RECOVERY
# ============================================================

def recovery_active_count():
    try:
        with db() as conn:
            r = conn.execute(
                "SELECT COUNT(*) c FROM recovery_orders WHERE state IN ('RETRY','RECONCILE')"
            ).fetchone()
            return si(r["c"]) if r else 0
    except Exception:
        return 0


def recovery_schedule_db(*, key, wallet, info, payload, source, requested, accounted, accounted_gross,
                         limit_price, mode, initial_status, initial_error, detected_ms):
    if not RECOVERY_ENABLE or str(mode).upper() != "LIVE":
        return False
    remaining = max(0.0, sf(requested) - sf(accounted))
    if remaining <= 1e-9:
        return False
    # A sub-minimum remainder cannot be submitted as a new order. Keep it visible
    # in the original partial-fill audit rather than creating an impossible loop.
    if remaining < MIN_ORDER_SHARES - 1e-9:
        return False
    status = str(initial_status or "").upper()
    state = "RECONCILE" if status in {"AMBIGUOUS", "DELAYED_AMBIGUOUS"} else "RETRY"
    now = now_ms()
    deadline = recovery_deadline_ms(payload, detected_ms)
    ambiguous_since = now if state == "RECONCILE" else None
    with db() as conn:
        conn.execute(
            """INSERT INTO recovery_orders(
                event_key,wallet,wallet_label,source,asset,side,payload_json,requested_shares,
                accounted_shares,accounted_gross,limit_price,mode,initial_status,initial_error,state,
                attempts,ambiguous_since_ms,detected_ms,deadline_ms,next_try_ms,last_error,created_ms,updated_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(event_key,wallet,asset,side,mode) DO UPDATE SET
                requested_shares=MAX(recovery_orders.requested_shares,excluded.requested_shares),
                accounted_shares=MAX(recovery_orders.accounted_shares,excluded.accounted_shares),
                accounted_gross=MAX(recovery_orders.accounted_gross,excluded.accounted_gross),
                limit_price=excluded.limit_price,initial_status=excluded.initial_status,
                initial_error=excluded.initial_error,state=excluded.state,
                ambiguous_since_ms=excluded.ambiguous_since_ms,deadline_ms=excluded.deadline_ms,
                next_try_ms=excluded.next_try_ms,last_error=excluded.last_error,updated_ms=excluded.updated_ms
            """,
            (
                key, wallet, safe_label(info.get("label"), wallet), source,
                str(payload.get("asset") or payload.get("asset_id") or ""),
                str(payload.get("side") or "").upper(), jd(payload), sf(requested), sf(accounted),
                sf(accounted_gross), sf(limit_price), str(mode).upper(), status, str(initial_error or ""),
                state, 0, ambiguous_since, int(detected_ms), int(deadline), now, str(initial_error or ""), now, now,
            ),
        )
        conn.commit()
    recovery_stats["scheduled"] += 1
    recovery_stats["active"] = recovery_active_count()
    return True


def recovery_due_rows(limit=20):
    with db() as conn:
        rows = conn.execute(
            """SELECT * FROM recovery_orders
               WHERE state IN ('RETRY','RECONCILE') AND next_try_ms<=?
               ORDER BY next_try_ms ASC LIMIT ?""",
            (now_ms(), int(limit)),
        ).fetchall()
        return [dict(r) for r in rows]


def recovery_update(job_id, **fields):
    allowed = {
        "accounted_shares", "accounted_gross", "state", "attempts", "ambiguous_since_ms",
        "deadline_ms", "next_try_ms", "last_error", "updated_ms", "initial_status", "initial_error"
    }
    items = [(k, v) for k, v in fields.items() if k in allowed]
    if not items:
        return
    if "updated_ms" not in dict(items):
        items.append(("updated_ms", now_ms()))
    sql = "UPDATE recovery_orders SET " + ",".join(f"{k}=?" for k, _ in items) + " WHERE id=?"
    with db() as conn:
        conn.execute(sql, [v for _, v in items] + [int(job_id)])
        conn.commit()


def recovery_payload(job):
    try:
        x = json.loads(job.get("payload_json") or "{}")
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def own_reconcile_wallet():
    # AsyncSecureClient resolves the actual trading wallet/proxy. Prefer it over
    # the raw env value because the env may be a signer rather than proxy wallet.
    w = normalize_address(getattr(live_client, "wallet", "") if live_client is not None else "")
    return w or normalize_address(POLYMARKET_WALLET_ADDRESS)


async def own_recent_matching_fills(job):
    """Return cumulative own-wallet fills that can belong to this recovery job.

    This is used only after an ambiguous submission. It never sits in front of the
    first fast FAK. Data API timestamps are second-resolution, so we use a narrow
    time/asset/side/price window and dedupe transaction-shaped rows.
    """
    wallet = own_reconcile_wallet()
    if not wallet:
        return None
    data = await get_json(
        f"{DATA_API}/trades",
        params={"user": wallet, "limit": RECOVERY_OWN_TRADES_LIMIT, "takerOnly": "false"},
        timeout=4,
    )
    if not isinstance(data, list):
        return None
    asset = str(job.get("asset") or "")
    side = str(job.get("side") or "").upper()
    limit_price = sf(job.get("limit_price"))
    start_ms = int(job.get("detected_ms") or 0) - 1500
    end_ms = now_ms() + 1500
    seen = set()
    total_shares = 0.0
    total_gross = 0.0
    matches = 0
    for t in data:
        if not isinstance(t, dict):
            continue
        if str(t.get("asset") or t.get("asset_id") or "") != asset:
            continue
        if str(t.get("side") or "").upper() != side:
            continue
        tsms = source_timestamp_ms(t)
        if tsms and not (start_ms <= tsms <= end_ms):
            continue
        price = sf(t.get("price"), -1)
        size = sf(t.get("size"), 0)
        if size <= 0 or price <= 0:
            continue
        if side == "BUY" and price > limit_price + max(0.001, LIVE_PRICE_TICK_FALLBACK) + 1e-9:
            continue
        if side == "SELL" and price < limit_price - max(0.001, LIVE_PRICE_TICK_FALLBACK) - 1e-9:
            continue
        tx = str(t.get("transactionHash") or t.get("transaction_hash") or "").lower()
        dkey = (tx, str(t.get("asset") or t.get("asset_id") or ""), side, round(price, 8), round(size, 8), si(t.get("timestamp"), 0))
        if dkey in seen:
            continue
        seen.add(dkey)
        total_shares += size
        total_gross += size * price
        matches += 1
    return {"shares": total_shares, "gross": total_gross, "matches": matches}


def recovery_book_ready(job):
    """For BTC15, only spend a CLOB POST when the warmed book is marketable.

    Generic markets return True because this bot does not keep all their books
    subscribed. The user slippage cap is still enforced by the FAK itself.
    """
    asset = str(job.get("asset") or "")
    book = btc15_books.get(asset)
    if not book:
        return True
    age = now_ms() - si(book.get("received_ms"), 0) if book.get("received_ms") else None
    if age is None or age > BTC15_BOOK_MAX_AGE_MS:
        return True
    side = str(job.get("side") or "").upper()
    limit_price = sf(job.get("limit_price"))
    levels = book.get("asks") if side == "BUY" else book.get("bids")
    if not levels:
        return False
    if side == "BUY":
        return any(sf(p) <= limit_price + 1e-12 and sf(q) > 0 for p, q in levels.items())
    return any(sf(p) >= limit_price - 1e-12 and sf(q) > 0 for p, q in levels.items())


def recovery_finish(job, state, message, *, accounted=None, gross=None, last_error=""):
    recovery_update(
        job["id"], state=state,
        accounted_shares=sf(job.get("accounted_shares")) if accounted is None else sf(accounted),
        accounted_gross=sf(job.get("accounted_gross")) if gross is None else sf(gross),
        next_try_ms=2**62, last_error=last_error or message,
    )
    if state == "DONE":
        recovery_stats["completed"] += 1
    elif state == "EXPIRED":
        recovery_stats["expired"] += 1
    else:
        recovery_stats["blocked"] += 1
    recovery_stats["active"] = recovery_active_count()
    queue_trade_notice(message)


async def recovery_apply_fill(job, payload, filled, avg, gross):
    if filled <= 1e-12:
        return
    side = str(job.get("side") or "").upper()
    if side == "BUY":
        position_apply_buy(job["wallet"], job["asset"], "LIVE", payload, filled, avg, gross)
    else:
        position_apply_sell(job["wallet"], job["asset"], "LIVE", filled, gross)


def record_recovery_execution(job, payload, *, status, filled=0.0, avg=None, gross=0.0, error="", result=None, source_suffix="retry"):
    result = result or {}
    detected = now_ms()
    row = base_order_row(
        str(job.get("event_key") or "") + f":recovery:{si(job.get('attempts'),0)+1}:{detected}",
        job.get("wallet"), {"label": job.get("wallet_label")}, payload,
        f"recovery:{source_suffix}", detected, "LIVE", 0.0, copy_slippage(),
    )
    row.update({
        "requested_shares": max(0.0, sf(job.get("requested_shares")) - sf(job.get("accounted_shares"))),
        "limit_price": sf(job.get("limit_price")),
        "build_sign_ms": result.get("build_sign_ms"),
        "build_sign_us": result.get("build_sign_us"),
        "detect_to_submit_ms": result.get("detect_to_submit_ms"),
        "detect_to_submit_us": result.get("detect_to_submit_us"),
        "fast_lane": 1 if btc15_fastlane_snapshot(payload).get("fast_lane") else 0,
        "fast_book_age_ms": btc15_fastlane_snapshot(payload).get("book_age_ms"),
        "fast_book_price": btc15_fastlane_snapshot(payload).get("book_price"),
        "fast_signer_warm": 1 if btc15_fastlane_snapshot(payload).get("signer_warm") else 0,
        "api_ms": result.get("api_ms"),
        "total_reaction_ms": 0,
        "status": str(status),
        "filled_shares": sf(filled),
        "avg_price": sf(avg) if avg is not None and sf(filled)>0 else None,
        "gross_amount": sf(gross),
        "fee_estimate": sf(result.get("fee")),
        "order_id": str(result.get("order_id") or ""),
        "response_json": str(result.get("response_json") or "{}"),
        "error": str(error or ""),
        "created_ms": now_ms(),
    })
    record_order(row)


async def process_recovery_job(job):
    now = now_ms()
    payload = recovery_payload(job)
    label = job.get("wallet_label") or short_addr(job.get("wallet"))
    requested = sf(job.get("requested_shares"))
    accounted = sf(job.get("accounted_shares"))
    accounted_gross = sf(job.get("accounted_gross"))
    remaining = max(0.0, requested - accounted)

    if remaining <= 1e-8:
        recovery_finish(
            job, "DONE",
            f"✅ RECOVERY ЗАВЕРШЁН\n👛 {label}\nИсполнено {accounted:.4f}/{requested:.4f}sh.",
            accounted=accounted, gross=accounted_gross,
        )
        return
    if now >= si(job.get("deadline_ms"), 0):
        recovery_finish(
            job, "EXPIRED",
            f"⏱ RECOVERY ЗАВЕРШИЛСЯ ПО ВРЕМЕНИ\n👛 {label}\n"
            f"Осталось {remaining:.4f}sh по limit {sf(job.get('limit_price')):.4f}. "
            "Цена/ликвидность так и не вернулись в разрешённый slippage до конца окна.",
            accounted=accounted, gross=accounted_gross, last_error="deadline",
        )
        return
    # STOP remains an absolute user control. A durable job survives STOP/redeploy
    # and resumes only after the user explicitly STARTs again.
    if not bot_running() or copy_mode() != "LIVE" or not LIVE_MASTER_ENABLE or not live_client_ready:
        recovery_update(job["id"], next_try_ms=now + 500)
        return

    state = str(job.get("state") or "RETRY").upper()
    if state == "RECONCILE":
        rec = await own_recent_matching_fills(job)
        if rec is not None:
            total_seen = min(requested, max(0.0, sf(rec.get("shares"))))
            total_gross = max(0.0, sf(rec.get("gross")))
            if total_seen > accounted + 1e-8:
                delta = min(remaining, total_seen - accounted)
                # Approximate the newly discovered fill at cumulative weighted avg.
                avg = (total_gross / total_seen) if total_seen > 1e-12 else sf(job.get("limit_price"))
                delta_gross = delta * avg
                await recovery_apply_fill(job, payload, delta, avg, delta_gross)
                await asyncio.to_thread(
                    record_recovery_execution, job, payload, status="RECOVERED_AMBIGUOUS",
                    filled=delta, avg=avg, gross=delta_gross, error="reconciled from own wallet trades", source_suffix="reconcile",
                )
                accounted += delta
                accounted_gross += delta_gross
                remaining = max(0.0, requested - accounted)
                recovery_stats["reconciled"] += 1
                queue_trade_notice(
                    f"🔎 AMBIGUOUS СВЕРЕН\n👛 {label}\n"
                    f"Нашёл подтверждённый fill +{delta:.4f}sh через собственную историю кошелька. "
                    f"Итого {accounted:.4f}/{requested:.4f}sh."
                )
                if remaining <= 1e-8:
                    recovery_finish(
                        job, "DONE",
                        f"✅ RECOVERY ЗАВЕРШЁН ПОСЛЕ СВЕРКИ\n👛 {label}\n"
                        f"Исполнено {accounted:.4f}/{requested:.4f}sh без слепого дубля.",
                        accounted=accounted, gross=accounted_gross,
                    )
                    return
                recovery_update(job["id"], accounted_shares=accounted, accounted_gross=accounted_gross)

        ambiguous_since = si(job.get("ambiguous_since_ms"), 0) or now
        if now - ambiguous_since < RECOVERY_AMBIGUOUS_GRACE_MS:
            recovery_update(
                job["id"], accounted_shares=accounted, accounted_gross=accounted_gross,
                next_try_ms=now + RECOVERY_RECONCILE_INTERVAL_MS,
            )
            return
        # No additional fill became visible during the reconcile grace window.
        # Only now may we create a fresh order for the remaining size.
        recovery_update(
            job["id"], state="RETRY", accounted_shares=accounted, accounted_gross=accounted_gross,
            next_try_ms=now, last_error="ambiguous_reconcile_grace_complete",
        )
        job = {**job, "state": "RETRY", "accounted_shares": accounted, "accounted_gross": accounted_gross}
        state = "RETRY"

    if remaining < MIN_ORDER_SHARES - 1e-9:
        recovery_finish(
            job, "BLOCKED",
            f"⚠️ RECOVERY ОСТАНОВЛЕН\n👛 {label}\n"
            f"Остаток {remaining:.4f}sh меньше минимального ордера {MIN_ORDER_SHARES:g}sh.",
            accounted=accounted, gross=accounted_gross, last_error="remaining_below_min_order",
        )
        return

    if not recovery_book_ready(job):
        # BTC15 hot book says our limit is not marketable yet. Wait in memory
        # instead of hammering the CLOB with guaranteed NO_MATCH FAKs.
        recovery_update(job["id"], next_try_ms=now + RECOVERY_BOOK_POLL_MS)
        return

    async with recovery_asset_locks[str(job.get("asset") or "")]:
        result = await submit_live_fak(
            str(job.get("asset") or ""), str(job.get("side") or ""), remaining,
            sf(job.get("limit_price")), now, None,
        )
    attempts = si(job.get("attempts"), 0) + 1
    recovery_stats["retries"] += 1
    filled = max(0.0, sf(result.get("filled")))
    avg = sf(result.get("avg"))
    gross = sf(result.get("gross"))
    await asyncio.to_thread(
        record_recovery_execution, job, payload, status=str(result.get("status") or "RECOVERY_RETRY"),
        filled=filled, avg=avg if filled > 0 else None, gross=gross, error=str(result.get("error") or ""),
        result=result, source_suffix="retry",
    )
    if filled > 1e-12:
        await recovery_apply_fill(job, payload, filled, avg, gross)
        accounted += filled
        accounted_gross += gross
        remaining = max(0.0, requested - accounted)
        queue_trade_notice(
            f"🔁 RECOVERY FILL\n👛 {label}\n"
            f"+{filled:.4f}sh @ {avg:.4f}; итого {accounted:.4f}/{requested:.4f}sh. "
            f"Попытка recovery #{attempts}."
        )
        if remaining <= 1e-8:
            recovery_finish(
                job, "DONE",
                f"✅ ПОЗИЦИЯ ДОИСПОЛНЕНА RECOVERY\n👛 {label}\n"
                f"Итого {accounted:.4f}/{requested:.4f}sh по исходному slippage-limit.",
                accounted=accounted, gross=accounted_gross,
            )
            return

    status = str(result.get("status") or "").upper()
    err = str(result.get("error") or "")
    if status in {"AMBIGUOUS", "DELAYED_AMBIGUOUS"}:
        recovery_update(
            job["id"], state="RECONCILE", attempts=attempts, accounted_shares=accounted,
            accounted_gross=accounted_gross, ambiguous_since_ms=now_ms(),
            next_try_ms=now_ms() + RECOVERY_RECONCILE_INTERVAL_MS, last_error=err,
        )
        return
    if status == "REJECTED_NO_MATCH" or (filled > 1e-12 and remaining >= MIN_ORDER_SHARES - 1e-9) or transient_recovery_status(status, err):
        recovery_update(
            job["id"], state="RETRY", attempts=attempts, accounted_shares=accounted,
            accounted_gross=accounted_gross, next_try_ms=now_ms() + RECOVERY_FAK_RETRY_MS, last_error=err,
        )
        return

    recovery_finish(
        job, "BLOCKED",
        f"⛔ RECOVERY ЗАБЛОКИРОВАН\n👛 {label}\n"
        f"Осталось {remaining:.4f}sh. Причина: {human_copy_reason(status, err)}\ntech: {err[:500]}",
        accounted=accounted, gross=accounted_gross, last_error=err or status,
    )


def backfill_recent_recovery_jobs():
    """Create recovery jobs for still-live v2.2-era copy rows after an upgrade.

    This is intentionally conservative: only original LIVE rows with a recoverable
    result and a still-open recovery window are considered, and rows that already
    have a recovery job are skipped.  The process still starts STOP, so backfilled
    jobs cannot submit until the user explicitly presses START.
    """
    if not RECOVERY_ENABLE:
        return 0
    cutoff = now_ms() - RECOVERY_MAX_AGE_SEC * 1000
    with db() as conn:
        rows = conn.execute(
            """SELECT o.* FROM copy_orders o
               WHERE o.mode='LIVE'
                 AND o.created_ms>=?
                 AND o.event_key NOT LIKE '%:recovery:%'
                 AND (
                      UPPER(COALESCE(o.status,'')) IN ('AMBIGUOUS','DELAYED_AMBIGUOUS','REJECTED_NO_MATCH')
                      OR (COALESCE(o.filled_shares,0)>0 AND COALESCE(o.filled_shares,0)+1e-9<COALESCE(o.requested_shares,0))
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM recovery_orders r
                     WHERE r.event_key=o.event_key AND r.wallet=o.wallet AND r.asset=o.asset
                       AND r.side=o.target_side AND r.mode=o.mode
                 )
               ORDER BY o.created_ms ASC""",
            (cutoff,),
        ).fetchall()
    made = 0
    for rr in rows:
        r = dict(rr)
        payload = {
            'proxyWallet': r.get('wallet'),
            'transactionHash': r.get('target_tx_hash') or '',
            'asset': r.get('asset') or '',
            'conditionId': r.get('condition_id') or '',
            'side': r.get('target_side') or '',
            'price': sf(r.get('target_price')),
            'size': sf(r.get('target_size')),
            'timestamp': si(r.get('target_timestamp'), 0),
            'title': r.get('title') or '',
            'slug': r.get('slug') or '',
            'outcome': r.get('outcome') or '',
        }
        detected = si(r.get('detected_ms'), 0) or si(r.get('created_ms'), now_ms())
        if recovery_deadline_ms(payload, detected) <= now_ms():
            continue
        if recovery_schedule_db(
            key=str(r.get('event_key') or ''), wallet=str(r.get('wallet') or ''),
            info={'label': r.get('wallet_label') or ''}, payload=payload, source=str(r.get('source') or 'upgrade-backfill'),
            requested=sf(r.get('requested_shares')), accounted=sf(r.get('filled_shares')),
            accounted_gross=sf(r.get('gross_amount')), limit_price=sf(r.get('limit_price')),
            mode='LIVE', initial_status=str(r.get('status') or ''), initial_error=str(r.get('error') or ''),
            detected_ms=detected,
        ):
            made += 1
    if made:
        log.warning('Backfilled %d still-live recovery job(s) from pre-v2.3 copy audit', made)
    return made


async def recovery_loop():
    while True:
        try:
            if not RECOVERY_ENABLE:
                await asyncio.sleep(1.0)
                continue
            rows = await asyncio.to_thread(recovery_due_rows, 20)
            recovery_stats["active"] = recovery_active_count()
            if not rows:
                await asyncio.sleep(RECOVERY_BOOK_POLL_MS / 1000.0)
                continue
            for job in rows:
                try:
                    await process_recovery_job(job)
                except Exception as e:
                    recovery_stats["last_error"] = f"{type(e).__name__}: {e}"
                    log.exception("Recovery job failed id=%s", job.get("id"))
                    recovery_update(job["id"], next_try_ms=now_ms() + 500, last_error=recovery_stats["last_error"])
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            recovery_stats["last_error"] = f"{type(e).__name__}: {e}"
            log.exception("Recovery loop")
            await asyncio.sleep(0.5)


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


async def resolve_proxy_wallet(address):
    """Resolve either a profile/user address or proxy address to canonical proxyWallet."""
    addr = normalize_address(address)
    if not addr:
        return "", ""
    data = await get_json(f"{GAMMA_API}/public-profile", params={"address": addr}, timeout=4)
    if isinstance(data, dict):
        proxy = normalize_address(data.get("proxyWallet"))
        name = str(data.get("name") or data.get("pseudonym") or "").strip()
        if proxy:
            return proxy, name[:40]
    return addr, ""


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


async def submit_live_fak(asset, side, shares, limit_price, detected_ms, detected_perf_ns=None):
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
    build_start_ns = time.perf_counter_ns()
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
            "build_sign_ms": now_ms() - build_start, "build_sign_us": (time.perf_counter_ns() - build_start_ns) / 1000.0,
            "detect_to_submit_ms": None, "api_ms": None,
        }

    build_end = now_ms()
    build_sign_us = (time.perf_counter_ns() - build_start_ns) / 1000.0
    attempts = 0
    last = None
    while attempts <= LIVE_NO_MATCH_RETRIES:
        if attempts and LIVE_NO_MATCH_RETRY_DELAY_MS:
            await asyncio.sleep(LIVE_NO_MATCH_RETRY_DELAY_MS / 1000.0)
            # New nonce/order hash for deterministic FAK retry.
            build_start_retry = now_ms()
            build_start_retry_ns = time.perf_counter_ns()
            try:
                signed = await live_client.create_limit_order(
                    token_id=str(asset), price=limit_str, size=size_str, side=side, post_only=False,
                )
                fak_order = replace(signed, order_type="FAK", post_only=False)
                build_end = now_ms()
                build_start = build_start_retry
                build_sign_us = (time.perf_counter_ns() - build_start_retry_ns) / 1000.0
            except Exception as e:
                return {
                    "ok": False, "status": "REJECTED_LOCAL", "filled": 0.0,
                    "error": f"{type(e).__name__}: {e}",
                    "build_sign_ms": now_ms() - build_start_retry,
                    "build_sign_us": (time.perf_counter_ns() - build_start_retry_ns) / 1000.0,
                    "detect_to_submit_ms": None, "detect_to_submit_us": None, "api_ms": None,
                }

        submit_ms = now_ms()
        submit_ns = time.perf_counter_ns()
        detect_to_submit_us = ((submit_ns - detected_perf_ns) / 1000.0) if detected_perf_ns else float(max(0, submit_ms - detected_ms) * 1000)
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
                        "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                        "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
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
                        "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                        "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
                        "api_ms": response_ms - submit_ms,
                        "response_json": response_json(response),
                    }
                return {
                    "ok": False, "status": "REJECTED", "filled": 0.0, "error": error,
                    "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                    "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
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
                "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
                "api_ms": response_ms - submit_ms,
            }
        except Exception as e:
            response_ms = now_ms()
            error = f"{type(e).__name__}: {e}"
            if is_definite_fak_no_match_error(e):
                last = {
                    "ok": False, "status": "REJECTED_NO_MATCH", "filled": 0.0, "error": error,
                    "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                    "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
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
                    "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                    "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
                    "api_ms": response_ms - submit_ms,
                    "response_json": "{}",
                }
            # Fail closed: after POST starts, an unknown transport/API exception can
            # mean the order was accepted. Never duplicate it automatically.
            return {
                "ok": False, "status": "AMBIGUOUS", "filled": 0.0, "error": error,
                "build_sign_ms": build_end - build_start, "build_sign_us": build_sign_us,
                "detect_to_submit_ms": submit_ms - detected_ms, "detect_to_submit_us": detect_to_submit_us,
                "api_ms": response_ms - submit_ms,
                "response_json": "{}",
            }

    return last or {"ok": False, "status": "REJECTED_NO_MATCH", "filled": 0.0, "error": "no_match"}



# ============================================================
# BTC 15M PREWARMED FAST LANE (NO PRE-COPY)
# ============================================================

def btc15_slot_start(ts=None):
    t = int(time.time() if ts is None else ts)
    return (t // 900) * 900


def btc15_slot_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None


async def btc15_fetch_event(slug):
    for url, params in (
        (f"{GAMMA_API}/events/slug/{slug}", None),
        (f"{GAMMA_API}/events", {"slug": slug}),
    ):
        data = await get_json(url, params=params, timeout=5)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    return None


def btc15_parse_market(raw, event):
    if not isinstance(raw, dict):
        return None
    slug = str(raw.get("slug") or event.get("slug") or "").lower()
    if not slug.startswith(BTC15_SLUG_PREFIX + "-"):
        return None
    cid = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid:
        return None
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    if len(tokens) < 2:
        return None
    up = down = None
    for i, o in enumerate(outcomes):
        if i >= len(tokens):
            break
        if o in {"UP", "YES"}:
            up = tokens[i]
        elif o in {"DOWN", "NO"}:
            down = tokens[i]
    up = up or tokens[0]
    down = down or tokens[1]
    start = btc15_slot_from_slug(slug)
    if not start:
        return None
    return {
        "condition_id": cid,
        "slug": slug,
        "question": str(raw.get("question") or raw.get("title") or event.get("title") or "BTC 15m"),
        "start_ts": int(start), "end_ts": int(start) + 900,
        "up_asset": str(up), "down_asset": str(down),
    }


async def btc15_discover_slot(slot_start):
    slug = f"{BTC15_SLUG_PREFIX}-{int(slot_start)}"
    event = await btc15_fetch_event(slug)
    if not event or not isinstance(event.get("markets"), list):
        return None
    for raw in event["markets"]:
        m = btc15_parse_market(raw, event)
        if m:
            return m
    return None


def btc15_best(asset, side):
    b = btc15_books.get(str(asset)) or {}
    levels = b.get("asks") if str(side).upper() == "BUY" else b.get("bids")
    if not levels:
        return None
    return min(levels) if str(side).upper() == "BUY" else max(levels)


def btc15_book_age(asset):
    b = btc15_books.get(str(asset)) or {}
    recv = si(b.get("received_ms"), 0)
    return now_ms() - recv if recv else None


def btc15_apply_book(asset, payload):
    asset = str(asset or "")
    if not asset:
        return
    prior = btc15_books.get(asset) or {}
    tick = sf(payload.get("tick_size") if isinstance(payload, dict) else None, sf(prior.get("tick_size"), LIVE_PRICE_TICK_FALLBACK))
    if tick <= 0:
        tick = LIVE_PRICE_TICK_FALLBACK
    btc15_books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(), "tick_size": tick,
    }
    btc15_stats["last_book_ms"] = now_ms()


def btc15_apply_price_change(payload):
    changes = payload.get("price_changes") or payload.get("priceChanges") or []
    recv = now_ms()
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset or asset not in btc15_assets:
            continue
        b = btc15_books.setdefault(asset, {"bids": {}, "asks": {}, "received_ms": recv, "tick_size": LIVE_PRICE_TICK_FALLBACK})
        price = sf(ch.get("price"), math.nan)
        size = sf(ch.get("size"), 0)
        side = str(ch.get("side") or "").upper()
        if math.isnan(price):
            continue
        levels = b["bids"] if side == "BUY" else b["asks"]
        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        b["received_ms"] = recv
        btc15_stats["last_book_ms"] = recv


async def btc15_signer_prewarm(market, asset, outcome):
    asset = str(asset or "")
    if not BTC15_SIGNER_PREWARM_ENABLE or not asset or asset in btc15_signer_warmed:
        return asset in btc15_signer_warmed
    if not live_client_ready or live_client is None:
        return False
    # Opportunistic local-only warming; never post an order and never wait for it
    # in the real copy path. Skip while any copy action is already using its lock.
    if any(lock.locked() for lock in list(asset_locks.values())):
        return False
    async with btc15_prewarm_lock:
        if asset in btc15_signer_warmed:
            return True
        started = time.perf_counter_ns()
        try:
            price = format(normalize_limit(BTC15_SIGNER_PREWARM_PRICE, "BUY"), "f")
            size = format(Decimal(str(round(BTC15_SIGNER_PREWARM_SIZE, 4))), "f")
            signed = await live_client.create_limit_order(token_id=asset, price=price, size=size, side="BUY", post_only=False)
            _ = replace(signed, order_type="FAK", post_only=False)
            elapsed = (time.perf_counter_ns() - started) / 1_000_000.0
            btc15_signer_warmed.add(asset)
            btc15_signer_warm_ms[asset] = elapsed
            btc15_stats["warmed_assets"] = len(btc15_signer_warmed)
            log.info("BTC15 SIGNER WARM %s %s | token=%s | %.2fms | LOCAL ONLY", market.get("slug"), outcome, asset[-10:], elapsed)
            return True
        except Exception as e:
            log.warning("BTC15 signer warm failed %s %s: %s", market.get("slug"), outcome, e)
            return False


async def btc15_register_market(market):
    cid = str(market.get("condition_id") or "")
    if not cid:
        return
    is_new = cid not in btc15_markets
    btc15_markets[cid] = market
    for outcome, asset in (("Up", market.get("up_asset")), ("Down", market.get("down_asset"))):
        asset = str(asset or "")
        if not asset:
            continue
        btc15_asset_meta[asset] = {**market, "outcome": outcome}
        if asset not in btc15_assets:
            btc15_assets.add(asset)
            await btc15_ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})
        if BTC15_SIGNER_PREWARM_ENABLE:
            asyncio.create_task(btc15_signer_prewarm(market, asset, outcome))
    if is_new:
        btc15_stats["discovered_markets"] = len(btc15_markets)
        log.info("BTC15 MARKET READY %s | UP=%s DOWN=%s", market.get("slug"), str(market.get("up_asset"))[-10:], str(market.get("down_asset"))[-10:])


async def btc15_discovery_loop():
    while True:
        try:
            if not BTC15_FASTLANE_ENABLE:
                await asyncio.sleep(2)
                continue
            cur = btc15_slot_start()
            # Current + next are the important warm pair. Previous is retained for
            # a few seconds around rollover so a late source event still maps cleanly.
            known_slots = {si(m.get("start_ts")) for m in btc15_markets.values()}
            for slot in (cur, cur + 900, cur - 900):
                if slot in known_slots:
                    continue
                m = await btc15_discover_slot(slot)
                if m:
                    await btc15_register_market(m)
                    known_slots.add(slot)
            cutoff = time.time() - 930
            stale_cids = [cid for cid, m in btc15_markets.items() if sf(m.get("end_ts")) < cutoff]
            for cid in stale_cids:
                btc15_markets.pop(cid, None)
            keep_assets = set()
            for m in btc15_markets.values():
                keep_assets.update({str(m.get("up_asset") or ""), str(m.get("down_asset") or "")})
            keep_assets.discard("")
            stale_assets = set(btc15_assets) - keep_assets
            for asset in stale_assets:
                btc15_assets.discard(asset)
                btc15_asset_meta.pop(asset, None)
                btc15_books.pop(asset, None)
                btc15_signer_warmed.discard(asset)
                btc15_signer_warm_ms.pop(asset, None)
                await btc15_ws_send_queue.put({"operation": "unsubscribe", "assets_ids": [asset]})
            btc15_stats["warmed_assets"] = len(btc15_signer_warmed)
            await asyncio.sleep(BTC15_DISCOVERY_INTERVAL_SEC)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            btc15_stats["last_error"] = f"discovery:{type(e).__name__}:{e}"
            log.warning("BTC15 discovery: %s", e)
            await asyncio.sleep(1)


async def btc15_ws_sender(ws):
    while True:
        msg = await btc15_ws_send_queue.get()
        try:
            await ws.send_str(jd(msg))
        except Exception:
            await btc15_ws_send_queue.put(msg)
            return
        finally:
            btc15_ws_send_queue.task_done()


async def btc15_ws_ping(ws):
    while True:
        await asyncio.sleep(10)
        try:
            await ws.send_str("PING")
        except Exception:
            return


async def btc15_market_ws_loop():
    while True:
        try:
            if not BTC15_FASTLANE_ENABLE or not btc15_assets:
                await asyncio.sleep(0.5)
                continue
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
            async with session.ws_connect(MARKET_WS, heartbeat=None, timeout=timeout, max_msg_size=20_000_000) as ws:
                btc15_stats["ws_connected"] = True
                btc15_stats["ws_reconnects"] += 1
                btc15_stats["last_error"] = ""
                await ws.send_str(jd({"assets_ids": list(btc15_assets), "type": "market", "custom_feature_enabled": True}))
                sender = asyncio.create_task(btc15_ws_sender(ws))
                ping = asyncio.create_task(btc15_ws_ping(ws))
                started = time.monotonic()
                log.info("BTC15 BOOK WS connected | assets=%d", len(btc15_assets))
                try:
                    async for msg in ws:
                        if time.monotonic() - started >= BTC15_WS_MAX_AGE_SEC:
                            break
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            if msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                                break
                            continue
                        btc15_stats["ws_messages"] += 1
                        for ev in iter_ws_messages(msg.data):
                            if not isinstance(ev, dict):
                                continue
                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                            if et == "book":
                                asset = str(payload.get("asset_id") or payload.get("token_id") or "")
                                if asset in btc15_assets:
                                    btc15_apply_book(asset, payload)
                            elif et == "price_change":
                                btc15_apply_price_change(payload)
                finally:
                    sender.cancel(); ping.cancel()
                    btc15_stats["ws_connected"] = False
        except asyncio.CancelledError:
            raise
        except Exception as e:
            btc15_stats["ws_connected"] = False
            btc15_stats["last_error"] = f"ws:{type(e).__name__}:{e}"
            log.warning("BTC15 book WS reconnect: %s", e)
            await asyncio.sleep(0.5)


def btc15_fastlane_snapshot(payload):
    if not BTC15_FASTLANE_ENABLE:
        return {"fast_lane": False}
    asset = str(payload.get("asset") or payload.get("asset_id") or "")
    slug = str(payload.get("slug") or payload.get("marketSlug") or "").lower()
    meta = btc15_asset_meta.get(asset)
    eligible = bool(meta) or slug.startswith(BTC15_SLUG_PREFIX + "-")
    if not eligible:
        return {"fast_lane": False}
    side = str(payload.get("side") or "BUY").upper()
    age = btc15_book_age(asset)
    best = btc15_best(asset, side)
    warm = asset in btc15_signer_warmed
    return {
        "fast_lane": True, "book_age_ms": age, "book_price": best,
        "book_fresh": age is not None and age <= BTC15_BOOK_MAX_AGE_MS,
        "signer_warm": warm, "slug": str((meta or {}).get("slug") or slug),
    }


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
    age_est = estimated_source_age_ms(payload, now_ms())
    age_line = f"\n⏱ source→detect ≈{age_est}ms*" if age_est is not None else ""
    fallback = "\n⚠️ Найдено через REST fallback — RTDS не был первым источником этой сделки." if str(source).startswith("rest:") else ""
    return (
        f"{head}\n"
        f"👛 {label} ({short_addr(wallet)})\n"
        f"{title}\n{outcome}\n"
        f"Источник: {side} {size:.4f}sh @ {price:.4f} ≈ ${usdc:.2f}\n"
        f"feed: {source}"
        + (f" | tx {tx}" if tx else "")
        + age_line
        + ("\n* публичный timestamp обычно имеет точность до 1 секунды." if age_est is not None else "")
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
        return "Ответ после отправки неоднозначен; recovery сначала сверяет собственные fills, и только потом разрешает новый ордер на остаток."
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
        if er.startswith("REST_LATE:"):
            return f"Сделка найдена резервным REST слишком поздно для безопасного копирования ({er.split(':',1)[1]}). Событие записано, но ордер не отправлен."
        if er.startswith("ORDER_TOO_SMALL"):
            return f"Расчётный размер меньше минимального ордера: {er.split(':',1)[-1]}."
        return er or "Сделка пропущена правилами бота."
    return er or st or "Причина не определена."


# ============================================================
# WALLET SHADOW / COPY ENGINE
# ============================================================

async def warm_target_positions(wallet):
    """Replace the target-wallet shadow with CURRENT positive inventory only.

    Existing target positions are never copied.  The shadow is used solely as
    the denominator for proportional future SELL copying.  A successful refresh
    is authoritative: stale assets from an earlier run are cleared first, and
    zero-size/redeemable rows are not counted as open inventory.
    """
    data = await get_json(
        f"{DATA_API}/positions",
        params={
            "user": wallet,
            "limit": 500,
            "sizeThreshold": 0,
            "redeemable": "false",
        },
        timeout=10,
    )
    if not isinstance(data, list):
        return 0

    # Only clear the previous shadow AFTER a successful API response.  This
    # prevents a transient API failure from erasing useful state.
    shadow_reset_wallet(wallet)

    count = 0
    for p in data:
        asset = str(p.get("asset") or "")
        sh = max(0.0, sf(p.get("size")))
        if not asset or sh <= 1e-6 or bool(p.get("redeemable")):
            continue
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


async def ingest_trade(payload, source="rtds", force_skip_reason=None):
    detected_perf_ns = time.perf_counter_ns()
    if not valid_trade_payload(payload):
        if str(source).startswith("rtds:"):
            rt_stats["invalid_messages"] += 1
        return
    wallet = target_wallet_from_payload(payload)
    info = watched_wallets.get(wallet)
    if not info or not si(info.get("enabled"), 1):
        return

    is_rtds = str(source).startswith("rtds:")
    if is_rtds:
        rt_stats["matched_wallet_raw_events"] += 1
    elif str(source).startswith("rest:"):
        feed_stats["rest_raw_events"] += 1
    key = event_key(payload)
    if not hot_seen_add(key):
        if is_rtds:
            rt_stats["duplicate_events"] += 1
        return

    detected_ms = now_ms()
    if is_rtds:
        rt_stats["matched_wallet_events"] += 1
    elif str(source).startswith("rest:"):
        feed_stats["rest_unique_events"] += 1
    # FOUND notification is queued immediately but Telegram is never awaited here,
    # so it cannot delay signing/submission.
    queue_trade_notice(source_detect_message(wallet, info, payload, source))
    # Persistence is intentionally off the critical path.
    asyncio.create_task(asyncio.to_thread(persist_seen, key, wallet, source, payload, detected_ms))

    if force_skip_reason:
        asyncio.create_task(log_skipped(
            key, wallet, info, payload, source, detected_ms, copy_mode(), copy_usdc(), copy_slippage(), force_skip_reason
        ))
        return
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

    asyncio.create_task(copy_trade(key, wallet, info, payload, source, detected_ms, detected_perf_ns))


async def copy_trade(key, wallet, info, payload, source, detected_ms, detected_perf_ns=None):
    asset = str(payload.get("asset") or payload.get("asset_id") or "")
    fast = btc15_fastlane_snapshot(payload)
    if fast.get("fast_lane"):
        btc15_stats["fastlane_events"] += 1
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
            result = await submit_live_fak(asset, side, requested, limit_dec, detected_ms, detected_perf_ns)
            # Explicit balance rejection is safe to retry once on SELL after the
            # short CLOB balance-cache propagation window.
            if side == "SELL" and result.get("status") == "REJECTED_BALANCE_ALLOWANCE":
                await asyncio.sleep(LIVE_SELL_BALANCE_RETRY_MS / 1000.0)
                result = await submit_live_fak(asset, side, requested, limit_dec, detected_ms, detected_perf_ns)
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
            "build_sign_us": result.get("build_sign_us"),
            "detect_to_submit_ms": result.get("detect_to_submit_ms"),
            "detect_to_submit_us": result.get("detect_to_submit_us"),
            "fast_lane": 1 if fast.get("fast_lane") else 0,
            "fast_book_age_ms": fast.get("book_age_ms"),
            "fast_book_price": fast.get("book_price"),
            "fast_signer_warm": 1 if fast.get("signer_warm") else 0,
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

        recovery_queued = False
        if mode == "LIVE" and RECOVERY_ENABLE:
            remaining_for_recovery = max(0.0, requested - filled)
            recoverable = (
                remaining_for_recovery >= MIN_ORDER_SHARES - 1e-9
                and (filled > 1e-12 or transient_recovery_status(row["status"], row["error"]))
            )
            if recoverable:
                recovery_queued = await asyncio.to_thread(
                    recovery_schedule_db,
                    key=key, wallet=wallet, info=info, payload=payload, source=source,
                    requested=requested, accounted=filled, accounted_gross=gross,
                    limit_price=limit_price, mode=mode, initial_status=row["status"],
                    initial_error=row["error"], detected_ms=detected_ms,
                )
                if recovery_queued:
                    queue_trade_notice(
                        f"🔁 RECOVERY ПОСТАВЛЕН В ОЧЕРЕДЬ\n"
                        f"👛 {safe_label(info.get('label'), wallet)}\n"
                        f"Остаток {remaining_for_recovery:.4f}sh будет доисполняться автоматически "
                        f"по тому же limit {limit_price:.4f}. STOP приостанавливает recovery; START возобновляет."
                    )

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
            if row.get("detect_to_submit_us") is not None:
                us = sf(row.get("detect_to_submit_us"))
                latency.append(f"detect→submit {us/1000.0:.2f}ms")
            elif row.get("detect_to_submit_ms") is not None:
                latency.append(f"detect→submit {si(row['detect_to_submit_ms'])}ms")
            if row.get("api_ms") is not None:
                latency.append(f"API {si(row['api_ms'])}ms")
            reason = "" if filled > 0 else "\nПричина: " + human_copy_reason(row["status"], row["error"])
            if (ambiguous or str(row["status"]).upper() in {"REJECTED", "REJECTED_NO_MATCH"}) and row.get("error"):
                reason += f"\ntech: {str(row['error'])[:500]}"
            age_est = row.get("source_age_ms_est")
            if age_est is not None:
                latency.insert(0, f"source→detect ≈{si(age_est)}ms*")
            if recovery_queued:
                reason += "\n🔁 Recovery: ON — остаток сохранён и будет доисполняться в фоне в пределах slippage."
            queue_trade_notice(
                f"{head}\n"
                f"👛 {label}\n"
                f"{payload.get('title') or payload.get('slug') or asset}\n"
                f"{payload.get('outcome') or ''}\n"
                f"{target_line}\n{sizing_line}\n{our_line}{reason}\n"
                f"mode {mode} | source {source}"
                + (
                    f"\n🚀 BTC15 FAST LANE | book "
                    f"{('fresh '+str(fast.get('book_age_ms'))+'ms') if fast.get('book_fresh') else ('age '+str(fast.get('book_age_ms'))+'ms' if fast.get('book_age_ms') is not None else 'not ready')}"
                    f" | signer {'WARM' if fast.get('signer_warm') else 'COLD'}"
                    if fast.get("fast_lane") else ""
                )
                + (f"\n⏱ {' | '.join(latency)}" if latency else "")
                + ("\n* source→detect по публичному timestamp, обычно с точностью до 1 секунды." if age_est is not None else "")
            )


def base_order_row(key, wallet, info, payload, source, detected_ms, mode, amount, slip):
    return {
        "event_key": key,
        "wallet": wallet,
        "wallet_label": safe_label(info.get("label"), wallet),
        "source": source,
        "detected_ms": detected_ms,
        "target_timestamp": si(payload.get("timestamp"), 0),
        "source_age_ms_est": estimated_source_age_ms(payload, detected_ms),
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
        "build_sign_us": None, "detect_to_submit_us": None,
        "fast_lane": 0, "fast_book_age_ms": None, "fast_book_price": None, "fast_signer_warm": 0,
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


async def _prime_rest_wallet(wallet):
    """Seed current REST history as already-seen so startup never copies old trades."""
    collected = []
    trades = await get_json(
        f"{DATA_API}/trades",
        params={"user": wallet, "limit": 100, "takerOnly": "false"},
        timeout=4,
    )
    if isinstance(trades, list):
        feed_stats["rest_trades_last_ms"] = now_ms()
        feed_stats["rest_trades_polls"] += 1
        collected.extend((p, "rest:prime:trades") for p in trades if isinstance(p, dict))
    else:
        feed_stats["rest_trades_errors"] += 1

    if REST_ACTIVITY_ENABLE:
        activity = await get_json(
            f"{DATA_API}/activity",
            params={"user": wallet, "type": "TRADE", "limit": 100},
            timeout=4,
        )
        if isinstance(activity, list):
            feed_stats["rest_activity_last_ms"] = now_ms()
            feed_stats["rest_activity_polls"] += 1
            collected.extend((p, "rest:prime:activity") for p in activity if isinstance(p, dict))
        else:
            feed_stats["rest_activity_errors"] += 1

    primed = 0
    for p, src in collected:
        p.setdefault("proxyWallet", wallet)
        if str(p.get("type") or "").upper() not in {"", "TRADE"}:
            continue
        k = event_key(p)
        if hot_seen_add(k):
            primed += 1
            asyncio.create_task(asyncio.to_thread(persist_seen, k, wallet, src, p, now_ms()))
    return primed


async def _poll_rest_wallet(wallet, use_activity=False):
    if use_activity:
        data = await get_json(
            f"{DATA_API}/activity",
            params={"user": wallet, "type": "TRADE", "limit": 50},
            timeout=4,
        )
        source = "rest:activity"
        if isinstance(data, list):
            feed_stats["rest_activity_last_ms"] = now_ms()
            feed_stats["rest_activity_polls"] += 1
        else:
            feed_stats["rest_activity_errors"] += 1
            return
    else:
        data = await get_json(
            f"{DATA_API}/trades",
            params={"user": wallet, "limit": 50, "takerOnly": "false"},
            timeout=4,
        )
        source = "rest:trades"
        if isinstance(data, list):
            feed_stats["rest_trades_last_ms"] = now_ms()
            feed_stats["rest_trades_polls"] += 1
        else:
            feed_stats["rest_trades_errors"] += 1
            return

    # Oldest first so target-shadow and proportional SELL tracking remain ordered.
    data = sorted((p for p in data if isinstance(p, dict)), key=lambda x: si(x.get("timestamp"), 0))
    for p in data:
        p.setdefault("proxyWallet", wallet)
        if str(p.get("type") or "").upper() not in {"", "TRADE"}:
            continue
        ts = si(p.get("timestamp"), 0)
        age = max(0.0, time.time() - ts) if ts else 999999.0
        if age > REST_AUDIT_LOOKBACK_SEC:
            continue
        k = event_key(p)
        # Do not pre-add k here: ingest_trade owns dedupe and FOUND/result auditing.
        if k in seen_hot:
            continue
        if age > REST_MAX_COPY_AGE_SEC:
            feed_stats["rest_late_detected"] += 1
            await ingest_trade(p, source=source, force_skip_reason=f"REST_LATE:{age:.1f}s > {REST_MAX_COPY_AGE_SEC:.1f}s")
        else:
            await ingest_trade(p, source=source)


async def rest_fallback_loop():
    # Priming is per runtime/wallet: existing history is marked seen once; any record
    # that APPEARS afterwards is new even if the Data API publishes it several seconds late.
    primed = set()
    last_activity = 0.0
    while True:
        try:
            if not REST_FALLBACK_ENABLE or not watched_wallets:
                await asyncio.sleep(0.5)
                continue
            wallets = list(watched_wallets)
            for wallet in wallets:
                if wallet not in primed:
                    await _prime_rest_wallet(wallet)
                    primed.add(wallet)
            await asyncio.gather(*(_poll_rest_wallet(w, use_activity=False) for w in wallets))
            if REST_ACTIVITY_ENABLE and (time.monotonic() - last_activity >= REST_ACTIVITY_INTERVAL):
                await asyncio.gather(*(_poll_rest_wallet(w, use_activity=True) for w in wallets))
                last_activity = time.monotonic()
            # If a wallet is removed and later re-added, prime it again to avoid historical copies.
            primed.intersection_update(watched_wallets.keys())
            await asyncio.sleep(REST_FALLBACK_INTERVAL)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("REST fallback loop")
            await asyncio.sleep(0.5)


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
            [{"text": "📡 STATUS"}, {"text": "🔔 NOTIFY"}],
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
        f"REST detected: unique {feed_stats['rest_unique_events']} | raw {feed_stats['rest_raw_events']} | late {feed_stats['rest_late_detected']}\n"
        f"REST polls: trades {feed_stats['rest_trades_polls']} err {feed_stats['rest_trades_errors']} | activity {feed_stats['rest_activity_polls']} err {feed_stats['rest_activity_errors']}\n"
        f"RECOVERY: {'ON' if RECOVERY_ENABLE else 'OFF'} | active {recovery_active_count()} | done {recovery_stats['completed']} | reconciled {recovery_stats['reconciled']} | retries {recovery_stats['retries']}\n"
        f"BTC15 FAST: {'ON' if BTC15_FASTLANE_ENABLE else 'OFF'} | bookWS {'UP' if btc15_stats['ws_connected'] else 'DOWN'} | assets {len(btc15_assets)} | signer warm {len(btc15_signer_warmed)} | hits {btc15_stats['fastlane_events']}\n"
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
        entered_addr = normalize_address(parts[0] if parts else "")
        label = parts[1].strip() if len(parts) > 1 else ""
        if not entered_addr:
            await tg_send("Invalid address. Format: 0x... Label")
            return
        addr, profile_name = await resolve_proxy_wallet(entered_addr)
        if not addr:
            await tg_send("Invalid/unresolvable wallet address.")
            return
        own = normalize_address(POLYMARKET_WALLET_ADDRESS)
        if addr == own or entered_addr == own:
            await tg_send("⛔ You cannot watch/copy your own execution wallet (self-copy loop protection).")
            return
        if addr not in watched_wallets and len(watched_wallets) >= MAX_WALLETS:
            await tg_send(f"Wallet limit reached: {MAX_WALLETS}")
            return
        label = safe_label(label or profile_name, addr)
        with db() as conn:
            conn.execute(
                """INSERT INTO watched_wallets(address,label,enabled,added_ms) VALUES(?,?,1,?)
                   ON CONFLICT(address) DO UPDATE SET label=excluded.label,enabled=1""",
                (addr, label, now_ms()),
            )
            conn.commit()
        load_wallets()
        resolved_note = f"\nResolved proxyWallet: {addr}" if entered_addr != addr else ""
        await tg_send(f"✅ Added {label}\nEntered: {entered_addr}{resolved_note}\nExisting positions will NOT be copied. I am only seeding target inventory for proportional future SELLs.")
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
    elif upper in {"📡 STATUS", "STATUS", "/STATUS", "MENU", "/MENU", "HELP", "/HELP"}:
        await tg_send(status_text(), main_keyboard())
    else:
        await tg_send(status_text(), main_keyboard())


async def _warm_wallet_notify(addr, label):
    n = await warm_target_positions(addr)
    await tg_send(f"👛 {label}: target inventory seeded for {n} active assets. Only NEW trades are eligible for copying.")


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
        "btc15_fastlane": {**btc15_stats, "enabled": BTC15_FASTLANE_ENABLE, "assets": len(btc15_assets), "signer_warm": len(btc15_signer_warmed)},
        "recovery": {**recovery_stats, "enabled": RECOVERY_ENABLE, "active": recovery_active_count()},
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
    backfill_recent_recovery_jobs()
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
        asyncio.create_task(btc15_discovery_loop(), name="btc15-discovery"),
        asyncio.create_task(btc15_market_ws_loop(), name="btc15-book-ws"),
        asyncio.create_task(recovery_loop(), name="delivery-recovery"),
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
