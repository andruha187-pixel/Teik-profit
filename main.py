import os
import io
import csv
import json
import time
import math
import zipfile
import sqlite3
import asyncio
import logging
import statistics
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# MULTI7 PRE-JUMP LAB — PAPER ONLY
# ============================================================
# Purpose:
#   Measure whether external crypto microstructure leads Polymarket 5m moves.
#
# Sources:
#   Polymarket CLOB (book + market discovery)
#   Binance USD-M futures (aggTrade + depth20@100ms)
#   Bybit USDT linear (publicTrade + orderbook50 + allLiquidation)
#   Coinbase spot where configured (market_trades + level2)
#
# Four PAPER strategies per token:
#   BASE        = exact SAFE67-style first V2 gate, ENTRY only
#   EXT_CONFIRM = BASE + external same-direction confirmation
#   EXT_VETO    = BASE unless external flow strongly opposes it
#   PRE_JUMP    = earlier entry below SAFE67 when cross-exchange microstructure is strong
#
# The lab also records external features every 500 ms and snapshots the
# 1/3/5/10/20 seconds BEFORE detected Polymarket jumps.
# ============================================================

VERSION = "1.0-multi7-prejump-lab"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))

ASSET_CONFIG = {
    "BTC":  {"prefix": "btc-updown-5m",  "label": "Bitcoin"},
    "XRP":  {"prefix": "xrp-updown-5m",  "label": "XRP"},
    "BNB":  {"prefix": "bnb-updown-5m",  "label": "BNB"},
    "SOL":  {"prefix": "sol-updown-5m",  "label": "Solana"},
    "ETH":  {"prefix": "eth-updown-5m",  "label": "Ethereum"},
    "DOGE": {"prefix": "doge-updown-5m", "label": "Dogecoin"},
    "HYPE": {"prefix": "hype-updown-5m", "label": "Hyperliquid"},
}


def _configured_symbols():
    raw = os.getenv("SYMBOLS", "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE")
    out = []
    for item in raw.split(","):
        sym = item.strip().upper()
        if sym in ASSET_CONFIG and sym not in out:
            out.append(sym)
    return out or list(ASSET_CONFIG)


SYMBOLS = _configured_symbols()

# Polymarket SAFE67 benchmark — intentionally 3-second sampling like our bot.
BASE_DECISION_INTERVAL = float(os.getenv("BASE_DECISION_INTERVAL", "3.0"))
TRADE_WINDOW_SECONDS = float(os.getenv("TRADE_WINDOW_SECONDS", "180"))
LOOKBACK_TICKS = int(os.getenv("LOOKBACK_TICKS", "2"))
V2_PRICE_MIN = float(os.getenv("V2_ELIGIBLE_PRICE_MIN", "0.55"))
V2_PRICE_MAX = float(os.getenv("V2_ELIGIBLE_PRICE_MAX", "0.75"))
V2_MOM_MIN = float(os.getenv("V2_ELIGIBLE_MOM_MIN", "0.03"))
V2_MOM_MAX = float(os.getenv("V2_ELIGIBLE_MOM_MAX", "0.30"))
SAFE_PRICE_MIN = float(os.getenv("SAFE_ENTRY_PRICE_MIN", "0.67"))
SAFE_PRICE_MAX = float(os.getenv("SAFE_ENTRY_PRICE_MAX", "0.75"))
SAFE_MOM_MIN = float(os.getenv("SAFE_ENTRY_MOM_MIN", "0.05"))
SAFE_MOM_MAX = float(os.getenv("SAFE_ENTRY_MOM_MAX", "0.10"))

# Fast lab sampling / storage.
FAST_INTERVAL = float(os.getenv("FAST_INTERVAL", "0.25"))
FEATURE_PERSIST_INTERVAL = float(os.getenv("FEATURE_PERSIST_INTERVAL", "0.50"))
FEATURE_HISTORY_SECONDS = float(os.getenv("FEATURE_HISTORY_SECONDS", "45"))
SOURCE_FRESH_MS = int(os.getenv("SOURCE_FRESH_MS", "2500"))

# External strategy thresholds. These are hypotheses to test, not claimed optimums.
EXT_VOTE_THRESHOLD = float(os.getenv("EXT_VOTE_THRESHOLD", "0.25"))
EXT_CONFIRM_SCORE = float(os.getenv("EXT_CONFIRM_SCORE", "0.30"))
EXT_CONFIRM_MIN_VENUES = int(os.getenv("EXT_CONFIRM_MIN_VENUES", "2"))
EXT_VETO_SCORE = float(os.getenv("EXT_VETO_SCORE", "-0.20"))
EXT_VETO_MIN_OPPOSING_VENUES = int(os.getenv("EXT_VETO_MIN_OPPOSING_VENUES", "2"))

PREJUMP_PRICE_MIN = float(os.getenv("PREJUMP_PRICE_MIN", "0.52"))
PREJUMP_PRICE_MAX = float(os.getenv("PREJUMP_PRICE_MAX", "0.66"))
PREJUMP_SCORE = float(os.getenv("PREJUMP_SCORE", "0.55"))
PREJUMP_MIN_VENUES = int(os.getenv("PREJUMP_MIN_VENUES", "2"))
PREJUMP_MIN_ELAPSED = float(os.getenv("PREJUMP_MIN_ELAPSED", "5"))
PREJUMP_MAX_ELAPSED = float(os.getenv("PREJUMP_MAX_ELAPSED", "160"))
PREJUMP_PM_MOM_MIN = float(os.getenv("PREJUMP_PM_MOM_MIN", "-0.01"))
PREJUMP_PM_MOM_MAX = float(os.getenv("PREJUMP_PM_MOM_MAX", "0.05"))
PREJUMP_REQUIRE_BINANCE_BYBIT = os.getenv(
    "PREJUMP_REQUIRE_BINANCE_BYBIT", "1"
).strip().lower() in {"1", "true", "yes", "on"}

# Jump detector used for retrospective lead/lag study.
JUMP_MOVE = float(os.getenv("JUMP_MOVE", "0.08"))
JUMP_WINDOW_SEC = float(os.getenv("JUMP_WINDOW_SEC", "5"))
JUMP_COOLDOWN_SEC = float(os.getenv("JUMP_COOLDOWN_SEC", "10"))

# PAPER simulation.
ENTRY_ORDER_SIZE = float(os.getenv("ENTRY_ORDER_SIZE", "5"))
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))
TAKE_PROFIT_USDC = float(os.getenv("TAKE_PROFIT_USDC", "0.60"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))

# Polymarket discovery/reporting.
DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "10"))
REPORT_DELAY_SECONDS = int(os.getenv("REPORT_DELAY_SECONDS", "300"))
REPORT_CHECK_INTERVAL = int(os.getenv("REPORT_CHECK_INTERVAL", "30"))
WS_MAX_CONNECTION_AGE_SEC = int(os.getenv("WS_MAX_CONNECTION_AGE_SEC", "900"))
MEMORY_CLEANUP_INTERVAL = int(os.getenv("MEMORY_CLEANUP_INTERVAL", "60"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "900"))

# External source switches / symbol maps.
ENABLE_BINANCE = os.getenv("ENABLE_BINANCE", "1").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_BYBIT = os.getenv("ENABLE_BYBIT", "1").strip().lower() in {"1", "true", "yes", "on"}
ENABLE_COINBASE = os.getenv("ENABLE_COINBASE", "1").strip().lower() in {"1", "true", "yes", "on"}

BINANCE_SYMBOLS = {s: f"{s}USDT" for s in SYMBOLS}
BYBIT_SYMBOLS = {s: f"{s}USDT" for s in SYMBOLS}


def _parse_coinbase_products():
    default = "BTC:BTC-USD,ETH:ETH-USD,SOL:SOL-USD,XRP:XRP-USD,DOGE:DOGE-USD"
    raw = os.getenv("COINBASE_PRODUCTS", default)
    out = {}
    for pair in raw.split(","):
        if ":" not in pair:
            continue
        sym, product = pair.split(":", 1)
        sym = sym.strip().upper()
        product = product.strip().upper()
        if sym in SYMBOLS and product:
            out[sym] = product
    return out


COINBASE_PRODUCTS = _parse_coinbase_products()
COINBASE_PRODUCT_TO_SYMBOL = {v: k for k, v in COINBASE_PRODUCTS.items()}

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
POLYMARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/public/stream")
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")
COINBASE_WS = os.getenv("COINBASE_WS", "wss://advanced-trade-ws.coinbase.com")

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "prejump_lab_multi7.db"
REPORT_DIR = DATA_DIR / "prejump_lab_multi7_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("prejump-lab")

# Four strategies, all ENTRY-only by design. This isolates signal quality.
STRATEGY_CODES = ("BASE", "EXT_CONFIRM", "EXT_VETO", "PRE_JUMP")
STRATEGIES = [
    {"symbol": symbol, "code": code, "name": f"{symbol}_{code}"}
    for symbol in SYMBOLS
    for code in STRATEGY_CODES
]
STRATEGIES_BY_SYMBOL = {
    symbol: [x for x in STRATEGIES if x["symbol"] == symbol]
    for symbol in SYMBOLS
}
STRATEGY_BY_NAME = {x["name"]: x for x in STRATEGIES}

session: Optional[aiohttp.ClientSession] = None

# Polymarket books / markets.
books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
base_price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=64)))
fast_pm_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=512)))
strategy_state = {}
settle_lock = asyncio.Lock()

# External state. Event feeds are aggregated into compact 100ms/500ms buckets.
venue_books = defaultdict(lambda: defaultdict(lambda: {
    "bids": {}, "asks": {}, "received_ms": 0,
}))
venue_last_price = defaultdict(dict)
venue_trade_buckets = defaultdict(lambda: defaultdict(lambda: deque(maxlen=600)))
venue_liq_buckets = defaultdict(lambda: defaultdict(lambda: deque(maxlen=120)))
venue_sample_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=256)))
feature_history = defaultdict(lambda: deque(maxlen=512))
source_health = defaultdict(lambda: defaultdict(lambda: {
    "connected": False, "last_ms": 0, "messages": 0, "errors": 0, "last_error": "",
}))
last_feature_persist_ms = defaultdict(int)
last_jump_ms = {}


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ts():
    return int(time.time())


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


def clamp(v, lo=-1.0, hi=1.0):
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


def parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def fee_usdc(shares, price):
    fee = shares * CRYPTO_FEE_RATE * price * (1.0 - price)
    return round(fee, 5) if fee >= 0.000005 else 0.0


def nearest_history(items, target_ms, max_gap_ms=1500):
    if not items:
        return None
    best = min(items, key=lambda x: abs(si(x.get("sample_ms")) - target_ms))
    return best if abs(si(best.get("sample_ms")) - target_ms) <= max_gap_ms else None


# ============================================================
# DATABASE
# ============================================================

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS discovered_markets (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            question TEXT,
            slug TEXT,
            start_ts INTEGER,
            end_ts INTEGER,
            up_asset TEXT,
            down_asset TEXT,
            discovered_ms INTEGER,
            resolved INTEGER DEFAULT 0,
            winning_asset TEXT,
            winning_outcome TEXT
        );

        CREATE TABLE IF NOT EXISTS gate_decisions (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            elapsed_sec REAL,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            ext_score REAL,
            same_votes INTEGER,
            opposing_votes INTEGER,
            passed INTEGER,
            reason TEXT,
            features_json TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS signal_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_ms INTEGER,
            condition_id TEXT,
            symbol TEXT,
            variant TEXT,
            outcome TEXT,
            asset TEXT,
            pm_ask REAL,
            pm_bid REAL,
            pm_momentum REAL,
            ext_score REAL,
            same_votes INTEGER,
            opposing_votes INTEGER,
            elapsed_sec REAL,
            reason TEXT,
            features_json TEXT
        );

        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_ms INTEGER,
            condition_id TEXT,
            symbol TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_cost REAL,
            fee REAL,
            total_cost REAL,
            book_age_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS paper_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exit_ms INTEGER,
            condition_id TEXT,
            symbol TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            reason TEXT,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_proceeds REAL,
            fee REAL,
            net_proceeds REAL,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT,
            symbol TEXT,
            variant TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            buy_cost REAL,
            exit_proceeds REAL,
            payout REAL,
            pnl REAL,
            buy_trades INTEGER,
            exit_trades INTEGER,
            settled_ms INTEGER,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS external_features (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_ms INTEGER,
            condition_id TEXT,
            symbol TEXT,
            elapsed_sec REAL,
            pm_up_bid REAL,
            pm_up_ask REAL,
            pm_down_bid REAL,
            pm_down_ask REAL,
            external_open_proxy REAL,
            external_gap_bps REAL,
            ext_score REAL,
            up_votes INTEGER,
            down_votes INTEGER,
            fresh_venues INTEGER,
            binance_score REAL,
            bybit_score REAL,
            coinbase_score REAL,
            binance_json TEXT,
            bybit_json TEXT,
            coinbase_json TEXT
        );

        CREATE TABLE IF NOT EXISTS jump_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            jump_ms INTEGER,
            condition_id TEXT,
            symbol TEXT,
            outcome TEXT,
            asset TEXT,
            elapsed_sec REAL,
            start_ask REAL,
            end_ask REAL,
            move REAL,
            window_sec REAL,
            pre_1s_json TEXT,
            pre_3s_json TEXT,
            pre_5s_json TEXT,
            pre_10s_json TEXT,
            pre_20s_json TEXT
        );

        CREATE TABLE IF NOT EXISTS trials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER UNIQUE,
            condition_id TEXT,
            symbol TEXT,
            variant TEXT,
            outcome TEXT,
            entry_ms INTEGER,
            entry_avg REAL,
            entry_cost REAL,
            ext_score REAL,
            same_votes INTEGER,
            features_json TEXT,
            max_net_3s REAL,
            max_net_5s REAL,
            max_net_10s REAL,
            max_net_20s REAL,
            hit_tp_3s INTEGER DEFAULT 0,
            hit_tp_5s INTEGER DEFAULT 0,
            hit_tp_10s INTEGER DEFAULT 0,
            hit_tp_20s INTEGER DEFAULT 0,
            complete_20s INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS source_health_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_ms INTEGER,
            venue TEXT,
            symbol TEXT,
            connected INTEGER,
            age_ms INTEGER,
            messages INTEGER,
            errors INTEGER,
            last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_feature_ms ON external_features(sample_ms);
        CREATE INDEX IF NOT EXISTS idx_jump_ms ON jump_events(jump_ms);
        CREATE INDEX IF NOT EXISTS idx_signal_ms ON signal_events(signal_ms);
        CREATE INDEX IF NOT EXISTS idx_trade_ms ON paper_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_exit_ms ON paper_exits(exit_ms);
        CREATE INDEX IF NOT EXISTS idx_result_ms ON market_results(settled_ms);
        CREATE INDEX IF NOT EXISTS idx_trial_entry ON trials(entry_ms);
        CREATE INDEX IF NOT EXISTS idx_health_ms ON source_health_log(sample_ms);
        """)

        defaults = {"trading_enabled": "1"}
        for strategy in STRATEGIES:
            defaults[f"paper_initial:{strategy['name']}"] = str(PAPER_START_BALANCE)
            defaults[f"paper_cash:{strategy['name']}"] = str(PAPER_START_BALANCE)
        for key, value in defaults.items():
            if conn.execute("SELECT 1 FROM state WHERE key=?", (key,)).fetchone() is None:
                conn.execute("INSERT INTO state(key,value) VALUES(?,?)", (key, value))
        conn.commit()


def state_get(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def state_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        conn.commit()


def paper_cash(strategy_name):
    return sf(state_get(f"paper_cash:{strategy_name}", PAPER_START_BALANCE), PAPER_START_BALANCE)


def set_paper_cash(strategy_name, value):
    state_set(f"paper_cash:{strategy_name}", round(float(value), 10))


def trading_enabled():
    return state_get("trading_enabled", "1") == "1"


# ============================================================
# HTTP / POLYMARKET BOOK
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                text = await r.text()
                if r.status == 200:
                    return json.loads(text)
                log.warning("HTTP %s %s -> %s", r.status, url, text[:200])
        except Exception as exc:
            log.warning("GET %s failed: %s", url, exc)
        await asyncio.sleep(0.3 * (attempt + 1))
    return None


def level_map(rows):
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            p = sf(row.get("price"), math.nan)
            q = sf(row.get("size"), 0)
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            p = sf(row[0], math.nan)
            q = sf(row[1], 0)
        else:
            continue
        if not math.isnan(p) and q > 0:
            out[p] = q
    return out


def apply_book(asset, payload, source="ws"):
    books[asset] = {
        "bids": level_map(payload.get("bids") or payload.get("b")),
        "asks": level_map(payload.get("asks") or payload.get("a")),
        "received_ms": now_ms(),
        "source": source,
    }


def apply_price_change(payload):
    changes = payload.get("price_changes") or payload.get("priceChanges") or []
    recv = now_ms()
    for ch in changes:
        if not isinstance(ch, dict):
            continue
        asset = str(ch.get("asset_id") or ch.get("token_id") or ch.get("tokenId") or "")
        if not asset:
            continue
        b = books.setdefault(asset, {"bids": {}, "asks": {}, "received_ms": recv, "source": "ws-delta"})
        p = sf(ch.get("price"), math.nan)
        q = sf(ch.get("size"), 0)
        side = str(ch.get("side", "")).upper()
        if math.isnan(p):
            continue
        target = b["bids"] if side == "BUY" else b["asks"]
        if q <= 0:
            target.pop(p, None)
        else:
            target[p] = q
        b["received_ms"] = recv
        b["source"] = "ws"


def best_ask(asset):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return None
    return min(b["asks"])


def best_bid(asset):
    b = books.get(asset)
    if not b or not b.get("bids"):
        return None
    return max(b["bids"])


async def refresh_book(asset):
    data = await get_json(f"{CLOB_API}/book", params={"token_id": asset})
    if isinstance(data, dict):
        apply_book(asset, data, "rest")
        return True
    return False


async def ensure_book(asset, need="asks"):
    b = books.get(asset)
    if b and b.get(need):
        age = now_ms() - si(b.get("received_ms"))
        if age <= MAX_BOOK_AGE_MS:
            return age
    await refresh_book(asset)
    b = books.get(asset)
    if not b or not b.get(need):
        return None
    return now_ms() - si(b.get("received_ms"))


def simulate_buy(asset, wanted):
    b = books.get(asset)
    if not b or not b.get("asks"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["asks"]):
        q = b["asks"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining


def simulate_sell(asset, wanted):
    b = books.get(asset)
    if not b or not b.get("bids"):
        return [], 0.0
    remaining = wanted
    fills = []
    for p in sorted(b["bids"], reverse=True):
        q = b["bids"][p]
        take = min(q, remaining)
        if take > 0:
            fills.append((p, take))
            remaining -= take
        if remaining <= 1e-12:
            break
    return fills, wanted - remaining

# ============================================================
# POLYMARKET MARKET DISCOVERY
# ============================================================

def market_symbol(market):
    sym = str((market or {}).get("symbol") or "").upper()
    if sym in ASSET_CONFIG:
        return sym
    slug = str((market or {}).get("slug") or "").lower()
    for candidate, cfg in ASSET_CONFIG.items():
        if slug.startswith(cfg["prefix"] + "-"):
            return candidate
    return None


def slot_start_from_slug(slug):
    try:
        return int(str(slug).rstrip("/").split("-")[-1])
    except Exception:
        return None


async def fetch_event_by_slug(slug):
    for url, params in (
        (f"{GAMMA_API}/events/slug/{slug}", None),
        (f"{GAMMA_API}/events", {"slug": slug}),
    ):
        data = await get_json(url, params=params)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
    return None


def parse_market_from_event(raw, event, symbol):
    if not isinstance(raw, dict) or symbol not in ASSET_CONFIG:
        return None
    cid = str(raw.get("conditionId") or raw.get("condition_id") or "")
    if not cid:
        return None
    title = str(raw.get("question") or raw.get("title") or event.get("title") or "Unknown")
    slug = str(raw.get("slug") or event.get("slug") or "")
    if slug and not slug.lower().startswith(ASSET_CONFIG[symbol]["prefix"] + "-"):
        return None

    outcomes = [str(x).strip().upper() for x in parse_jsonish(raw.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(raw.get("clobTokenIds"))]
    if len(tokens) < 2:
        return None

    up_asset = down_asset = None
    for i, outcome in enumerate(outcomes):
        if i >= len(tokens):
            break
        if outcome in {"UP", "YES"}:
            up_asset = tokens[i]
        elif outcome in {"DOWN", "NO"}:
            down_asset = tokens[i]
    up_asset = up_asset or tokens[0]
    down_asset = down_asset or tokens[1]

    start_ts = slot_start_from_slug(slug)
    if not start_ts:
        start_dt = parse_iso(raw.get("startDate")) or parse_iso(event.get("startDate"))
        start_ts = int(start_dt.timestamp()) if start_dt else None
    if not start_ts:
        return None

    return {
        "condition_id": cid,
        "symbol": symbol,
        "question": title,
        "slug": slug,
        "start_ts": int(start_ts),
        "end_ts": int(start_ts) + 300,
        "up_asset": str(up_asset),
        "down_asset": str(down_asset),
    }


async def discover_slot_market(symbol, slot_start):
    cfg = ASSET_CONFIG.get(symbol)
    if not cfg:
        return None
    slug = f"{cfg['prefix']}-{slot_start}"
    event = await fetch_event_by_slug(slug)
    if not event or not isinstance(event.get("markets"), list):
        return None
    for raw in event["markets"]:
        market = parse_market_from_event(raw, event, symbol)
        if market:
            return market
    return None


def persist_market(market):
    with db() as conn:
        conn.execute("""
            INSERT INTO discovered_markets(
                condition_id,symbol,question,slug,start_ts,end_ts,
                up_asset,down_asset,discovered_ms
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                symbol=excluded.symbol,question=excluded.question,slug=excluded.slug,
                start_ts=excluded.start_ts,end_ts=excluded.end_ts,
                up_asset=excluded.up_asset,down_asset=excluded.down_asset
        """, (
            market["condition_id"], market_symbol(market), market["question"], market["slug"],
            market["start_ts"], market["end_ts"], market["up_asset"], market["down_asset"], now_ms(),
        ))
        conn.commit()


async def subscribe_asset(asset):
    if not asset or asset in subscribed_assets:
        return
    subscribed_assets.add(asset)
    await ws_send_queue.put({"operation": "subscribe", "assets_ids": [asset]})


async def discovery_loop():
    last_current_slot = {}
    while True:
        try:
            n = now_ts()
            current = (n // 300) * 300
            for symbol in SYMBOLS:
                candidates = []
                for slot_start in (current, current + 300, current - 300):
                    market = await discover_slot_market(symbol, slot_start)
                    if market:
                        candidates.append(market)
                if not candidates:
                    log.info("Discovery %s: market not found for %s", symbol, utc_iso(current))
                    continue

                active = [m for m in candidates if m["start_ts"] - 5 <= n <= m["end_ts"] + 5]
                chosen = min(active or candidates, key=lambda m: abs(n - m["start_ts"]))
                for market in candidates:
                    cid = market["condition_id"]
                    if cid in markets:
                        continue
                    markets[cid] = market
                    persist_market(market)
                    await subscribe_asset(market["up_asset"])
                    await subscribe_asset(market["down_asset"])
                    log.info(
                        "MARKET %s | %s | slug=%s | start=%s",
                        symbol, market["question"], market["slug"], utc_iso(market["start_ts"]),
                    )
                if last_current_slot.get(symbol) != current:
                    log.info("CURRENT %s %s | selected=%s", symbol, utc_iso(current), chosen["slug"])
                    last_current_slot[symbol] = current
        except Exception:
            log.exception("Discovery loop failed")
        await asyncio.sleep(DISCOVERY_INTERVAL)


# ============================================================
# POLYMARKET WEBSOCKET
# ============================================================

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, list) else [obj]
    except Exception:
        return []


async def ws_sender(ws):
    while True:
        msg = await ws_send_queue.get()
        try:
            await ws.send(jd(msg))
        except Exception:
            await ws_send_queue.put(msg)
            return


async def ws_ping(ws):
    while True:
        try:
            await ws.send("PING")
        except Exception:
            return
        await asyncio.sleep(10)


async def polymarket_ws_loop():
    while True:
        try:
            if not subscribed_assets:
                await asyncio.sleep(1)
                continue
            async with websockets.connect(
                POLYMARKET_WS,
                ping_interval=None,
                close_timeout=5,
                max_size=20_000_000,
            ) as ws:
                await ws.send(jd({
                    "assets_ids": list(subscribed_assets),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log.info("Polymarket WS connected | assets=%d", len(subscribed_assets))
                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))
                try:
                    started = time.monotonic()
                    async for raw in ws:
                        if time.monotonic() - started >= WS_MAX_CONNECTION_AGE_SEC:
                            log.info("Polymarket WS periodic reconnect")
                            break
                        for ev in parse_ws(raw):
                            if not isinstance(ev, dict):
                                continue
                            et = str(ev.get("event_type") or ev.get("type") or "")
                            payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else ev
                            if et == "book":
                                asset = str(payload.get("asset_id") or payload.get("token_id") or "")
                                if asset:
                                    apply_book(asset, payload)
                            elif et == "price_change":
                                apply_price_change(payload)
                            elif et == "market_resolved":
                                await settle_from_resolution(payload)
                finally:
                    sender.cancel()
                    ping.cancel()
        except Exception as exc:
            log.warning("Polymarket WS reconnect: %s", exc)
            await asyncio.sleep(1)


# ============================================================
# EXTERNAL MICROSTRUCTURE STATE
# ============================================================

def mark_source(venue, symbol, connected=None, error=None):
    h = source_health[venue][symbol]
    if connected is not None:
        h["connected"] = bool(connected)
    if error is not None:
        h["errors"] += 1
        h["last_error"] = str(error)[:300]
    h["last_ms"] = now_ms()


def source_message(venue, symbol):
    h = source_health[venue][symbol]
    h["connected"] = True
    h["last_ms"] = now_ms()
    h["messages"] += 1


def append_bucket(bucket_deque, event_ms, signed_value, abs_value, bucket_ms=100):
    key = (si(event_ms) // bucket_ms) * bucket_ms
    if bucket_deque and bucket_deque[-1][0] == key:
        old = bucket_deque[-1]
        bucket_deque[-1] = (key, old[1] + signed_value, old[2] + abs_value)
    else:
        bucket_deque.append((key, signed_value, abs_value))


def update_external_trade(venue, symbol, event_ms, price, qty, taker_side):
    price = sf(price)
    qty = sf(qty)
    if price <= 0 or qty <= 0:
        return
    side = str(taker_side).upper()
    sign = 1.0 if side == "BUY" else -1.0
    notional = price * qty
    append_bucket(venue_trade_buckets[venue][symbol], event_ms, sign * notional, notional, 100)
    venue_last_price[venue][symbol] = price
    source_message(venue, symbol)


def update_liquidation(venue, symbol, event_ms, price, qty, forced_market_side):
    price = sf(price)
    qty = sf(qty)
    if price <= 0 or qty <= 0:
        return
    side = str(forced_market_side).upper()
    sign = 1.0 if side == "BUY" else -1.0
    notional = price * qty
    append_bucket(venue_liq_buckets[venue][symbol], event_ms, sign * notional, notional, 500)
    source_message(venue, symbol)


def replace_external_book(venue, symbol, bids, asks, event_ms=None):
    b = venue_books[venue][symbol]
    b["bids"] = level_map(bids)
    b["asks"] = level_map(asks)
    b["received_ms"] = si(event_ms, now_ms()) or now_ms()
    source_message(venue, symbol)


def apply_external_book_delta(venue, symbol, bids, asks, event_ms=None):
    b = venue_books[venue][symbol]
    for side_name, rows in (("bids", bids), ("asks", asks)):
        target = b[side_name]
        for row in rows or []:
            if isinstance(row, dict):
                p = sf(row.get("price_level") or row.get("price"), math.nan)
                q = sf(row.get("new_quantity") or row.get("size"), 0)
            elif isinstance(row, (list, tuple)) and len(row) >= 2:
                p = sf(row[0], math.nan)
                q = sf(row[1], 0)
            else:
                continue
            if math.isnan(p):
                continue
            if q <= 0:
                target.pop(p, None)
            else:
                target[p] = q
    b["received_ms"] = si(event_ms, now_ms()) or now_ms()
    source_message(venue, symbol)


def book_metrics(venue, symbol, levels=10):
    b = venue_books[venue][symbol]
    bids = b.get("bids") or {}
    asks = b.get("asks") or {}
    if not bids or not asks:
        return None

    bid_prices = sorted(bids, reverse=True)[:levels]
    ask_prices = sorted(asks)[:levels]
    best_bid = bid_prices[0]
    best_ask = ask_prices[0]
    bid_q = sf(bids[best_bid])
    ask_q = sf(asks[best_ask])
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return None

    bid_depth = sum(p * sf(bids[p]) for p in bid_prices)
    ask_depth = sum(p * sf(asks[p]) for p in ask_prices)
    denom = bid_depth + ask_depth
    obi = (bid_depth - ask_depth) / denom if denom > 0 else 0.0

    micro_denom = bid_q + ask_q
    micro = (
        (best_ask * bid_q + best_bid * ask_q) / micro_denom
        if micro_denom > 0 else mid
    )
    micro_bps = (micro / mid - 1.0) * 10000.0
    spread_bps = (best_ask / best_bid - 1.0) * 10000.0 if best_bid > 0 else 0.0

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_q": bid_q,
        "ask_q": ask_q,
        "mid": mid,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "obi": clamp(obi),
        "micro_bps": micro_bps,
        "spread_bps": spread_bps,
        "book_age_ms": max(0, now_ms() - si(b.get("received_ms"))),
    }


def bucket_flow(venue, symbol, window_sec, liquidations=False):
    dq = venue_liq_buckets[venue][symbol] if liquidations else venue_trade_buckets[venue][symbol]
    cutoff = now_ms() - int(window_sec * 1000)
    signed = absolute = 0.0
    for ms, sv, av in reversed(dq):
        if ms < cutoff:
            break
        signed += sv
        absolute += av
    return signed, absolute, (signed / absolute if absolute > 0 else 0.0)


def prior_venue_sample(venue, symbol, seconds):
    hist = venue_sample_history[venue][symbol]
    if not hist:
        return None
    target = now_ms() - int(seconds * 1000)
    best = min(hist, key=lambda x: abs(si(x.get("sample_ms")) - target))
    return best if abs(si(best.get("sample_ms")) - target) <= max(750, FAST_INTERVAL * 3000) else None


def venue_features(venue, symbol):
    bm = book_metrics(venue, symbol)
    last_price = sf(venue_last_price[venue].get(symbol), 0)
    if bm is None and last_price <= 0:
        return None

    current = bm or {
        "best_bid": last_price,
        "best_ask": last_price,
        "bid_q": 0.0,
        "ask_q": 0.0,
        "mid": last_price,
        "bid_depth": 0.0,
        "ask_depth": 0.0,
        "obi": 0.0,
        "micro_bps": 0.0,
        "spread_bps": 0.0,
        "book_age_ms": 999999,
    }
    price = last_price or current["mid"]

    p1 = prior_venue_sample(venue, symbol, 1)
    p3 = prior_venue_sample(venue, symbol, 3)
    p10 = prior_venue_sample(venue, symbol, 10)

    def ret_bps(prior):
        old = sf((prior or {}).get("price"), 0)
        return (price / old - 1.0) * 10000.0 if old > 0 and price > 0 else 0.0

    ret1 = ret_bps(p1)
    ret3 = ret_bps(p3)
    ret10 = ret_bps(p10)
    _, _, flow1 = bucket_flow(venue, symbol, 1)
    _, _, flow3 = bucket_flow(venue, symbol, 3)
    _, _, flow10 = bucket_flow(venue, symbol, 10)
    liq_signed, liq_abs, liq_3 = bucket_flow(venue, symbol, 3, liquidations=True)

    prior_bid_depth = sf((p1 or {}).get("bid_depth"), current["bid_depth"])
    prior_ask_depth = sf((p1 or {}).get("ask_depth"), current["ask_depth"])
    bid_change = (
        current["bid_depth"] / prior_bid_depth - 1.0
        if prior_bid_depth > 0 and current["bid_depth"] > 0 else 0.0
    )
    ask_change = (
        current["ask_depth"] / prior_ask_depth - 1.0
        if prior_ask_depth > 0 and current["ask_depth"] > 0 else 0.0
    )
    # Positive means bullish liquidity pressure: asks disappear and/or bids build.
    liquidity_pressure = clamp((bid_change - ask_change) / 2.0)

    score = (
        0.28 * flow1
        + 0.17 * flow3
        + 0.16 * math.tanh(ret1 / 6.0)
        + 0.10 * math.tanh(ret3 / 15.0)
        + 0.10 * current["obi"]
        + 0.07 * math.tanh(current["micro_bps"] / 1.5)
        + 0.08 * liquidity_pressure
        + 0.04 * liq_3
    )
    score = clamp(score)

    h = source_health[venue][symbol]
    age = max(0, now_ms() - si(h.get("last_ms"))) if h.get("last_ms") else 999999
    fresh = bool(h.get("connected")) and age <= SOURCE_FRESH_MS

    return {
        "sample_ms": now_ms(),
        "venue": venue,
        "symbol": symbol,
        "fresh": fresh,
        "source_age_ms": age,
        "price": price,
        "ret_bps_1s": ret1,
        "ret_bps_3s": ret3,
        "ret_bps_10s": ret10,
        "flow_1s": flow1,
        "flow_3s": flow3,
        "flow_10s": flow10,
        "flow_accel": flow1 - flow10,
        "obi": current["obi"],
        "micro_bps": current["micro_bps"],
        "spread_bps": current["spread_bps"],
        "bid_depth": current["bid_depth"],
        "ask_depth": current["ask_depth"],
        "bid_depth_change_1s": bid_change,
        "ask_depth_change_1s": ask_change,
        "liquidity_pressure": liquidity_pressure,
        "liq_signed_3s": liq_signed,
        "liq_abs_3s": liq_abs,
        "liq_flow_3s": liq_3,
        "score": score,
    }


def build_external_snapshot(symbol, sample_ms=None):
    sample_ms = si(sample_ms, now_ms()) or now_ms()
    venue_data = {}
    for venue in ("binance", "bybit", "coinbase"):
        vf = venue_features(venue, symbol)
        if vf:
            venue_data[venue] = vf

    weights = {"binance": 0.40, "bybit": 0.40, "coinbase": 0.20}
    weighted = total_weight = 0.0
    up_votes = down_votes = fresh_venues = 0
    fresh_names = []
    for venue, vf in venue_data.items():
        if not vf.get("fresh"):
            continue
        fresh_venues += 1
        fresh_names.append(venue)
        w = weights.get(venue, 0.0)
        weighted += w * sf(vf.get("score"))
        total_weight += w
        if sf(vf.get("score")) >= EXT_VOTE_THRESHOLD:
            up_votes += 1
        if sf(vf.get("score")) <= -EXT_VOTE_THRESHOLD:
            down_votes += 1

    ext_score = weighted / total_weight if total_weight > 0 else 0.0
    mids = [sf(v.get("price")) for v in venue_data.values() if v.get("fresh") and sf(v.get("price")) > 0]
    median_price = statistics.median(mids) if mids else None

    return {
        "sample_ms": sample_ms,
        "symbol": symbol,
        "ext_score": clamp(ext_score),
        "up_votes": up_votes,
        "down_votes": down_votes,
        "fresh_venues": fresh_venues,
        "fresh_names": fresh_names,
        "median_external_price": median_price,
        "binance": venue_data.get("binance"),
        "bybit": venue_data.get("bybit"),
        "coinbase": venue_data.get("coinbase"),
    }


def latest_feature(symbol):
    hist = feature_history[symbol]
    return hist[-1] if hist else None


def directional_external(feature, outcome):
    if not feature:
        return {
            "score": 0.0, "same_votes": 0, "opposing_votes": 0,
            "fresh_venues": 0, "binance_same": False, "bybit_same": False,
        }
    sign = 1.0 if str(outcome).upper() == "UP" else -1.0
    score = sign * sf(feature.get("ext_score"))
    same_votes = feature.get("up_votes", 0) if sign > 0 else feature.get("down_votes", 0)
    opposing_votes = feature.get("down_votes", 0) if sign > 0 else feature.get("up_votes", 0)

    def same_vote(venue):
        vf = feature.get(venue) or {}
        return bool(vf.get("fresh")) and sign * sf(vf.get("score")) >= EXT_VOTE_THRESHOLD

    return {
        "score": score,
        "same_votes": si(same_votes),
        "opposing_votes": si(opposing_votes),
        "fresh_venues": si(feature.get("fresh_venues")),
        "binance_same": same_vote("binance"),
        "bybit_same": same_vote("bybit"),
    }


def append_venue_feature_history(venue, symbol, vf):
    if not vf:
        return
    venue_sample_history[venue][symbol].append({
        "sample_ms": si(vf.get("sample_ms"), now_ms()),
        "price": sf(vf.get("price")),
        "bid_depth": sf(vf.get("bid_depth")),
        "ask_depth": sf(vf.get("ask_depth")),
    })


# ============================================================
# OFFICIAL-SHAPE MESSAGE HANDLERS (unit-testable)
# ============================================================

def handle_binance_payload(symbol, data):
    if not isinstance(data, dict):
        return
    event = str(data.get("e") or "")
    if event == "aggTrade":
        # Binance m=True => buyer is maker => taker side is SELL.
        taker = "SELL" if bool(data.get("m")) else "BUY"
        update_external_trade(
            "binance", symbol,
            data.get("T") or data.get("E") or now_ms(),
            data.get("p"), data.get("q"), taker,
        )
    elif event == "depthUpdate" or data.get("b") is not None:
        replace_external_book(
            "binance", symbol,
            data.get("b") or data.get("bids") or [],
            data.get("a") or data.get("asks") or [],
            data.get("T") or data.get("E") or now_ms(),
        )


def handle_bybit_message(symbol, msg):
    if not isinstance(msg, dict):
        return
    topic = str(msg.get("topic") or "")
    data = msg.get("data")
    if topic.startswith("publicTrade.") and isinstance(data, list):
        for tr in data:
            if isinstance(tr, dict):
                update_external_trade(
                    "bybit", symbol,
                    tr.get("T") or msg.get("ts") or now_ms(),
                    tr.get("p"), tr.get("v"), tr.get("S"),
                )
    elif topic.startswith("orderbook.") and isinstance(data, dict):
        event_ms = data.get("cts") or msg.get("cts") or msg.get("ts") or now_ms()
        if str(msg.get("type")) == "snapshot":
            replace_external_book("bybit", symbol, data.get("b") or [], data.get("a") or [], event_ms)
        else:
            apply_external_book_delta("bybit", symbol, data.get("b") or [], data.get("a") or [], event_ms)
    elif topic.startswith("allLiquidation."):
        rows = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
        for liq in rows:
            # Bybit S=Buy means LONG position liquidated => forced SELL.
            position_side = str(liq.get("S") or "")
            forced_side = "SELL" if position_side.upper() == "BUY" else "BUY"
            update_liquidation(
                "bybit", symbol,
                liq.get("T") or msg.get("ts") or now_ms(),
                liq.get("p"), liq.get("v"), forced_side,
            )


def handle_coinbase_message(msg):
    if not isinstance(msg, dict):
        return
    channel = str(msg.get("channel") or "")
    events = msg.get("events") or []
    if channel in {"l2_data", "level2"}:
        for ev in events:
            if not isinstance(ev, dict):
                continue
            product = str(ev.get("product_id") or "").upper()
            symbol = COINBASE_PRODUCT_TO_SYMBOL.get(product)
            if not symbol:
                continue
            bids, asks = [], []
            for u in (ev.get("updates") or []):
                row = [u.get("price_level"), u.get("new_quantity")]
                if str(u.get("side")).lower() == "bid":
                    bids.append(row)
                else:
                    asks.append(row)
            if str(ev.get("type")) == "snapshot":
                replace_external_book("coinbase", symbol, bids, asks, now_ms())
            else:
                apply_external_book_delta("coinbase", symbol, bids, asks, now_ms())
    elif channel == "market_trades":
        for ev in events:
            for tr in (ev.get("trades") or []):
                product = str(tr.get("product_id") or "").upper()
                symbol = COINBASE_PRODUCT_TO_SYMBOL.get(product)
                if not symbol:
                    continue
                # Coinbase side is maker side; taker side is opposite.
                maker_side = str(tr.get("side") or "").upper()
                taker_side = "SELL" if maker_side == "BUY" else "BUY"
                event_dt = parse_iso(tr.get("time"))
                event_ms = int(event_dt.timestamp() * 1000) if event_dt else now_ms()
                update_external_trade("coinbase", symbol, event_ms, tr.get("price"), tr.get("size"), taker_side)
    elif channel == "heartbeats":
        for symbol in COINBASE_PRODUCTS:
            source_message("coinbase", symbol)


# ============================================================
# BINANCE USD-M FUTURES PUBLIC FEED
# ============================================================

async def binance_symbol_loop(symbol):
    ex_symbol = BINANCE_SYMBOLS[symbol].lower()
    streams = f"{ex_symbol}@aggTrade/{ex_symbol}@depth20@100ms"
    url = f"{BINANCE_WS_BASE}?streams={streams}"
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, close_timeout=5,
                max_size=8_000_000,
            ) as ws:
                mark_source("binance", symbol, connected=True)
                log.info("Binance connected %s", symbol)
                backoff = 1.0
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data") if isinstance(msg, dict) else None
                    if not isinstance(data, dict):
                        continue
                    handle_binance_payload(symbol, data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mark_source("binance", symbol, connected=False, error=exc)
            log.warning("Binance %s reconnect: %s", symbol, exc)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)


# ============================================================
# BYBIT USDT LINEAR PUBLIC FEED
# ============================================================

async def bybit_symbol_loop(symbol):
    ex_symbol = BYBIT_SYMBOLS[symbol]
    topics = [
        f"orderbook.50.{ex_symbol}",
        f"publicTrade.{ex_symbol}",
        f"allLiquidation.{ex_symbol}",
    ]
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                BYBIT_WS, ping_interval=None, close_timeout=5, max_size=12_000_000,
            ) as ws:
                await ws.send(jd({"op": "subscribe", "args": topics}))
                mark_source("bybit", symbol, connected=True)
                log.info("Bybit connected %s", symbol)
                backoff = 1.0
                last_ping = time.monotonic()
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    except asyncio.TimeoutError:
                        await ws.send(jd({"op": "ping"}))
                        last_ping = time.monotonic()
                        continue
                    msg = json.loads(raw)
                    if msg.get("op") in {"subscribe", "pong"}:
                        if msg.get("success") is False:
                            mark_source("bybit", symbol, error=msg.get("ret_msg") or msg)
                        continue
                    handle_bybit_message(symbol, msg)
                    if time.monotonic() - last_ping > 20:
                        await ws.send(jd({"op": "ping"}))
                        last_ping = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            mark_source("bybit", symbol, connected=False, error=exc)
            log.warning("Bybit %s reconnect: %s", symbol, exc)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)


# ============================================================
# COINBASE SPOT PUBLIC FEED (configured products only)
# ============================================================

async def coinbase_loop():
    if not COINBASE_PRODUCTS:
        return
    products = list(COINBASE_PRODUCTS.values())
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                COINBASE_WS, ping_interval=20, ping_timeout=20, close_timeout=5,
                max_size=12_000_000,
            ) as ws:
                for channel in ("level2", "market_trades", "heartbeats"):
                    msg = {"type": "subscribe", "channel": channel}
                    if channel != "heartbeats":
                        msg["product_ids"] = products
                    await ws.send(jd(msg))
                for symbol in COINBASE_PRODUCTS:
                    mark_source("coinbase", symbol, connected=True)
                log.info("Coinbase connected | products=%s", ",".join(products))
                backoff = 1.0
                async for raw in ws:
                    msg = json.loads(raw)
                    handle_coinbase_message(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            for symbol in COINBASE_PRODUCTS:
                mark_source("coinbase", symbol, connected=False, error=exc)
            log.warning("Coinbase reconnect: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)

# ============================================================
# STRATEGY STATE / PAPER ACCOUNTING
# ============================================================

def get_strategy_state(condition_id, strategy):
    key = (condition_id, strategy["name"])
    if key in strategy_state:
        return strategy_state[key]

    st = {
        "gate_decided": False,
        "gate_passed": False,
        "gate_asset": None,
        "started": False,
        "primary_asset": None,
        "primary_outcome": None,
        "closed": False,
    }
    with db() as conn:
        gate = conn.execute(
            "SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?",
            (condition_id, strategy["name"]),
        ).fetchone()
        if gate:
            st["gate_decided"] = True
            st["gate_passed"] = bool(gate["passed"])
            st["gate_asset"] = str(gate["asset"] or "") or None

        trade = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id LIMIT 1",
            (condition_id, strategy["name"]),
        ).fetchone()
        if trade:
            st["started"] = True
            st["primary_asset"] = str(trade["asset"])
            st["primary_outcome"] = str(trade["outcome"])

        result = conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (condition_id, strategy["name"]),
        ).fetchone()
        st["closed"] = bool(result)

    strategy_state[key] = st
    return st


def position_totals(condition_id, variant_name):
    with db() as conn:
        buys = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id",
            (condition_id, variant_name),
        ).fetchall()
        exits = conn.execute(
            "SELECT * FROM paper_exits WHERE condition_id=? AND variant=? ORDER BY id",
            (condition_id, variant_name),
        ).fetchall()

    bought = sum(sf(r["filled_shares"]) for r in buys)
    exited = sum(sf(r["filled_shares"]) for r in exits)
    buy_cost = sum(sf(r["total_cost"]) for r in buys)
    exit_net = sum(sf(r["net_proceeds"]) for r in exits)
    asset = str(buys[0]["asset"]) if buys else None
    outcome = str(buys[0]["outcome"]) if buys else None
    return {
        "buys": buys,
        "exits": exits,
        "bought": bought,
        "exited": exited,
        "remaining": max(0.0, bought - exited),
        "buy_cost": buy_cost,
        "exit_net": exit_net,
        "primary_asset": asset,
        "primary_outcome": outcome,
    }


def projected_full_exit(condition_id, variant_name):
    pos = position_totals(condition_id, variant_name)
    remaining = pos["remaining"]
    asset = pos["primary_asset"]
    if not asset or remaining <= 1e-8:
        return None
    fills, filled = simulate_sell(asset, remaining)
    if filled < remaining - 1e-8:
        return None
    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    net = gross - fee
    avg = gross / filled if filled > 1e-9 else None
    pnl = pos["exit_net"] + net - pos["buy_cost"]
    return {
        "fills": fills, "filled": filled, "gross": gross, "fee": fee,
        "net": net, "avg": avg, "total_pnl": pnl, "asset": asset,
        "remaining": remaining, "pos": pos,
    }


def store_gate(market, strategy, asset, outcome, ask, ref, momentum, elapsed, passed, reason, feature):
    directional = directional_external(feature, outcome)
    with db() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO gate_decisions(
                condition_id,variant,decision_ms,elapsed_sec,asset,outcome,ask,
                reference_ask,momentum,ext_score,same_votes,opposing_votes,
                passed,reason,features_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            market["condition_id"], strategy["name"], now_ms(), elapsed,
            asset, outcome, ask, ref, momentum, directional["score"],
            directional["same_votes"], directional["opposing_votes"],
            1 if passed else 0, reason, jd(feature or {}),
        ))
        conn.commit()


def store_signal(market, strategy, asset, outcome, ask, momentum, elapsed, reason, feature):
    directional = directional_external(feature, outcome)
    with db() as conn:
        conn.execute("""
            INSERT INTO signal_events(
                signal_ms,condition_id,symbol,variant,outcome,asset,pm_ask,pm_bid,
                pm_momentum,ext_score,same_votes,opposing_votes,elapsed_sec,reason,features_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), market["condition_id"], market_symbol(market), strategy["name"],
            outcome, asset, ask, best_bid(asset), momentum,
            directional["score"], directional["same_votes"], directional["opposing_votes"],
            elapsed, reason, jd(feature or {}),
        ))
        conn.commit()


async def execute_paper_entry(market, strategy, asset, outcome, feature):
    st = get_strategy_state(market["condition_id"], strategy)
    if st["closed"] or st["started"]:
        return False

    age = await ensure_book(asset, "asks")
    if age is None:
        return False
    fills, filled = simulate_buy(asset, ENTRY_ORDER_SIZE)
    if filled < ENTRY_ORDER_SIZE - 1e-8:
        log.info("PAPER NO LIQ %s %s | wanted %.2f got %.2f", market_symbol(market), strategy["code"], ENTRY_ORDER_SIZE, filled)
        return False

    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    total = gross + fee
    cash_before = paper_cash(strategy["name"])
    if cash_before - total < MIN_FREE_CASH:
        log.info("PAPER CASH BLOCK %s | need %.2f cash %.2f", strategy["name"], total, cash_before)
        return False
    avg = gross / filled
    trade_ms = now_ms()

    directional = directional_external(feature, outcome)
    with db() as conn:
        cur = conn.execute("""
            INSERT INTO paper_trades(
                trade_ms,condition_id,symbol,variant,asset,outcome,requested_shares,
                filled_shares,avg_price,gross_cost,fee,total_cost,book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade_ms, market["condition_id"], market_symbol(market), strategy["name"],
            asset, outcome, ENTRY_ORDER_SIZE, filled, avg, gross, fee, total, age,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        trade_id = cur.lastrowid
        conn.execute("""
            INSERT INTO trials(
                trade_id,condition_id,symbol,variant,outcome,entry_ms,entry_avg,entry_cost,
                ext_score,same_votes,features_json,
                max_net_3s,max_net_5s,max_net_10s,max_net_20s
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade_id, market["condition_id"], market_symbol(market), strategy["name"],
            outcome, trade_ms, avg, total, directional["score"], directional["same_votes"],
            jd(feature or {}), -total, -total, -total, -total,
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{strategy['name']}", str(cash_before - total)),
        )
        conn.commit()

    st["started"] = True
    st["primary_asset"] = asset
    st["primary_outcome"] = outcome
    log.info(
        "PAPER BUY %-4s %-11s %s | %.2fsh @ %.4f | ext=%+.3f votes=%d | cash %.2f->%.2f",
        market_symbol(market), strategy["code"], outcome, filled, avg,
        directional["score"], directional["same_votes"], cash_before, cash_before-total,
    )
    return True


async def maybe_take_profit(market, strategy):
    st = get_strategy_state(market["condition_id"], strategy)
    if not st["started"] or st["closed"]:
        return False
    candidate = projected_full_exit(market["condition_id"], strategy["name"])
    if not candidate or candidate["total_pnl"] + 1e-12 < TAKE_PROFIT_USDC:
        return False

    await ensure_book(candidate["asset"], "bids")
    candidate = projected_full_exit(market["condition_id"], strategy["name"])
    if not candidate or candidate["total_pnl"] + 1e-12 < TAKE_PROFIT_USDC:
        return False

    pos = candidate["pos"]
    outcome = pos["primary_outcome"]
    cash_before = paper_cash(strategy["name"])
    cash_after = cash_before + candidate["net"]
    with db() as conn:
        conn.execute("""
            INSERT INTO paper_exits(
                exit_ms,condition_id,symbol,variant,asset,outcome,reason,requested_shares,
                filled_shares,avg_price,gross_proceeds,fee,net_proceeds,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), market["condition_id"], market_symbol(market), strategy["name"],
            candidate["asset"], outcome, "TAKE_PROFIT", candidate["remaining"],
            candidate["filled"], candidate["avg"], candidate["gross"], candidate["fee"],
            candidate["net"], jd([{"price": p, "shares": q} for p, q in candidate["fills"]]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{strategy['name']}", str(cash_after)),
        )
        pnl = candidate["total_pnl"]
        conn.execute("""
            INSERT OR IGNORE INTO market_results(
                condition_id,symbol,variant,winning_asset,winning_outcome,buy_cost,
                exit_proceeds,payout,pnl,buy_trades,exit_trades,settled_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            market["condition_id"], market_symbol(market), strategy["name"], "", "TAKE_PROFIT",
            pos["buy_cost"], pos["exit_net"] + candidate["net"], 0.0, pnl,
            len(pos["buys"]), len(pos["exits"]) + 1, now_ms(),
        ))
        conn.commit()
    st["closed"] = True
    log.info("TAKE PROFIT %-4s %-11s | NET=%+.3f target=%.2f", market_symbol(market), strategy["code"], candidate["total_pnl"], TAKE_PROFIT_USDC)
    return True


# ============================================================
# SAFE67 / EXTERNAL STRATEGY SIGNALS
# ============================================================

def base_momentum(condition_id, asset):
    h = base_price_history[condition_id][asset]
    if len(h) <= LOOKBACK_TICKS:
        return None, None
    ask = sf(h[-1][1])
    ref = sf(h[-1 - LOOKBACK_TICKS][1])
    return ask - ref, ref


def first_v2_candidates(market):
    out = []
    for asset, outcome in ((market["up_asset"], "Up"), (market["down_asset"], "Down")):
        ask = best_ask(asset)
        if ask is None:
            continue
        momentum, ref = base_momentum(market["condition_id"], asset)
        if momentum is None:
            continue
        if V2_PRICE_MIN <= ask <= V2_PRICE_MAX and V2_MOM_MIN <= momentum <= V2_MOM_MAX:
            out.append((momentum, asset, outcome, ask, ref))
    out.sort(key=lambda x: (-x[0], x[2]))
    return out


def safe67_pass(ask, momentum):
    return SAFE_PRICE_MIN <= ask <= SAFE_PRICE_MAX and SAFE_MOM_MIN <= momentum <= SAFE_MOM_MAX


async def evaluate_base_family(market, elapsed):
    candidates = first_v2_candidates(market)
    if not candidates:
        return
    momentum, asset, outcome, ask, ref = candidates[0]
    feature = latest_feature(market_symbol(market))
    directional = directional_external(feature, outcome)

    for code in ("BASE", "EXT_CONFIRM", "EXT_VETO"):
        strategy = next(x for x in STRATEGIES_BY_SYMBOL[market_symbol(market)] if x["code"] == code)
        st = get_strategy_state(market["condition_id"], strategy)
        if st["closed"] or st["started"]:
            continue

        if not st["gate_decided"]:
            safe = safe67_pass(ask, momentum)
            if not safe:
                passed = False
                if ask < SAFE_PRICE_MIN:
                    reason = "SAFE_PRICE_LOW"
                elif ask > SAFE_PRICE_MAX:
                    reason = "SAFE_PRICE_HIGH"
                elif momentum < SAFE_MOM_MIN:
                    reason = "SAFE_MOM_LOW"
                else:
                    reason = "SAFE_MOM_HIGH"
            elif code == "EXT_CONFIRM":
                passed = (
                    directional["score"] >= EXT_CONFIRM_SCORE
                    and directional["same_votes"] >= EXT_CONFIRM_MIN_VENUES
                )
                reason = "EXT_CONFIRM_OK" if passed else "EXT_CONFIRM_MISSING"
            elif code == "EXT_VETO":
                veto = (
                    directional["score"] <= EXT_VETO_SCORE
                    and directional["opposing_votes"] >= EXT_VETO_MIN_OPPOSING_VENUES
                )
                passed = not veto
                reason = "EXT_VETO_BLOCK" if veto else "EXT_VETO_OK"
            else:
                passed = True
                reason = "SAFE67_OK"

            st["gate_decided"] = True
            st["gate_passed"] = bool(passed)
            st["gate_asset"] = asset if passed else None
            store_gate(market, strategy, asset, outcome, ask, ref, momentum, elapsed, passed, reason, feature)
            log.info(
                "GATE %-4s %-11s %s %.3f mom=%+.3f | ext=%+.3f votes=%d/%d | %s",
                market_symbol(market), code, outcome, ask, momentum,
                directional["score"], directional["same_votes"], directional["opposing_votes"],
                "PASS" if passed else f"SKIP {reason}",
            )
            if passed:
                store_signal(market, strategy, asset, outcome, ask, momentum, elapsed, reason, feature)

        if not st["gate_passed"]:
            continue
        # Preserve current SAFE67 entry quality at actual execution time.
        asset2 = st.get("gate_asset")
        ask2 = best_ask(asset2) if asset2 else None
        mom2, _ = base_momentum(market["condition_id"], asset2) if asset2 else (None, None)
        if ask2 is None or mom2 is None or not safe67_pass(ask2, mom2):
            continue
        outcome2 = "Up" if asset2 == market["up_asset"] else "Down"
        await execute_paper_entry(market, strategy, asset2, outcome2, feature)


def pm_fast_momentum(condition_id, asset, seconds=1.0):
    h = fast_pm_history[condition_id][asset]
    if len(h) < 2:
        return None
    target = now_ms() - int(seconds * 1000)
    prior = min(h, key=lambda x: abs(x[0] - target))
    if abs(prior[0] - target) > 700:
        return None
    return sf(h[-1][1]) - sf(prior[1])


async def evaluate_prejump(market, elapsed):
    strategy = next(x for x in STRATEGIES_BY_SYMBOL[market_symbol(market)] if x["code"] == "PRE_JUMP")
    st = get_strategy_state(market["condition_id"], strategy)
    if st["closed"] or st["started"] or st["gate_decided"]:
        return
    if not (PREJUMP_MIN_ELAPSED <= elapsed <= PREJUMP_MAX_ELAPSED):
        return

    feature = latest_feature(market_symbol(market))
    if not feature or si(feature.get("fresh_venues")) < PREJUMP_MIN_VENUES:
        return
    ext_score = sf(feature.get("ext_score"))
    if abs(ext_score) < PREJUMP_SCORE:
        return
    outcome = "Up" if ext_score > 0 else "Down"
    asset = market["up_asset"] if outcome == "Up" else market["down_asset"]
    directional = directional_external(feature, outcome)
    if directional["same_votes"] < PREJUMP_MIN_VENUES:
        return
    if PREJUMP_REQUIRE_BINANCE_BYBIT and not (directional["binance_same"] and directional["bybit_same"]):
        return

    ask = best_ask(asset)
    if ask is None or not (PREJUMP_PRICE_MIN <= ask <= PREJUMP_PRICE_MAX):
        return
    mom = pm_fast_momentum(market["condition_id"], asset, 1.0)
    if mom is None or not (PREJUMP_PM_MOM_MIN <= mom <= PREJUMP_PM_MOM_MAX):
        return

    st["gate_decided"] = True
    st["gate_passed"] = True
    st["gate_asset"] = asset
    reason = "PREJUMP_EXTERNAL_OK"
    store_gate(market, strategy, asset, outcome, ask, ask - mom, mom, elapsed, True, reason, feature)
    store_signal(market, strategy, asset, outcome, ask, mom, elapsed, reason, feature)
    log.info(
        "PREJUMP %-4s %s %.3f pmMom=%+.3f | ext=%+.3f votes=%d | venues=%s",
        market_symbol(market), outcome, ask, mom, directional["score"],
        directional["same_votes"], ",".join(feature.get("fresh_names") or []),
    )
    await execute_paper_entry(market, strategy, asset, outcome, feature)

# ============================================================
# FAST FEATURE SAMPLING / JUMP DETECTOR / HORIZON LABELS
# ============================================================

external_open_proxy = {}


def current_market_for_symbol(symbol, at_ts=None):
    at_ts = sf(at_ts, time.time())
    candidates = [m for m in markets.values() if market_symbol(m) == symbol and m["start_ts"] - 2 <= at_ts <= m["end_ts"] + 2]
    if not candidates:
        return None
    return min(candidates, key=lambda m: abs(at_ts - m["start_ts"]))


def enrich_feature_with_polymarket(feature, market, elapsed):
    out = dict(feature)
    if not market:
        out.update({
            "condition_id": None, "elapsed_sec": None,
            "pm_up_bid": None, "pm_up_ask": None,
            "pm_down_bid": None, "pm_down_ask": None,
            "external_open_proxy": None, "external_gap_bps": None,
        })
        return out

    cid = market["condition_id"]
    median_price = sf(feature.get("median_external_price"), 0)
    if cid not in external_open_proxy and median_price > 0 and elapsed >= -2:
        external_open_proxy[cid] = median_price
    proxy = sf(external_open_proxy.get(cid), 0)
    gap = (median_price / proxy - 1.0) * 10000.0 if proxy > 0 and median_price > 0 else None

    out.update({
        "condition_id": cid,
        "elapsed_sec": elapsed,
        "pm_up_bid": best_bid(market["up_asset"]),
        "pm_up_ask": best_ask(market["up_asset"]),
        "pm_down_bid": best_bid(market["down_asset"]),
        "pm_down_ask": best_ask(market["down_asset"]),
        # This is an exchange-median proxy relative to the first sample near slot start.
        # It is NOT labelled Chainlink; direct Chainlink Data Streams require credentials.
        "external_open_proxy": proxy if proxy > 0 else None,
        "external_gap_bps": gap,
    })
    return out


def persist_feature(feature):
    with db() as conn:
        conn.execute("""
            INSERT INTO external_features(
                sample_ms,condition_id,symbol,elapsed_sec,pm_up_bid,pm_up_ask,
                pm_down_bid,pm_down_ask,external_open_proxy,external_gap_bps,
                ext_score,up_votes,down_votes,fresh_venues,
                binance_score,bybit_score,coinbase_score,binance_json,bybit_json,coinbase_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            feature.get("sample_ms"), feature.get("condition_id"), feature.get("symbol"),
            feature.get("elapsed_sec"), feature.get("pm_up_bid"), feature.get("pm_up_ask"),
            feature.get("pm_down_bid"), feature.get("pm_down_ask"),
            feature.get("external_open_proxy"), feature.get("external_gap_bps"), feature.get("ext_score"),
            feature.get("up_votes"), feature.get("down_votes"), feature.get("fresh_venues"),
            sf((feature.get("binance") or {}).get("score"), math.nan),
            sf((feature.get("bybit") or {}).get("score"), math.nan),
            sf((feature.get("coinbase") or {}).get("score"), math.nan),
            jd(feature.get("binance") or {}), jd(feature.get("bybit") or {}), jd(feature.get("coinbase") or {}),
        ))
        conn.commit()


def detect_jump(market, asset, outcome, elapsed):
    cid = market["condition_id"]
    hist = fast_pm_history[cid][asset]
    if len(hist) < 2:
        return
    current_ms, current_ask = hist[-1]
    target_ms = current_ms - int(JUMP_WINDOW_SEC * 1000)
    prior = min(hist, key=lambda x: abs(x[0] - target_ms))
    if abs(prior[0] - target_ms) > 800:
        return
    move = sf(current_ask) - sf(prior[1])
    if move < JUMP_MOVE:
        return
    key = (cid, asset)
    if current_ms - si(last_jump_ms.get(key)) < int(JUMP_COOLDOWN_SEC * 1000):
        return
    last_jump_ms[key] = current_ms

    fh = list(feature_history[market_symbol(market)])
    contexts = {}
    for sec in (1, 3, 5, 10, 20):
        snap = nearest_history(fh, current_ms - sec * 1000, max_gap_ms=900)
        contexts[sec] = snap or {}

    with db() as conn:
        conn.execute("""
            INSERT INTO jump_events(
                jump_ms,condition_id,symbol,outcome,asset,elapsed_sec,start_ask,end_ask,
                move,window_sec,pre_1s_json,pre_3s_json,pre_5s_json,pre_10s_json,pre_20s_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            current_ms, cid, market_symbol(market), outcome, asset, elapsed,
            sf(prior[1]), sf(current_ask), move, JUMP_WINDOW_SEC,
            jd(contexts[1]), jd(contexts[3]), jd(contexts[5]), jd(contexts[10]), jd(contexts[20]),
        ))
        conn.commit()
    log.info(
        "JUMP %-4s %-4s %.3f->%.3f (+%.3f/%gs) | ext now=%+.3f",
        market_symbol(market), outcome, sf(prior[1]), sf(current_ask), move,
        JUMP_WINDOW_SEC, sf((latest_feature(market_symbol(market)) or {}).get("ext_score")),
    )


def executable_net_for_trial(trial):
    with db() as conn:
        trade = conn.execute("SELECT * FROM paper_trades WHERE id=?", (trial["trade_id"],)).fetchone()
    if not trade:
        return None
    shares = sf(trade["filled_shares"])
    fills, filled = simulate_sell(str(trade["asset"]), shares)
    if filled < shares - 1e-8:
        return None
    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    return gross - fee - sf(trial["entry_cost"])


def update_trial_horizons():
    now = now_ms()
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM trials WHERE complete_20s=0 ORDER BY entry_ms LIMIT 500"
        ).fetchall()

    for row in rows:
        age = (now - si(row["entry_ms"])) / 1000.0
        pnl = executable_net_for_trial(row)
        updates = {}
        if pnl is not None:
            for sec, col in ((3, "max_net_3s"), (5, "max_net_5s"), (10, "max_net_10s"), (20, "max_net_20s")):
                if age <= sec + FAST_INTERVAL:
                    updates[col] = max(sf(row[col], -1e9), pnl)
                    if pnl + 1e-12 >= TAKE_PROFIT_USDC:
                        updates[f"hit_tp_{sec}s"] = 1
        if age >= 20:
            updates["complete_20s"] = 1
        if not updates:
            continue
        cols = ",".join(f"{k}=?" for k in updates)
        with db() as conn:
            conn.execute(f"UPDATE trials SET {cols} WHERE id=?", (*updates.values(), row["id"]))
            conn.commit()


async def fast_lab_loop():
    while True:
        started = time.monotonic()
        t_ms = now_ms()
        t_s = time.time()
        try:
            for symbol in SYMBOLS:
                # Compute venue features from the event buckets/books, then append the
                # compact venue sample used for future 1/3/10s return/depth comparisons.
                raw_feature = build_external_snapshot(symbol, t_ms)
                for venue in ("binance", "bybit", "coinbase"):
                    append_venue_feature_history(venue, symbol, raw_feature.get(venue))

                market = current_market_for_symbol(symbol, t_s)
                elapsed = (t_s - market["start_ts"]) if market else None
                feature = enrich_feature_with_polymarket(raw_feature, market, elapsed)
                feature_history[symbol].append(feature)

                if market and -2 <= elapsed <= 305:
                    for asset, outcome in ((market["up_asset"], "Up"), (market["down_asset"], "Down")):
                        ask = best_ask(asset)
                        if ask is not None:
                            fast_pm_history[market["condition_id"]][asset].append((t_ms, ask))
                            if elapsed >= 0:
                                detect_jump(market, asset, outcome, elapsed)

                    if trading_enabled() and 0 <= elapsed <= TRADE_WINDOW_SECONDS:
                        await evaluate_prejump(market, elapsed)

                    # PAPER TP protection remains active even if new entries are STOPped.
                    for strategy in STRATEGIES_BY_SYMBOL[symbol]:
                        await maybe_take_profit(market, strategy)

                if t_ms - last_feature_persist_ms[symbol] >= int(FEATURE_PERSIST_INTERVAL * 1000):
                    if market:
                        persist_feature(feature)
                    last_feature_persist_ms[symbol] = t_ms

            update_trial_horizons()
        except Exception:
            log.exception("Fast lab loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.02, FAST_INTERVAL - spent))


async def base_strategy_loop():
    while True:
        started = time.monotonic()
        t_s = time.time()
        try:
            for symbol in SYMBOLS:
                market = current_market_for_symbol(symbol, t_s)
                if not market:
                    continue
                elapsed = t_s - market["start_ts"]
                if not (0 <= elapsed <= TRADE_WINDOW_SECONDS):
                    continue
                for asset in (market["up_asset"], market["down_asset"]):
                    ask = best_ask(asset)
                    if ask is not None:
                        base_price_history[market["condition_id"]][asset].append((now_ms(), ask))
                if trading_enabled():
                    await evaluate_base_family(market, elapsed)
        except Exception:
            log.exception("BASE strategy loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.05, BASE_DECISION_INTERVAL - spent))

# ============================================================
# RESOLUTION
# ============================================================

async def settle_from_resolution(ev):
    cid = str(ev.get("market") or ev.get("condition_id") or "")
    winning_asset = str(ev.get("winning_asset_id") or ev.get("winning_asset") or "")
    winning_outcome = str(ev.get("winning_outcome") or "")
    if cid and winning_asset:
        await settle_market(cid, winning_asset, winning_outcome)


async def settle_market(cid, winning_asset, winning_outcome):
    async with settle_lock:
        market = markets.get(cid)
        if not market:
            with db() as conn:
                row = conn.execute("SELECT * FROM discovered_markets WHERE condition_id=?", (cid,)).fetchone()
                if not row:
                    return
                market = dict(row)
        symbol = market_symbol(market)
        messages = []

        with db() as conn:
            for strategy in STRATEGIES_BY_SYMBOL.get(symbol, []):
                name = strategy["name"]
                if conn.execute(
                    "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchone():
                    continue
                buys = conn.execute(
                    "SELECT * FROM paper_trades WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchall()
                if not buys:
                    continue
                exits = conn.execute(
                    "SELECT * FROM paper_exits WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchall()
                buy_cost = sum(sf(r["total_cost"]) for r in buys)
                exit_net = sum(sf(r["net_proceeds"]) for r in exits)
                winning_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == str(winning_asset))
                winning_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == str(winning_asset))
                payout = max(0.0, winning_bought - winning_exited)
                pnl = exit_net + payout - buy_cost

                conn.execute("""
                    INSERT INTO market_results(
                        condition_id,symbol,variant,winning_asset,winning_outcome,buy_cost,
                        exit_proceeds,payout,pnl,buy_trades,exit_trades,settled_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    cid, symbol, name, winning_asset, winning_outcome, buy_cost,
                    exit_net, payout, pnl, len(buys), len(exits), now_ms(),
                ))
                cash_row = conn.execute(
                    "SELECT value FROM state WHERE key=?", (f"paper_cash:{name}",)
                ).fetchone()
                cash_before = sf(cash_row["value"] if cash_row else PAPER_START_BALANCE, PAPER_START_BALANCE)
                cash_after = cash_before + payout
                conn.execute(
                    "INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (f"paper_cash:{name}", str(cash_after)),
                )
                st = get_strategy_state(cid, strategy)
                st["closed"] = True
                messages.append(f"{strategy['code']} {pnl:+.2f}")

            conn.execute(
                "UPDATE discovered_markets SET resolved=1,winning_asset=?,winning_outcome=? WHERE condition_id=?",
                (winning_asset, winning_outcome, cid),
            )
            conn.commit()

        if messages:
            log.info("RESOLVED %s %s | %s", symbol, winning_outcome, " | ".join(messages))


def resolve_winner_from_market(market_row):
    if not isinstance(market_row, dict):
        return None, None
    outcomes = [str(x) for x in parse_jsonish(market_row.get("outcomes"))]
    tokens = [str(x) for x in parse_jsonish(market_row.get("clobTokenIds"))]
    prices_raw = parse_jsonish(market_row.get("outcomePrices"))
    if len(outcomes) >= 2 and len(tokens) >= 2 and len(prices_raw) >= 2:
        prices = [sf(x, -1) for x in prices_raw]
        best_idx = max(range(len(prices)), key=lambda i: prices[i])
        best = prices[best_idx]
        second = max([prices[i] for i in range(len(prices)) if i != best_idx] or [-1])
        closed = bool(market_row.get("closed", False))
        resolved = bool(market_row.get("resolved", False) or market_row.get("umaResolutionStatus") == "resolved")
        if best >= 0.999 and second <= 0.001 and (closed or resolved or best >= 0.9999):
            return tokens[best_idx], outcomes[best_idx]
    token_objs = market_row.get("tokens")
    if isinstance(token_objs, list):
        for tok in token_objs:
            if isinstance(tok, dict) and bool(tok.get("winner", False)):
                asset = str(tok.get("token_id") or tok.get("tokenId") or tok.get("id") or "")
                outcome = str(tok.get("outcome") or tok.get("name") or "")
                if asset:
                    return asset, outcome
    return None, None


async def fetch_resolved_market_by_slug(slug, condition_id):
    event = await fetch_event_by_slug(slug)
    if not isinstance(event, dict) or not isinstance(event.get("markets"), list):
        return None
    for row in event["markets"]:
        if isinstance(row, dict):
            cid = str(row.get("conditionId") or row.get("condition_id") or "")
            if cid == str(condition_id):
                return row
    if len(event["markets"]) == 1 and isinstance(event["markets"][0], dict):
        return event["markets"][0]
    return None


async def resolution_fallback_loop():
    while True:
        try:
            cutoff = now_ts() - 10
            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id,slug FROM discovered_markets
                    WHERE resolved=0 AND end_ts<? ORDER BY end_ts LIMIT 50
                """, (cutoff,)).fetchall()
            for row in rows:
                market_row = await fetch_resolved_market_by_slug(str(row["slug"] or ""), str(row["condition_id"]))
                if not market_row:
                    continue
                winning_asset, winning_outcome = resolve_winner_from_market(market_row)
                if winning_asset:
                    log.info("RESOLUTION FALLBACK %s | winner=%s", row["slug"], winning_outcome)
                    await settle_market(str(row["condition_id"]), winning_asset, winning_outcome)
        except Exception:
            log.exception("Resolution fallback failed")
        await asyncio.sleep(10)


# ============================================================
# SOURCE HEALTH / MEMORY
# ============================================================

def current_rss_mb():
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return None


def cleanup_memory():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as conn:
        rows = conn.execute(
            "SELECT condition_id FROM discovered_markets WHERE resolved=1 AND end_ts<?",
            (cutoff,),
        ).fetchall()
    old_cids = {str(r["condition_id"]) for r in rows}
    for cid in old_cids:
        market = markets.pop(cid, None)
        base_price_history.pop(cid, None)
        fast_pm_history.pop(cid, None)
        external_open_proxy.pop(cid, None)
        if market:
            for asset in (market.get("up_asset"), market.get("down_asset")):
                last_jump_ms.pop((cid, asset), None)
    for key in list(strategy_state):
        if key[0] in old_cids:
            strategy_state.pop(key, None)

    keep_assets = set()
    for market in markets.values():
        keep_assets.add(str(market.get("up_asset") or ""))
        keep_assets.add(str(market.get("down_asset") or ""))
    keep_assets.discard("")
    for asset in list(books):
        if asset not in keep_assets:
            books.pop(asset, None)
    subscribed_assets.intersection_update(keep_assets)
    return len(old_cids)


async def maintenance_loop():
    last_health_log = 0
    while True:
        try:
            removed = cleanup_memory()
            t = now_ms()
            if t - last_health_log >= 60_000:
                with db() as conn:
                    for venue in ("binance", "bybit", "coinbase"):
                        for symbol in SYMBOLS:
                            h = source_health[venue][symbol]
                            age = t - si(h.get("last_ms")) if h.get("last_ms") else 999999
                            conn.execute("""
                                INSERT INTO source_health_log(
                                    sample_ms,venue,symbol,connected,age_ms,messages,errors,last_error
                                ) VALUES(?,?,?,?,?,?,?,?)
                            """, (
                                t, venue, symbol, 1 if h.get("connected") else 0, age,
                                si(h.get("messages")), si(h.get("errors")), str(h.get("last_error") or ""),
                            ))
                    conn.commit()
                last_health_log = t
            log.info(
                "MEMORY | RSS=%s MB | removed=%d | markets=%d | PMbooks=%d | features=%d",
                f"{current_rss_mb():.1f}" if current_rss_mb() is not None else "?",
                removed, len(markets), len(books), sum(len(x) for x in feature_history.values()),
            )
        except Exception:
            log.exception("Maintenance failed")
        await asyncio.sleep(MEMORY_CLEANUP_INTERVAL)


# ============================================================
# REPORTING
# ============================================================

def csv_bytes(rows):
    rows = list(rows or [])
    if not rows:
        return b""
    first = rows[0]
    fields = list(first.keys()) if hasattr(first, "keys") else list(first)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))
    return buf.getvalue().encode("utf-8-sig")


def strategy_summary(strategy, start_ms=None, end_ms=None):
    name = strategy["name"]
    where = "variant=?"
    args = [name]
    if start_ms is not None and end_ms is not None:
        where += " AND settled_ms>=? AND settled_ms<?"
        args += [start_ms, end_ms]
    with db() as conn:
        results = conn.execute(f"SELECT * FROM market_results WHERE {where}", args).fetchall()
        trade_args = [name]
        trade_where = "variant=?"
        exit_args = [name]
        exit_where = "variant=?"
        if start_ms is not None and end_ms is not None:
            trade_where += " AND trade_ms>=? AND trade_ms<?"
            trade_args += [start_ms, end_ms]
            exit_where += " AND exit_ms>=? AND exit_ms<?"
            exit_args += [start_ms, end_ms]
        trades = conn.execute(f"SELECT * FROM paper_trades WHERE {trade_where}", trade_args).fetchall()
        exits = conn.execute(f"SELECT * FROM paper_exits WHERE {exit_where}", exit_args).fetchall()
    wins = sum(1 for r in results if sf(r["pnl"]) > 0)
    losses = sum(1 for r in results if sf(r["pnl"]) < 0)
    pnl = sum(sf(r["pnl"]) for r in results)
    tp = sum(1 for r in results if str(r["winning_outcome"]) == "TAKE_PROFIT")
    return {
        "symbol": strategy["symbol"], "variant": name, "code": strategy["code"],
        "traded_markets": len(results), "wins": wins, "losses": losses,
        "winrate_pct": 100.0 * wins / (wins + losses) if wins + losses else 0.0,
        "pnl": pnl, "buy_trades": len(trades), "exits": len(exits), "tp_exits": tp,
        "cash_now": paper_cash(name),
    }


def make_report(start_ts, end_ts):
    sm, em = start_ts * 1000, end_ts * 1000
    summaries = [strategy_summary(s, sm, em) for s in STRATEGIES]
    with db() as conn:
        features = conn.execute(
            "SELECT * FROM external_features WHERE sample_ms>=? AND sample_ms<? ORDER BY sample_ms,symbol",
            (sm, em),
        ).fetchall()
        jumps = conn.execute(
            "SELECT * FROM jump_events WHERE jump_ms>=? AND jump_ms<? ORDER BY jump_ms",
            (sm, em),
        ).fetchall()
        signals = conn.execute(
            "SELECT * FROM signal_events WHERE signal_ms>=? AND signal_ms<? ORDER BY signal_ms",
            (sm, em),
        ).fetchall()
        trades = conn.execute(
            "SELECT * FROM paper_trades WHERE trade_ms>=? AND trade_ms<? ORDER BY trade_ms",
            (sm, em),
        ).fetchall()
        exits = conn.execute(
            "SELECT * FROM paper_exits WHERE exit_ms>=? AND exit_ms<? ORDER BY exit_ms",
            (sm, em),
        ).fetchall()
        trials = conn.execute(
            "SELECT * FROM trials WHERE entry_ms>=? AND entry_ms<? ORDER BY entry_ms",
            (sm, em),
        ).fetchall()
        results = conn.execute(
            "SELECT * FROM market_results WHERE settled_ms>=? AND settled_ms<? ORDER BY settled_ms",
            (sm, em),
        ).fetchall()
        health = conn.execute(
            "SELECT * FROM source_health_log WHERE sample_ms>=? AND sample_ms<? ORDER BY sample_ms,venue,symbol",
            (sm, em),
        ).fetchall()

    d1 = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    d2 = datetime.fromtimestamp(end_ts, tz=timezone.utc)
    path = REPORT_DIR / f"prejump_lab_{d1:%Y-%m-%d_%H-%M}_{d2:%H-%M}_UTC.zip"

    lines = [
        "MULTI7 PRE-JUMP LAB",
        "=" * 72,
        f"Version: {VERSION}",
        f"Period UTC: {utc_iso(start_ts)} -> {utc_iso(end_ts)}",
        f"Symbols: {', '.join(SYMBOLS)}",
        f"Feature persistence: {FEATURE_PERSIST_INTERVAL:.2f}s | fast loop {FAST_INTERVAL:.2f}s",
        f"Jump detector: +{JUMP_MOVE:.2f} within {JUMP_WINDOW_SEC:g}s",
        f"PAPER TP: +${TAKE_PROFIT_USDC:.2f} NET",
        "",
        "STRATEGIES",
        "BASE = current SAFE67-style first V2 gate, ENTRY only",
        f"EXT_CONFIRM = BASE + ext score >= {EXT_CONFIRM_SCORE:.2f}, >= {EXT_CONFIRM_MIN_VENUES} same-side venue votes",
        f"EXT_VETO = BASE unless ext score <= {EXT_VETO_SCORE:.2f} with >= {EXT_VETO_MIN_OPPOSING_VENUES} opposing votes",
        f"PRE_JUMP = price {PREJUMP_PRICE_MIN:.2f}..{PREJUMP_PRICE_MAX:.2f}, ext |score| >= {PREJUMP_SCORE:.2f}, >= {PREJUMP_MIN_VENUES} venue votes",
        "",
        f"Detected Polymarket jumps: {len(jumps)}",
        f"Signals: {len(signals)} | PAPER entries: {len(trades)} | TP exits: {len(exits)}",
        "",
    ]
    for symbol in SYMBOLS:
        lines.append(f"[{symbol}]")
        for s in [x for x in summaries if x["symbol"] == symbol]:
            lines.append(
                f"{s['code']:<11} PnL {s['pnl']:+.2f} | W/L {s['wins']}/{s['losses']} "
                f"({s['winrate_pct']:.1f}%) | TP {s['tp_exits']} | cash {s['cash_now']:.2f}"
            )
        lines.append("")

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("report.txt", "\n".join(lines).encode("utf-8"))
        z.writestr("strategy_summary.csv", csv_bytes(summaries))
        z.writestr("external_features.csv", csv_bytes(features))
        z.writestr("jump_events.csv", csv_bytes(jumps))
        z.writestr("signal_events.csv", csv_bytes(signals))
        z.writestr("paper_trades.csv", csv_bytes(trades))
        z.writestr("paper_exits.csv", csv_bytes(exits))
        z.writestr("trials_3_5_10_20s.csv", csv_bytes(trials))
        z.writestr("market_results.csv", csv_bytes(results))
        z.writestr("source_health.csv", csv_bytes(health))
    return path, summaries


async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            return r.status == 200
    except Exception as exc:
        log.warning("Telegram send: %s", exc)
        return False


async def tg_file(path, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        form = aiohttp.FormData()
        form.add_field("chat_id", TELEGRAM_CHAT_ID)
        form.add_field("caption", caption[:1000])
        form.add_field("document", path.read_bytes(), filename=path.name, content_type="application/zip")
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
            data=form,
            timeout=aiohttp.ClientTimeout(total=90),
        ) as r:
            return r.status == 200
    except Exception as exc:
        log.warning("Telegram file: %s", exc)
        return False


async def report_loop():
    saved = si(state_get("last_report_end", "0"))
    if saved <= 0:
        saved = int(datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp())
        state_set("last_report_end", saved)
    last_end = saved

    while True:
        try:
            eligible = ((now_ts() - REPORT_DELAY_SECONDS) // 3600) * 3600
            while last_end < eligible:
                start, end = last_end, last_end + 3600
                path, summaries = make_report(start, end)
                total_pnl = sum(sf(s["pnl"]) for s in summaries)
                with db() as conn:
                    jump_count = si(conn.execute(
                        "SELECT COUNT(*) c FROM jump_events WHERE jump_ms>=? AND jump_ms<?",
                        (start*1000, end*1000),
                    ).fetchone()["c"])
                caption = (
                    f"🧪 PRE-JUMP LAB {utc_iso(start)} → {utc_iso(end)}\n"
                    f"jumps={jump_count} | aggregate PAPER PnL={total_pnl:+.2f}"
                )
                if not await tg_file(path, caption):
                    break
                last_end = end
                state_set("last_report_end", last_end)
                # High-frequency samples are already exported. Keep a 2-minute overlap
                # in SQLite to control disk growth; research events/trials/results remain.
                with db() as conn:
                    conn.execute("DELETE FROM external_features WHERE sample_ms<?", ((end-120)*1000,))
                    conn.execute("DELETE FROM source_health_log WHERE sample_ms<?", ((end-120)*1000,))
                    conn.commit()
        except Exception:
            log.exception("Report loop failed")
        await asyncio.sleep(REPORT_CHECK_INTERVAL)

# ============================================================
# TELEGRAM CONTROL
# ============================================================

def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "📊 STATISTICS"}, {"text": "🌐 SOURCES"}],
            [{"text": "📈 POSITIONS"}, {"text": "📜 TRADES"}],
            [{"text": "🧪 LAB INFO"}],
        ],
        "resize_keyboard": True,
    }


async def tg_send_keyboard(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text, "reply_markup": jd(keyboard())},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            return r.status == 200
    except Exception as exc:
        log.warning("Telegram keyboard: %s", exc)
        return False


async def send_statistics():
    lines = ["📊 PRE-JUMP LAB STATISTICS", f"Trading entries: {'ON' if trading_enabled() else 'OFF'}"]
    for symbol in SYMBOLS:
        lines.append(f"\n[{symbol}]")
        for strategy in STRATEGIES_BY_SYMBOL[symbol]:
            s = strategy_summary(strategy)
            lines.append(
                f"{strategy['code']}: PnL {s['pnl']:+.2f} | W/L {s['wins']}/{s['losses']} "
                f"({s['winrate_pct']:.1f}%) | TP {s['tp_exits']} | cash {s['cash_now']:.2f}"
            )
    with db() as conn:
        jumps = si(conn.execute("SELECT COUNT(*) c FROM jump_events").fetchone()["c"])
        trials = si(conn.execute("SELECT COUNT(*) c FROM trials").fetchone()["c"])
        complete = si(conn.execute("SELECT COUNT(*) c FROM trials WHERE complete_20s=1").fetchone()["c"])
    lines.append(f"\nDetected jumps: {jumps} | trials: {trials} | 20s complete: {complete}")
    await tg_send("\n".join(lines))


async def send_sources():
    lines = ["🌐 EXTERNAL SOURCES"]
    t = now_ms()
    for symbol in SYMBOLS:
        f = latest_feature(symbol) or {}
        lines.append(
            f"\n{symbol}: composite {sf(f.get('ext_score')):+.3f} | "
            f"votes U/D {si(f.get('up_votes'))}/{si(f.get('down_votes'))}"
        )
        for venue in ("binance", "bybit", "coinbase"):
            if venue == "coinbase" and symbol not in COINBASE_PRODUCTS:
                lines.append(f"  {venue}: N/A")
                continue
            h = source_health[venue][symbol]
            age = t - si(h.get("last_ms")) if h.get("last_ms") else 999999
            vf = f.get(venue) or {}
            status = "OK" if h.get("connected") and age <= SOURCE_FRESH_MS else "STALE"
            lines.append(
                f"  {venue}: {status} age={age}ms score={sf(vf.get('score')):+.3f} "
                f"msg={si(h.get('messages'))} err={si(h.get('errors'))}"
            )
    await tg_send("\n".join(lines))


async def send_positions():
    lines = ["📈 OPEN PAPER POSITIONS"]
    found = False
    for strategy in STRATEGIES:
        with db() as conn:
            rows = conn.execute("""
                SELECT DISTINCT t.condition_id,t.outcome
                FROM paper_trades t
                LEFT JOIN market_results r
                  ON r.condition_id=t.condition_id AND r.variant=t.variant
                WHERE t.variant=? AND r.condition_id IS NULL
            """, (strategy["name"],)).fetchall()
        for row in rows:
            pos = position_totals(str(row["condition_id"]), strategy["name"])
            if pos["remaining"] <= 1e-8:
                continue
            found = True
            mark = projected_full_exit(str(row["condition_id"]), strategy["name"])
            mark_txt = f" | exitPnL {mark['total_pnl']:+.2f}" if mark else ""
            lines.append(
                f"{strategy['symbol']} {strategy['code']} {row['outcome']} "
                f"{pos['remaining']:.2f}sh | cost {pos['buy_cost']:.2f}{mark_txt}"
            )
    if not found:
        lines.append("None")
    await tg_send("\n".join(lines))


async def send_trades():
    with db() as conn:
        rows = conn.execute("""
            SELECT trade_ms AS ms,symbol,variant,outcome,'BUY' action,filled_shares,avg_price
            FROM paper_trades
            UNION ALL
            SELECT exit_ms AS ms,symbol,variant,outcome,'SELL_TP' action,filled_shares,avg_price
            FROM paper_exits
            ORDER BY ms DESC LIMIT 30
        """).fetchall()
    lines = ["📜 LAST LAB ACTIONS"]
    for r in rows:
        strategy = STRATEGY_BY_NAME.get(str(r["variant"]))
        code = strategy["code"] if strategy else str(r["variant"])
        dt = datetime.fromtimestamp(sf(r["ms"])/1000.0, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
        lines.append(
            f"{dt} {r['symbol']} {code} {r['action']} {r['outcome']} "
            f"{sf(r['filled_shares']):.2f}sh @ {sf(r['avg_price']):.3f}"
        )
    if not rows:
        lines.append("No trades yet.")
    await tg_send("\n".join(lines))


async def send_lab_info():
    await tg_send(
        "🧪 PRE-JUMP LAB\n"
        "PAPER ONLY — no real orders.\n\n"
        "BASE: current SAFE67 benchmark.\n"
        f"EXT_CONFIRM: BASE + score >= {EXT_CONFIRM_SCORE:.2f}, >= {EXT_CONFIRM_MIN_VENUES} venue votes.\n"
        f"EXT_VETO: blocks BASE when external directional score <= {EXT_VETO_SCORE:.2f} "
        f"with >= {EXT_VETO_MIN_OPPOSING_VENUES} opposing votes.\n"
        f"PRE_JUMP: PM ask {PREJUMP_PRICE_MIN:.2f}–{PREJUMP_PRICE_MAX:.2f}, "
        f"external score >= {PREJUMP_SCORE:.2f}, >= {PREJUMP_MIN_VENUES} venues.\n\n"
        f"TP simulation: +${TAKE_PROFIT_USDC:.2f} NET.\n"
        f"Jump label: +{JUMP_MOVE:.2f} in {JUMP_WINDOW_SEC:g}s.\n"
        "Hourly ZIP contains 500ms features plus 1/3/5/10/20s pre-jump contexts."
    )


async def handle_tg(text):
    cmd = str(text or "").strip().upper()
    if cmd in {"/START", "START", "▶️ START"}:
        state_set("trading_enabled", "1")
        await tg_send("▶️ PRE-JUMP PAPER entries ON. External data collection is always ON.")
    elif cmd in {"/STOP", "STOP", "⏹ STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("⏹ New PAPER entries OFF. Data collection and TP monitoring continue.")
    elif cmd in {"STATISTICS", "/STATS", "📊 STATISTICS"}:
        await send_statistics()
    elif cmd in {"SOURCES", "🌐 SOURCES"}:
        await send_sources()
    elif cmd in {"POSITIONS", "📈 POSITIONS"}:
        await send_positions()
    elif cmd in {"TRADES", "📜 TRADES"}:
        await send_trades()
    elif cmd in {"INFO", "LAB", "🧪 LAB INFO"}:
        await send_lab_info()
    else:
        await send_lab_info()


async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    offset = 0
    await tg_send_keyboard(
        f"🤖 {VERSION} online\n"
        f"Assets: {', '.join(SYMBOLS)}\n"
        f"Strategies: {len(STRATEGIES)} PAPER accounts\n"
        f"Entries: {'ON' if trading_enabled() else 'OFF'}\n"
        f"Sources: Binance={'ON' if ENABLE_BINANCE else 'OFF'}, "
        f"Bybit={'ON' if ENABLE_BYBIT else 'OFF'}, Coinbase={'ON' if ENABLE_COINBASE else 'OFF'}\n"
        "Hourly research ZIP enabled."
    )
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as r:
                data = await r.json()
            for update in data.get("result", []):
                offset = max(offset, si(update.get("update_id")) + 1)
                msg = update.get("message") or {}
                if str((msg.get("chat") or {}).get("id", "")) != str(TELEGRAM_CHAT_ID):
                    continue
                if msg.get("text"):
                    await handle_tg(msg["text"])
        except Exception as exc:
            log.warning("Telegram polling: %s", exc)
            await asyncio.sleep(2)


# ============================================================
# HEALTH / MAIN
# ============================================================

async def health(request):
    source_status = {}
    t = now_ms()
    for venue in ("binance", "bybit", "coinbase"):
        source_status[venue] = {}
        for symbol in SYMBOLS:
            h = source_health[venue][symbol]
            age = t - si(h.get("last_ms")) if h.get("last_ms") else None
            source_status[venue][symbol] = {
                "connected": bool(h.get("connected")),
                "age_ms": age,
                "messages": si(h.get("messages")),
                "errors": si(h.get("errors")),
                "last_error": str(h.get("last_error") or ""),
            }
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "paper_only": True,
        "trading_entries": trading_enabled(),
        "symbols": SYMBOLS,
        "strategies": STRATEGY_CODES,
        "take_profit_usdc_net": TAKE_PROFIT_USDC,
        "fast_interval": FAST_INTERVAL,
        "feature_persist_interval": FEATURE_PERSIST_INTERVAL,
        "sources": source_status,
        "markets_tracked": len(markets),
        "polymarket_books": len(books),
        "rss_mb": current_rss_mb(),
        "time_utc": utc_iso(),
    })


async def web_server():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server on :%d", PORT)


async def main():
    global session
    init_db()
    session = aiohttp.ClientSession(headers={
        "User-Agent": f"PolymarketPreJumpLab/{VERSION}",
        "Accept": "application/json",
    })

    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(polymarket_ws_loop()),
        asyncio.create_task(fast_lab_loop()),
        asyncio.create_task(base_strategy_loop()),
        asyncio.create_task(resolution_fallback_loop()),
        asyncio.create_task(report_loop()),
        asyncio.create_task(telegram_loop()),
        asyncio.create_task(maintenance_loop()),
    ]
    if ENABLE_BINANCE:
        tasks += [asyncio.create_task(binance_symbol_loop(symbol)) for symbol in SYMBOLS]
    if ENABLE_BYBIT:
        tasks += [asyncio.create_task(bybit_symbol_loop(symbol)) for symbol in SYMBOLS]
    if ENABLE_COINBASE and COINBASE_PRODUCTS:
        tasks.append(asyncio.create_task(coinbase_loop()))

    log.info(
        "%s started | symbols=%s | accounts=%d | fast=%.2fs | persist=%.2fs | TP=%.2f | entries=%s",
        VERSION, ",".join(SYMBOLS), len(STRATEGIES), FAST_INTERVAL,
        FEATURE_PERSIST_INTERVAL, TAKE_PROFIT_USDC, "ON" if trading_enabled() else "OFF",
    )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if session:
            await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
