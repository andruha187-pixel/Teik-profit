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
from dataclasses import replace
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Optional

import aiohttp
from aiohttp import web
import websockets
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
# MULTI7 PRE-JUMP — PAPER + LIVE
# v20.11 adds local-only per-token create/sign prewarm for upcoming LIVE markets.
# ============================================================
# Signal copied from the forward-tested PRE-JUMP lab:
#   external composite score >= runtime threshold (default 0.40)
#   >= 2 same-direction fresh venue votes
#   Polymarket ask 0.52..0.66
#   1-second Polymarket ask momentum -0.01..+0.05
#   elapsed 1..160 seconds
# One ENTRY per token/5-minute market. No DCA. No stop-loss.
# Whole-position NET take-profit is configurable (default +$0.60).
# ============================================================

VERSION = "20.11-multi7-prejump-live-presign-prewarm"

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
    result = []
    for item in raw.split(","):
        symbol = item.strip().upper()
        if symbol in ASSET_CONFIG and symbol not in result:
            result.append(symbol)
    return result or list(ASSET_CONFIG)


SYMBOLS = _configured_symbols()
TRADE_SYMBOLS = list(SYMBOLS)

# PRE-JUMP decision cadence / Polymarket filters.
FAST_INTERVAL = max(0.10, float(os.getenv("FAST_INTERVAL", "0.10")))
DECISION_INTERVAL = FAST_INTERVAL  # compatibility with older helper messages
TRADE_WINDOW_SECONDS = float(os.getenv("PREJUMP_MAX_ELAPSED", "160"))
SOURCE_FRESH_MS = int(os.getenv("SOURCE_FRESH_MS", "2500"))
EXT_VOTE_THRESHOLD = float(os.getenv("EXT_VOTE_THRESHOLD", "0.25"))

PREJUMP_PRICE_MIN = float(os.getenv("PREJUMP_PRICE_MIN", "0.52"))
PREJUMP_PRICE_MAX = float(os.getenv("PREJUMP_PRICE_MAX", "0.66"))
PREJUMP_SCORE_MIN = 0.40
PREJUMP_SCORE_MAX = 1.00
try:
    PREJUMP_SCORE_ENV = float(os.getenv("PREJUMP_SCORE", "0.40").replace(",", "."))
except Exception:
    PREJUMP_SCORE_ENV = 0.40
PREJUMP_SCORE_ENV = max(PREJUMP_SCORE_MIN, min(PREJUMP_SCORE_MAX, PREJUMP_SCORE_ENV))
PREJUMP_SCORE_RUNTIME = PREJUMP_SCORE_ENV
PREJUMP_MIN_VENUES = int(os.getenv("PREJUMP_MIN_VENUES", "2"))
PREJUMP_MIN_ELAPSED = float(os.getenv("PREJUMP_MIN_ELAPSED", "1"))
PREJUMP_MAX_ELAPSED = float(os.getenv("PREJUMP_MAX_ELAPSED", "160"))
PREJUMP_PM_MOM_MIN = float(os.getenv("PREJUMP_PM_MOM_MIN", "-0.01"))
PREJUMP_PM_MOM_MAX = float(os.getenv("PREJUMP_PM_MOM_MAX", "0.05"))
PREJUMP_REQUIRE_BINANCE_BYBIT = os.getenv(
    "PREJUMP_REQUIRE_BINANCE_BYBIT", "0"
).strip().lower() in {"1", "true", "yes", "on"}

# BTC-only first-signal quality filter. IMPORTANT: this is intentionally applied
# *after* the normal PRE-JUMP 0.40 family has produced its first otherwise-valid
# candidate (including PM ask/momentum). If that first BTC candidate does not meet
# these stricter quality limits, the BTC market is skipped permanently. This
# preserves the historical filter that was tested on the accumulated PAPER data;
# simply raising the global PREJUMP_SCORE would create a different trade set by
# allowing a later 0.43+ signal in the same market.
try:
    BTC_PREJUMP_SCORE = float(os.getenv("BTC_PREJUMP_SCORE", "0.43").replace(",", "."))
except Exception:
    BTC_PREJUMP_SCORE = 0.43
BTC_PREJUMP_SCORE = max(PREJUMP_SCORE_MIN, min(PREJUMP_SCORE_MAX, BTC_PREJUMP_SCORE))
try:
    BTC_PREJUMP_PRICE_MAX = float(os.getenv("BTC_PREJUMP_PRICE_MAX", "0.55").replace(",", "."))
except Exception:
    BTC_PREJUMP_PRICE_MAX = 0.55
BTC_PREJUMP_PRICE_MAX = max(PREJUMP_PRICE_MIN, min(PREJUMP_PRICE_MAX, BTC_PREJUMP_PRICE_MAX))

# Token-specific FIRST-base-signal quality filters derived from the accumulated
# PAPER report set. They do NOT wait for a later, nicer signal in the same
# 5-minute market: once the normal PRE-JUMP candidate exists, the relevant
# token filter either accepts it immediately or permanently closes that market's
# entry gate. Defaults reproduce the forward-test candidates selected from the
# historical analysis; every value remains explicit/configurable in Coolify.
def _safe_float_env(name, default):
    try:
        return float(os.getenv(name, str(default)).replace(",", "."))
    except Exception:
        return float(default)

SOL_PREJUMP_PM_MOM_MAX = _safe_float_env("SOL_PREJUMP_PM_MOM_MAX", 0.02)
SOL_PREJUMP_PM_MOM_MAX = max(PREJUMP_PM_MOM_MIN, min(PREJUMP_PM_MOM_MAX, SOL_PREJUMP_PM_MOM_MAX))

XRP_PREJUMP_SCORE = _safe_float_env("XRP_PREJUMP_SCORE", 0.41)
XRP_PREJUMP_SCORE = max(PREJUMP_SCORE_MIN, min(PREJUMP_SCORE_MAX, XRP_PREJUMP_SCORE))

BNB_PREJUMP_PM_MOM_MIN = _safe_float_env("BNB_PREJUMP_PM_MOM_MIN", 0.01)
BNB_PREJUMP_PM_MOM_MIN = max(PREJUMP_PM_MOM_MIN, min(PREJUMP_PM_MOM_MAX, BNB_PREJUMP_PM_MOM_MIN))

DOGE_PREJUMP_MAX_SPREAD = max(0.0, _safe_float_env("DOGE_PREJUMP_MAX_SPREAD", 0.02))

# ETH FIRST-base-signal filter validated on the accumulated PAPER set plus the
# subsequent six-hour forward batch. The first otherwise-valid base PRE-JUMP
# candidate must satisfy BOTH limits; a failure permanently skips that 5-minute
# ETH market. HYPE intentionally remains unfiltered.
ETH_PREJUMP_SCORE = _safe_float_env("ETH_PREJUMP_SCORE", 0.455)
ETH_PREJUMP_SCORE = max(PREJUMP_SCORE_MIN, min(PREJUMP_SCORE_MAX, ETH_PREJUMP_SCORE))

ETH_PREJUMP_PM_MOM_MAX = _safe_float_env("ETH_PREJUMP_PM_MOM_MAX", 0.02)
ETH_PREJUMP_PM_MOM_MAX = max(PREJUMP_PM_MOM_MIN, min(PREJUMP_PM_MOM_MAX, ETH_PREJUMP_PM_MOM_MAX))

# Runtime load controls: 100ms fallback scorer + event-driven LIVE entry, TP every 750ms.
EVENT_DRIVEN_LIVE_ENTRY = os.getenv("EVENT_DRIVEN_LIVE_ENTRY", "1").strip().lower() in {"1", "true", "yes", "on"}
EVENT_DRIVEN_MIN_INTERVAL_MS = max(0, min(100, int(os.getenv("EVENT_DRIVEN_MIN_INTERVAL_MS", "5"))))
TP_CHECK_INTERVAL = max(0.25, float(os.getenv("TP_CHECK_INTERVAL", "0.75")))
TRAJECTORY_INTERVAL = max(1.0, float(os.getenv("TRAJECTORY_INTERVAL", "3.0")))

# Execution / accounting.
ENTRY_ORDER_SIZE = float(os.getenv("ENTRY_ORDER_SIZE", "5"))
DCA_ORDER_SIZE = 0.0
PAPER_START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "500"))
MIN_FREE_CASH = float(os.getenv("MIN_FREE_CASH", "5"))
CRYPTO_FEE_RATE = float(os.getenv("CRYPTO_FEE_RATE", "0.07"))
DISCOVERY_INTERVAL = float(os.getenv("DISCOVERY_INTERVAL", "10"))
MAX_BOOK_AGE_MS = int(os.getenv("MAX_BOOK_AGE_MS", "1000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.08"))
MAX_PRICE = float(os.getenv("MAX_PRICE", "0.95"))

MEMORY_CLEANUP_INTERVAL = int(os.getenv("MEMORY_CLEANUP_INTERVAL", "60"))
MEMORY_KEEP_RESOLVED_SEC = int(os.getenv("MEMORY_KEEP_RESOLVED_SEC", "900"))
WS_MAX_CONNECTION_AGE_SEC = int(os.getenv("WS_MAX_CONNECTION_AGE_SEC", "900"))
MEMORY_LOG_INTERVAL = int(os.getenv("MEMORY_LOG_INTERVAL", "300"))


def _take_profit_from_env():
    raw = os.getenv("TAKE_PROFIT_USDC", "0.60").strip()
    if raw.upper() in {"OFF", "NONE", "DISABLED"}:
        return None
    try:
        value = float(raw.replace(",", "."))
    except (TypeError, ValueError):
        return 0.60
    return value if value > 0 else None


TAKE_PROFIT_USDC = _take_profit_from_env()
TAKE_PROFIT_RUNTIME_USDC = TAKE_PROFIT_USDC

# LIVE safety gates. A real order needs BOTH master=1 and a confirmed token mode=LIVE.
LIVE_MASTER_ENABLE = os.getenv("LIVE_MASTER_ENABLE", "0").strip().lower() in {"1", "true", "yes", "on"}
ALLOW_MULTI_LIVE_PER_TOKEN = False  # one PRE-JUMP strategy per token in this build
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
POLYMARKET_WALLET_ADDRESS = (
    os.getenv("POLYMARKET_WALLET_ADDRESS", "").strip()
    or os.getenv("POLYMARKET_DEPOSIT_WALLET", "").strip()
)
POLYMARKET_RELAYER_API_KEY = os.getenv("POLYMARKET_RELAYER_API_KEY", "").strip()
POLYMARKET_RELAYER_API_KEY_ADDRESS = os.getenv("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()
LIVE_MAX_SHARES_PER_ORDER = float(os.getenv("LIVE_MAX_SHARES_PER_ORDER", "1000"))
LIVE_MIN_SHARES = float(os.getenv("LIVE_MIN_SHARES", "0.01"))

# LIVE ENTRY execution tolerance. PRE-JUMP moves fast, so the visible ask can
# disappear between signal evaluation and FAK submission. v20.11 sends the first
# FAK from the already-validated WS book and retries deterministic NO_MATCH
# immediately from the latest WS book; REST retry is opt-in only.
LIVE_ENTRY_MAX_SLIPPAGE = max(0.0, float(os.getenv("LIVE_ENTRY_MAX_SLIPPAGE", "0.05")))
LIVE_ENTRY_NO_MATCH_RETRIES = max(0, min(2, int(os.getenv("LIVE_ENTRY_NO_MATCH_RETRIES", "1"))))
LIVE_ENTRY_RETRY_DELAY_MS = max(0, min(1000, int(os.getenv("LIVE_ENTRY_RETRY_DELAY_MS", "0"))))
LIVE_ENTRY_RETRY_FORCE_REST = os.getenv(
    "LIVE_ENTRY_RETRY_FORCE_REST", "0"
).strip().lower() in {"1", "true", "yes", "on"}
# Legacy compatibility only. It never forces REST before the first PRE-JUMP FAK.
LIVE_ENTRY_FORCE_REST_BOOK = False
# Safe/read-only authenticated transport keepalive. This never creates an order.
LIVE_PREWARM_ENABLE = os.getenv("LIVE_PREWARM_ENABLE", "1").strip().lower() in {"1", "true", "yes", "on"}
LIVE_PREWARM_INTERVAL_SEC = max(10.0, min(300.0, float(os.getenv("LIVE_PREWARM_INTERVAL_SEC", "30"))))
LIVE_PREWARM_LEAD_SEC = max(1.0, min(30.0, float(os.getenv("LIVE_PREWARM_LEAD_SEC", "3"))))
# v20.11: warm the *actual local order build/sign path* for the next 5-minute
# market. create_limit_order() is called for both outcome tokens, but the signed
# object is discarded and post_order() is NEVER called by prewarm. This is meant
# to move lazy token/market metadata and signer initialization out of the first
# latency-sensitive PRE-JUMP FAK.
LIVE_PRESIGN_PREWARM_ENABLE = os.getenv(
    "LIVE_PRESIGN_PREWARM_ENABLE", "1"
).strip().lower() in {"1", "true", "yes", "on"}
LIVE_PRESIGN_PREWARM_LEAD_SEC = max(3.0, min(60.0, float(os.getenv("LIVE_PRESIGN_PREWARM_LEAD_SEC", "12"))))
LIVE_PRESIGN_PREWARM_SIZE = min(LIVE_MAX_SHARES_PER_ORDER, max(5.0, float(os.getenv("LIVE_PRESIGN_PREWARM_SIZE", "5"))))
LIVE_PRICE_TICK_FALLBACK = max(0.0001, float(os.getenv("LIVE_PRICE_TICK_FALLBACK", "0.01")))

# LIVE TP settlement/CTF balance propagation guard. A successful BUY can be
# acknowledged by the matching engine slightly before the newly acquired outcome
# shares become available to a subsequent SELL. Do not fire TP immediately after
# a fill; if CLOB deterministically rejects a TP SELL for balance/allowance, keep
# the position tracked and retry on later TP cycles instead of poisoning it as
# AMBIGUOUS.
LIVE_TP_MIN_HOLD_MS = max(0, min(10000, int(os.getenv("LIVE_TP_MIN_HOLD_MS", "2000"))))
LIVE_TP_BALANCE_RETRY_DELAY_MS = max(250, min(10000, int(os.getenv("LIVE_TP_BALANCE_RETRY_DELAY_MS", "1000"))))

# External public feeds — exact scoring family used by the PRE-JUMP lab.
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
        sym, product = sym.strip().upper(), product.strip().upper()
        if sym in SYMBOLS and product:
            out[sym] = product
    return out


COINBASE_PRODUCTS = _parse_coinbase_products()
COINBASE_PRODUCT_TO_SYMBOL = {v: k for k, v in COINBASE_PRODUCTS.items()}
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com/public/stream")
BYBIT_WS = os.getenv("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear")
COINBASE_WS = os.getenv("COINBASE_WS", "wss://advanced-trade-ws.coinbase.com")

# Compatibility constants for retained generic accounting helpers.
ENTRY_MOVE = 0.0
LOOKBACK_TICKS = 2
CONSENSUS_WINDOW_SEC = 10.0
CONSENSUS_MIN_OTHER_TOKENS = 2


def _strategy_set(symbol):
    return [{
        "symbol": symbol,
        "code": "PJ",
        "name": f"{symbol}_PRE_JUMP",
        "short": f"{symbol} / PRE-JUMP",
        "max_buys_side": 1,
        "dca_enabled": False,
        "consensus_enabled": False,
        "stop_loss_price": None,
    }]


STRATEGIES = [s for symbol in SYMBOLS for s in _strategy_set(symbol)]
STRATEGIES_BY_SYMBOL = {symbol: [s for s in STRATEGIES if s["symbol"] == symbol] for symbol in SYMBOLS}
STRATEGY_BY_NAME = {x["name"]: x for x in STRATEGIES}

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
MARKET_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

DATA_DIR = Path(os.getenv("DATA_DIR", "/var/data"))
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    probe = DATA_DIR / ".write_test"
    probe.write_text("ok", encoding="utf-8")
    probe.unlink()
except Exception:
    DATA_DIR = Path("./data")
    DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "prejump_multi7_paper_live.db"
REPORT_DIR = DATA_DIR / "prejump_live_reports_DISABLED"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("prejump-paper-live")

session: Optional[aiohttp.ClientSession] = None

# Polymarket market/book state.
books = {}
markets = {}
subscribed_assets = set()
ws_send_queue: asyncio.Queue = asyncio.Queue()
price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=100)))  # compatibility
fast_pm_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=512)))
strategy_state = {}
settle_lock = asyncio.Lock()
last_tp_check = defaultdict(float)
last_trajectory_sample = defaultdict(float)

# External microstructure state. All high-frequency data stays in memory only.
venue_books = defaultdict(lambda: defaultdict(lambda: {"bids": {}, "asks": {}, "received_ms": 0}))
venue_last_price = defaultdict(dict)
venue_trade_buckets = defaultdict(lambda: defaultdict(lambda: deque(maxlen=600)))
venue_liq_buckets = defaultdict(lambda: defaultdict(lambda: deque(maxlen=120)))
venue_sample_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=256)))
feature_history = defaultdict(lambda: deque(maxlen=512))
# Event-driven LIVE-entry coordination. The timer loop remains as a 100ms fallback
# and keeps PAPER behavior unchanged. A per-market lock prevents timer/event races.
external_eval_events = {symbol: asyncio.Event() for symbol in SYMBOLS}
external_eval_received_ms = defaultdict(int)
external_eval_last_run = defaultdict(float)
prejump_eval_locks = defaultdict(asyncio.Lock)
live_entry_latency = {}
source_health = defaultdict(lambda: defaultdict(lambda: {
    "connected": False, "last_ms": 0, "messages": 0, "errors": 0, "last_error": "",
}))


# ============================================================
# HELPERS
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



# ============================================================
# DATABASE / PERSISTENT PAPER ACCOUNTS
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
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            signal_type TEXT,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            signal_type TEXT,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_cost REAL,
            fee REAL,
            total_cost REAL,
            book_age_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS dca_events (
            condition_id TEXT,
            variant TEXT,
            armed_ms INTEGER,
            armed_elapsed_sec REAL,
            armed_ask REAL,
            filled_ms INTEGER,
            filled_elapsed_sec REAL,
            filled_ask REAL,
            filled_momentum REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS v2_votes (
            condition_id TEXT PRIMARY KEY,
            symbol TEXT,
            decision_ms INTEGER,
            asset TEXT,
            outcome TEXT,
            ask REAL,
            reference_ask REAL,
            momentum REAL,
            elapsed_sec REAL
        );

        CREATE TABLE IF NOT EXISTS consensus_events (
            condition_id TEXT,
            variant TEXT,
            decision_ms INTEGER,
            target_symbol TEXT,
            target_outcome TEXT,
            target_ask REAL,
            target_momentum REAL,
            window_sec REAL,
            required_count INTEGER,
            confirm_count INTEGER,
            confirm_symbols_json TEXT,
            confirm_ages_ms_json TEXT,
            passed INTEGER,
            reason TEXT,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS stop_events (
            condition_id TEXT,
            variant TEXT,
            trigger_ms INTEGER,
            trigger_bid REAL,
            stop_price REAL,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS paper_exits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exit_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            asset TEXT,
            outcome TEXT,
            reason TEXT,
            trigger_price REAL,
            requested_shares REAL,
            filled_shares REAL,
            avg_price REAL,
            gross_proceeds REAL,
            fee REAL,
            net_proceeds REAL,
            book_age_ms INTEGER,
            book_received_ms INTEGER,
            fills_json TEXT
        );

        CREATE TABLE IF NOT EXISTS live_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            submitted_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            symbol TEXT,
            asset TEXT,
            outcome TEXT,
            action TEXT,
            reason TEXT,
            requested_shares REAL,
            limit_price REAL,
            order_id TEXT,
            status TEXT,
            filled_shares REAL,
            avg_price REAL,
            gross_amount REAL,
            fee_estimate REAL,
            net_or_total REAL,
            trade_ids_json TEXT,
            response_json TEXT,
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS market_results (
            condition_id TEXT,
            variant TEXT,
            winning_asset TEXT,
            winning_outcome TEXT,
            buy_cost REAL,
            exit_proceeds REAL,
            payout REAL,
            pnl REAL,
            buy_trades INTEGER,
            exit_trades INTEGER,
            up_bought REAL,
            down_bought REAL,
            up_exited REAL,
            down_exited REAL,
            stopped_out INTEGER,
            execution_mode TEXT,
            settled_ms INTEGER,
            PRIMARY KEY(condition_id, variant)
        );

        CREATE TABLE IF NOT EXISTS position_trajectory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sample_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            elapsed_sec REAL,
            primary_asset TEXT,
            primary_outcome TEXT,
            opposite_asset TEXT,
            bought_shares REAL,
            exited_shares REAL,
            remaining_shares REAL,
            gross_entry_cost REAL,
            entry_fees REAL,
            total_buy_cost REAL,
            exit_net_so_far REAL,
            primary_best_bid REAL,
            primary_best_ask REAL,
            opposite_best_bid REAL,
            opposite_best_ask REAL,
            mark_filled_shares REAL,
            mark_avg_price REAL,
            mark_fee REAL,
            mark_net_proceeds REAL,
            unrealized_total_pnl REAL,
            mfe_pnl REAL,
            mae_pnl REAL,
            stop_triggered INTEGER
        );

        CREATE TABLE IF NOT EXISTS prejump_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_ms INTEGER,
            condition_id TEXT,
            variant TEXT,
            symbol TEXT,
            asset TEXT,
            outcome TEXT,
            pm_ask REAL,
            pm_bid REAL,
            pm_momentum REAL,
            ext_score REAL,
            same_votes INTEGER,
            opposing_votes INTEGER,
            fresh_venues INTEGER,
            elapsed_sec REAL,
            threshold REAL,
            features_json TEXT
        );

        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_gate_ms ON gate_decisions(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_trades_ms ON paper_trades(trade_ms);
        CREATE INDEX IF NOT EXISTS idx_exits_ms ON paper_exits(exit_ms);
        CREATE INDEX IF NOT EXISTS idx_dca_armed_ms ON dca_events(armed_ms);
        CREATE INDEX IF NOT EXISTS idx_consensus_ms ON consensus_events(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_v2_votes_ms ON v2_votes(decision_ms);
        CREATE INDEX IF NOT EXISTS idx_results_ms ON market_results(settled_ms);
        CREATE INDEX IF NOT EXISTS idx_live_orders_ms ON live_orders(submitted_ms);
        CREATE INDEX IF NOT EXISTS idx_live_orders_cond ON live_orders(condition_id,variant,submitted_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_ms ON position_trajectory(sample_ms);
        CREATE INDEX IF NOT EXISTS idx_traj_cond ON position_trajectory(condition_id,variant,sample_ms);
        CREATE INDEX IF NOT EXISTS idx_prejump_signal_ms ON prejump_signals(signal_ms);
        """)

        defaults = {
            "trading_enabled": "0",
            "take_profit_usdc": (
                "OFF" if TAKE_PROFIT_USDC is None else f"{TAKE_PROFIT_USDC:.10g}"
            ),
            "prejump_score": f"{PREJUMP_SCORE_ENV:.10g}",
        }
        for strategy in STRATEGIES:
            name = strategy["name"]
            defaults[f"mode:{name}"] = "PAPER"
            defaults[f"entry_shares:{name}"] = str(ENTRY_ORDER_SIZE)
            defaults[f"dca_shares:{name}"] = str(DCA_ORDER_SIZE if strategy.get("dca_enabled") else 0)
            defaults[f"paper_initial:{name}"] = str(PAPER_START_BALANCE)
            defaults[f"paper_cash:{name}"] = str(PAPER_START_BALANCE)
        for key, value in defaults.items():
            if conn.execute("SELECT 1 FROM state WHERE key=?", (key,)).fetchone() is None:
                conn.execute("INSERT INTO state(key,value) VALUES(?,?)", (key, value))

        # v20.3 migration: older builds incorrectly marked an explicit CLOB
        # TAKE_PROFIT balance/allowance rejection as AMBIGUOUS. Such a rejection
        # means the SELL was not accepted, so it is safe to unpoison the action
        # and let the new balance-propagation retry logic continue. This also
        # repairs positions already affected before the upgrade.
        repaired = conn.execute("""
            UPDATE live_orders
               SET status='REJECTED_BALANCE_ALLOWANCE'
             WHERE action='SELL' AND reason='TAKE_PROFIT'
               AND status IN ('AMBIGUOUS','DELAYED_AMBIGUOUS')
               AND (
                    lower(COALESCE(error,'')) LIKE '%not enough balance%'
                 OR lower(COALESCE(error,'')) LIKE '%insufficient balance%'
               )
        """).rowcount
        conn.commit()
        if repaired:
            log.warning("v20.3 repaired %d old TP balance rejection(s) from AMBIGUOUS", repaired)

    load_take_profit_usdc()
    load_prejump_score()


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


def _parse_take_profit_state(raw, fallback):
    if raw is None:
        return fallback

    text = str(raw).strip()
    if text.upper() in {"OFF", "NONE", "DISABLED"}:
        return None

    try:
        value = float(text.replace(",", "."))
    except (TypeError, ValueError):
        return fallback

    if not math.isfinite(value):
        return fallback
    return value if value > 0 else None


def load_take_profit_usdc():
    """Hydrate the Telegram-configurable TP once from persistent SQLite state."""
    global TAKE_PROFIT_RUNTIME_USDC
    raw = state_get(
        "take_profit_usdc",
        "OFF" if TAKE_PROFIT_USDC is None else f"{TAKE_PROFIT_USDC:.10g}",
    )
    TAKE_PROFIT_RUNTIME_USDC = _parse_take_profit_state(raw, TAKE_PROFIT_USDC)
    return TAKE_PROFIT_RUNTIME_USDC


def take_profit_usdc():
    """Current global whole-position NET TP; hot-loop read is in-memory."""
    return TAKE_PROFIT_RUNTIME_USDC


def set_take_profit_usdc(value):
    """Persist and immediately apply global TP. None/0 means OFF."""
    global TAKE_PROFIT_RUNTIME_USDC

    if value is None or sf(value, 0.0) <= 0:
        state_set("take_profit_usdc", "OFF")
        TAKE_PROFIT_RUNTIME_USDC = None
        return None

    value = float(value)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("take-profit must be a finite positive number")

    state_set("take_profit_usdc", f"{value:.10g}")
    TAKE_PROFIT_RUNTIME_USDC = value
    return value


def format_take_profit(value):
    return "OFF" if value is None else f"${float(value):.2f} NET"



def load_prejump_score():
    global PREJUMP_SCORE_RUNTIME
    raw = state_get("prejump_score", f"{PREJUMP_SCORE_ENV:.10g}")
    try:
        value = float(str(raw).replace(",", "."))
    except Exception:
        value = PREJUMP_SCORE_ENV
    PREJUMP_SCORE_RUNTIME = max(PREJUMP_SCORE_MIN, min(PREJUMP_SCORE_MAX, value))
    return PREJUMP_SCORE_RUNTIME


def prejump_score():
    return PREJUMP_SCORE_RUNTIME


def set_prejump_score(value):
    global PREJUMP_SCORE_RUNTIME
    value = float(value)
    if not math.isfinite(value) or value < PREJUMP_SCORE_MIN - 1e-12 or value > PREJUMP_SCORE_MAX + 1e-12:
        raise ValueError(f"score must be {PREJUMP_SCORE_MIN:.2f}..{PREJUMP_SCORE_MAX:.2f}")
    value = round(value, 4)
    state_set("prejump_score", f"{value:.4f}")
    PREJUMP_SCORE_RUNTIME = value
    return value


def format_prejump_score(value=None):
    if value is None:
        value = prejump_score()
    return f"{float(value):.2f}"


def safe_first_signal_filter_lines():
    return [
        f"BTC SAFE: score >= {BTC_PREJUMP_SCORE:.3f} | ask <= {BTC_PREJUMP_PRICE_MAX:.2f}",
        f"SOL SAFE: PM mom <= {SOL_PREJUMP_PM_MOM_MAX:+.3f}",
        f"XRP SAFE: score >= {XRP_PREJUMP_SCORE:.3f}",
        f"BNB SAFE: PM mom >= {BNB_PREJUMP_PM_MOM_MIN:+.3f}",
        f"DOGE SAFE: spread <= {DOGE_PREJUMP_MAX_SPREAD:.3f}",
        f"ETH SAFE: score >= {ETH_PREJUMP_SCORE:.3f} | PM mom <= {ETH_PREJUMP_PM_MOM_MAX:+.3f}",
        "HYPE SAFE: none (base PRE-JUMP only)",
    ]


def safe_first_signal_filter_text():
    return "\n".join(safe_first_signal_filter_lines())


def paper_cash(strategy_name):
    return sf(state_get(f"paper_cash:{strategy_name}", PAPER_START_BALANCE), PAPER_START_BALANCE)


def paper_initial(strategy_name):
    return sf(state_get(f"paper_initial:{strategy_name}", PAPER_START_BALANCE), PAPER_START_BALANCE)


def set_paper_cash(strategy_name, value):
    state_set(f"paper_cash:{strategy_name}", round(float(value), 10))


def trading_enabled():
    return state_get("trading_enabled", "0") == "1"


def strategy_mode(strategy_name):
    mode = str(state_get(f"mode:{strategy_name}", "PAPER") or "PAPER").upper()
    return mode if mode in {"PAPER", "LIVE", "OFF"} else "PAPER"


def entry_shares(strategy_or_name):
    name = strategy_or_name["name"] if isinstance(strategy_or_name, dict) else str(strategy_or_name)
    return max(0.0, sf(state_get(f"entry_shares:{name}", ENTRY_ORDER_SIZE), ENTRY_ORDER_SIZE))


def dca_shares(strategy_or_name):
    strategy = strategy_or_name if isinstance(strategy_or_name, dict) else STRATEGY_BY_NAME.get(str(strategy_or_name))
    name = strategy["name"] if strategy else str(strategy_or_name)
    default = DCA_ORDER_SIZE if strategy and strategy.get("dca_enabled") else 0.0
    return max(0.0, sf(state_get(f"dca_shares:{name}", default), default))


def requested_shares(variant, signal_type):
    return dca_shares(variant) if str(signal_type).upper() == "DCA" else entry_shares(variant)


def _valid_user_shares(value):
    x = sf(value, -1.0)
    return LIVE_MIN_SHARES <= x <= LIVE_MAX_SHARES_PER_ORDER


live_client = None
live_client_ready = False
live_client_error = ""
live_order_locks = defaultdict(asyncio.Lock)
live_tp_retry_after_ms = defaultdict(int)
live_tp_balance_reject_count = defaultdict(int)
live_prewarm_lock = asyncio.Lock()
live_prewarm_last_ms = 0
live_presign_prewarm_lock = asyncio.Lock()
live_presign_warmed_assets = set()
live_presign_warm_ms = {}
live_presign_warm_failures = defaultdict(int)


async def init_live_client():
    global live_client, live_client_ready, live_client_error
    live_client_ready = False
    live_client_error = ""

    if AsyncSecureClient is None:
        live_client_error = "polymarket-client is not installed"
        log.warning("LIVE disabled: %s", live_client_error)
        return False

    if not POLYMARKET_PRIVATE_KEY:
        live_client_error = "POLYMARKET_PRIVATE_KEY not configured"
        log.info("LIVE signer not configured; PAPER remains available")
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
            "LIVE wallet ready | wallet=%s | signer=%s | wallet_type=%s | master=%s",
            str(getattr(live_client, "wallet", POLYMARKET_WALLET_ADDRESS)),
            str(getattr(live_client, "signer", "")),
            str(getattr(live_client, "wallet_type", "")),
            "ON" if LIVE_MASTER_ENABLE else "OFF",
        )
        return True
    except Exception as e:
        live_client = None
        live_client_error = f"{type(e).__name__}: {e}"
        log.exception("LIVE wallet initialization failed")
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
            log.exception("LIVE client close failed")


async def live_collateral_balance():
    if not live_client_ready or live_client is None:
        return None
    try:
        b = await live_client.get_balance_allowance(asset_type="COLLATERAL")
        return sf(getattr(b, "balance", 0)) / 1_000_000.0
    except Exception:
        log.exception("LIVE balance read failed")
        return None


async def prewarm_live_transport(reason="periodic"):
    """Warm the authenticated SDK HTTP transport without creating an order.

    A collateral balance/allowance read is safe/read-only and exercises the same
    authenticated client before a latency-sensitive PRE-JUMP FAK. It is skipped
    whenever a LIVE order lock is already active so prewarm can never compete
    with real execution.
    """
    global live_prewarm_last_ms
    if not LIVE_PREWARM_ENABLE or not live_client_ready or live_client is None:
        return False
    if any(lock.locked() for lock in live_order_locks.values()):
        return False
    t = now_ms()
    if t - live_prewarm_last_ms < int(LIVE_PREWARM_INTERVAL_SEC * 1000):
        return False
    async with live_prewarm_lock:
        t = now_ms()
        if t - live_prewarm_last_ms < int(LIVE_PREWARM_INTERVAL_SEC * 1000):
            return False
        if any(lock.locked() for lock in live_order_locks.values()):
            return False
        started = now_ms()
        try:
            await live_client.get_balance_allowance(asset_type="COLLATERAL")
            live_prewarm_last_ms = now_ms()
            log.debug("LIVE PREWARM %s OK | %dms", reason, live_prewarm_last_ms - started)
            return True
        except Exception as e:
            # Prewarm is opportunistic only; never changes LIVE readiness or order state.
            live_prewarm_last_ms = now_ms()
            log.debug("LIVE PREWARM %s skipped/failed | %s", reason, e)
            return False


async def prewarm_live_asset_signer(market, asset, outcome):
    """Exercise the real local create/sign path without ever submitting an order.

    Safety invariant: this function calls create_limit_order() only. It does not
    call post_order(), sdk_post_order_with_allowance_recovery(), or any other
    placement method. The returned signed object is discarded immediately.
    """
    asset = str(asset or "")
    if not asset or not LIVE_PRESIGN_PREWARM_ENABLE:
        return False
    if not live_client_ready or live_client is None:
        return False
    if asset in live_presign_warmed_assets:
        return True
    # Never compete with a real LIVE action. The slot prewarm runs after the
    # previous market's 160s entry window, but this extra guard is fail-safe.
    if any(lock.locked() for lock in live_order_locks.values()):
        return False

    symbol = market_symbol(market)
    variant = strategy_for(symbol)
    if not variant or strategy_mode(variant["name"]) != "LIVE":
        return False

    # Use a boring, valid mid-price and a normal share size. Nothing is posted,
    # so this price has no trading effect; it only forces the SDK to resolve the
    # token/market metadata and execute the same signing path as a real ENTRY.
    price_dec = _normalize_live_limit_price(asset, 0.50, "BUY")
    price_str = format(price_dec, "f")
    size_str = format(Decimal(str(LIVE_PRESIGN_PREWARM_SIZE)), "f")
    started = now_ms()
    try:
        signed = await live_client.create_limit_order(
            token_id=asset,
            price=price_str,
            size=size_str,
            side="BUY",
            post_only=False,
        )
        # Match the real ENTRY's final local conversion too, still without POST.
        _ = replace(signed, order_type="FAK", post_only=False)
        elapsed = max(0, now_ms() - started)
        live_presign_warmed_assets.add(asset)
        live_presign_warm_ms[asset] = elapsed
        log.info(
            "LIVE PRESIGN WARM %s %s | token=%s | build/sign=%dms | LOCAL ONLY / NOT POSTED",
            symbol, outcome, asset[-10:], elapsed,
        )
        return True
    except Exception as e:
        live_presign_warm_failures[asset] += 1
        log.warning(
            "LIVE PRESIGN WARM %s %s failed | token=%s | %s",
            symbol, outcome, asset[-10:], e,
        )
        return False


async def prewarm_live_slot_signers(slot_start):
    """Pre-sign both outcome tokens for LIVE symbols in the upcoming slot."""
    if not LIVE_PRESIGN_PREWARM_ENABLE or not live_client_ready or live_client is None:
        return 0, 0
    # The function is called shortly before the next slot. At that point the
    # previous market is outside PREJUMP_MAX_ELAPSED, so this cannot steal time
    # from a normal new ENTRY. Still skip if a real order is currently active.
    if any(lock.locked() for lock in live_order_locks.values()):
        return 0, 0

    targets = []
    live_symbols = set()
    for symbol in SYMBOLS:
        v = strategy_for(symbol)
        if v and strategy_mode(v["name"]) == "LIVE":
            live_symbols.add(symbol)
    if not live_symbols:
        return 0, 0

    for market in list(markets.values()):
        if si(market.get("start_ts")) != si(slot_start):
            continue
        symbol = market_symbol(market)
        if symbol not in live_symbols:
            continue
        targets.append((market, str(market.get("up_asset") or ""), "Up"))
        targets.append((market, str(market.get("down_asset") or ""), "Down"))

    expected = len(targets)
    if not expected:
        return 0, 0

    warmed = 0
    async with live_presign_prewarm_lock:
        for market, asset, outcome in targets:
            if not asset:
                continue
            if asset in live_presign_warmed_assets:
                warmed += 1
                continue
            # Stop immediately if a real order appeared while warming assets.
            if any(lock.locked() for lock in live_order_locks.values()):
                break
            if await prewarm_live_asset_signer(market, asset, outcome):
                warmed += 1
    return warmed, expected


async def prewarm_symbol_current_or_next(symbol):
    """Best-effort pre-sign when a symbol is switched to LIVE.

    Active-slot warming is intentionally skipped while entries are ON, because
    a background signer warmup must never delay a real PRE-JUMP order.
    """
    if not LIVE_PRESIGN_PREWARM_ENABLE or not live_client_ready or live_client is None:
        return False
    symbol = str(symbol).upper()
    now = time.time()
    candidates = [m for m in markets.values() if market_symbol(m) == symbol and sf(m.get("end_ts")) > now]
    if not candidates:
        return False
    candidates.sort(key=lambda m: (sf(m.get("start_ts")) < now, abs(sf(m.get("start_ts")) - now)))
    for market in candidates:
        start_ts = sf(market.get("start_ts"))
        # Future market is always safe to prewarm. Current market is warmable only
        # when global entries are OFF, so no latency-sensitive real order can race.
        if start_ts <= now and trading_enabled():
            continue
        async with live_presign_prewarm_lock:
            ok1 = await prewarm_live_asset_signer(market, market.get("up_asset"), "Up")
            ok2 = await prewarm_live_asset_signer(market, market.get("down_asset"), "Down")
        return bool(ok1 and ok2)
    return False


async def live_prewarm_loop():
    """Prewarm transport and the actual build/sign path before each 5m slot."""
    last_transport_slot = None
    while True:
        now = time.time()
        next_slot = ((int(now) // 300) + 1) * 300
        until = next_slot - now

        # Start signer warming earlier than the transport ping. Repeated calls in
        # this short lead window are cheap because successfully warmed token ids
        # are cached in live_presign_warmed_assets. If discovery has not found a
        # future market yet, the next loop retries instead of giving up the slot.
        if LIVE_PRESIGN_PREWARM_ENABLE and until <= LIVE_PRESIGN_PREWARM_LEAD_SEC:
            warmed, expected = await prewarm_live_slot_signers(next_slot)
            if expected and warmed == expected:
                log.debug("LIVE PRESIGN SLOT %s ready | %d/%d assets", next_slot, warmed, expected)

        if until <= LIVE_PREWARM_LEAD_SEC and last_transport_slot != next_slot:
            await prewarm_live_transport(f"slot-{next_slot}")
            last_transport_slot = next_slot

        lead = max(LIVE_PREWARM_LEAD_SEC, LIVE_PRESIGN_PREWARM_LEAD_SEC if LIVE_PRESIGN_PREWARM_ENABLE else 0)
        sleep_for = max(0.25, min(2.0, until - lead))
        await asyncio.sleep(sleep_for)


# ============================================================
# HTTP / BOOK
# ============================================================

async def get_json(url, params=None):
    for attempt in range(3):
        try:
            async with session.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                text = await r.text()
                if r.status == 200:
                    return json.loads(text)
                log.warning("HTTP %s %s %s -> %s", r.status, url, params, text[:200])
        except Exception as e:
            log.warning("GET %s failed: %s", url, e)
        await asyncio.sleep(0.3 * (attempt + 1))
    return None


def level_map(rows):
    out = {}
    for row in rows or []:
        if isinstance(row, dict):
            p = sf(row.get("price") if row.get("price") is not None else row.get("price_level"), math.nan)
            q = sf(row.get("size") if row.get("size") is not None else row.get("new_quantity"), 0)
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            p = sf(row[0], math.nan)
            q = sf(row[1], 0)
        else:
            continue
        if not math.isnan(p) and q > 0:
            out[p] = q
    return out


async def _refresh_entry_book_if_needed(asset):
    """Refresh stale/crossed Polymarket book before a real signal is accepted."""
    b = books.get(asset) or {}
    age = now_ms() - si(b.get("received_ms")) if b.get("received_ms") else 999999
    bid = best_bid(asset)
    ask = best_ask(asset)
    crossed = bid is not None and ask is not None and bid >= ask - 1e-12
    if age > MAX_BOOK_AGE_MS or ask is None or crossed:
        await refresh_book(asset)
        b = books.get(asset) or {}
        age = now_ms() - si(b.get("received_ms")) if b.get("received_ms") else 999999
        bid = best_bid(asset)
        ask = best_ask(asset)
        crossed = bid is not None and ask is not None and bid >= ask - 1e-12
    if ask is None or age > MAX_BOOK_AGE_MS or crossed:
        return None, None, age
    return bid, ask, age


def apply_book(asset, payload, source="ws"):
    # Polymarket book snapshots can include the market tick size. Preserve it so
    # signed LIVE order prices are always aligned to the token's actual increment.
    prior = books.get(asset) or {}
    tick_raw = (
        payload.get("tick_size")
        if isinstance(payload, dict) and payload.get("tick_size") is not None
        else payload.get("tickSize") if isinstance(payload, dict) else None
    )
    tick = sf(tick_raw, sf(prior.get("tick_size"), LIVE_PRICE_TICK_FALLBACK))
    if tick <= 0:
        tick = LIVE_PRICE_TICK_FALLBACK
    books[asset] = {
        "bids": level_map(payload.get("bids")),
        "asks": level_map(payload.get("asks")),
        "received_ms": now_ms(),
        "source": source,
        "tick_size": tick,
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
        b = books.setdefault(asset, {
            "bids": {}, "asks": {}, "received_ms": recv, "source": "ws-delta"
        })
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


async def ensure_book(asset):
    b = books.get(asset)
    if b and b.get("asks"):
        age = now_ms() - b["received_ms"]
        if age <= MAX_BOOK_AGE_MS:
            return age
    await refresh_book(asset)
    b = books.get(asset)
    if not b:
        return None
    return now_ms() - b["received_ms"]


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
    """Walk visible bids from best to worst for an executable PAPER exit mark."""
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
# MARKET DISCOVERY
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


def strategies_for_market(market):
    return STRATEGIES_BY_SYMBOL.get(market_symbol(market), [])


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
    expected_prefix = ASSET_CONFIG[symbol]["prefix"] + "-"
    if slug and not slug.lower().startswith(expected_prefix):
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
        "raw": raw,
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

def persist_market(m):
    with db() as conn:
        conn.execute("""
            INSERT INTO discovered_markets(
                condition_id,symbol,question,slug,start_ts,end_ts,up_asset,down_asset,discovered_ms
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id) DO UPDATE SET
                symbol=excluded.symbol, question=excluded.question, slug=excluded.slug,
                start_ts=excluded.start_ts, end_ts=excluded.end_ts,
                up_asset=excluded.up_asset, down_asset=excluded.down_asset
        """, (
            m["condition_id"], market_symbol(m), m["question"], m["slug"],
            m["start_ts"], m["end_ts"], m["up_asset"], m["down_asset"], now_ms(),
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
                    log.info("Discovery %s: market not found for slot %s", symbol, utc_iso(current))
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

def parse_ws(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "ignore")
    if raw in ("", "PING", "PONG"):
        return []
    try:
        x = json.loads(raw)
        return x if isinstance(x, list) else [x]
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


async def ws_loop():
    while True:
        try:
            if not subscribed_assets:
                await asyncio.sleep(1)
                continue

            async with websockets.connect(
                MARKET_WS,
                ping_interval=None,
                close_timeout=5,
                max_size=20_000_000,
            ) as ws:
                await ws.send(jd({
                    "assets_ids": list(subscribed_assets),
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log.info("WS connected | assets=%d", len(subscribed_assets))

                sender = asyncio.create_task(ws_sender(ws))
                ping = asyncio.create_task(ws_ping(ws))
                try:
                    ws_started = time.monotonic()
                    async for raw in ws:
                        if time.monotonic() - ws_started >= WS_MAX_CONNECTION_AGE_SEC:
                            log.info("WS periodic reconnect | active_assets=%d", len(subscribed_assets))
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
        except Exception as e:
            log.warning("WS reconnect: %s", e)
            await asyncio.sleep(1)


# ============================================================
# EXTERNAL MICROSTRUCTURE / PUBLIC VENUE FEEDS
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
    touched = set()
    if not isinstance(msg, dict):
        return touched
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
            touched.add(symbol)
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
                touched.add(symbol)
                # Coinbase side is maker side; taker side is opposite.
                maker_side = str(tr.get("side") or "").upper()
                taker_side = "SELL" if maker_side == "BUY" else "BUY"
                event_dt = parse_iso(tr.get("time"))
                event_ms = int(event_dt.timestamp() * 1000) if event_dt else now_ms()
                update_external_trade("coinbase", symbol, event_ms, tr.get("price"), tr.get("size"), taker_side)
    elif channel == "heartbeats":
        for symbol in COINBASE_PRODUCTS:
            source_message("coinbase", symbol)
    return touched


def trigger_external_entry_eval(symbol):
    """Wake the per-symbol LIVE entry evaluator after an external WS update.

    This is deliberately non-blocking for the exchange feed reader. Multiple bursts
    are coalesced by asyncio.Event; the evaluator can run again immediately if a new
    event arrives while it is busy.
    """
    if not EVENT_DRIVEN_LIVE_ENTRY:
        return
    symbol = str(symbol).upper()
    ev = external_eval_events.get(symbol)
    if ev is None:
        return
    external_eval_received_ms[symbol] = now_ms()
    ev.set()


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
                    trigger_external_entry_eval(symbol)
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
                    trigger_external_entry_eval(symbol)
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
                    for symbol in handle_coinbase_message(msg):
                        trigger_external_entry_eval(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            for symbol in COINBASE_PRODUCTS:
                mark_source("coinbase", symbol, connected=False, error=exc)
            log.warning("Coinbase reconnect: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(30.0, backoff * 1.7)

# ============================================================
# MEMORY / RENDER STABILITY
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


def cleanup_resolved_market_memory():
    cutoff = now_ts() - MEMORY_KEEP_RESOLVED_SEC
    with db() as conn:
        rows = conn.execute(
            "SELECT condition_id FROM discovered_markets WHERE resolved=1 AND end_ts < ?",
            (cutoff,),
        ).fetchall()
    old_cids = {str(r["condition_id"]) for r in rows}
    if not old_cids:
        return 0

    for cid in old_cids:
        markets.pop(cid, None)
        price_history.pop(cid, None)
        fast_pm_history.pop(cid, None)
    for key in list(strategy_state):
        if key[0] in old_cids:
            strategy_state.pop(key, None)

    keep_assets = set()
    for m in markets.values():
        if m.get("up_asset"):
            keep_assets.add(str(m["up_asset"]))
        if m.get("down_asset"):
            keep_assets.add(str(m["down_asset"]))
    for asset in list(books):
        if asset not in keep_assets:
            books.pop(asset, None)
    # v20.11 signer-prewarm bookkeeping follows the same market lifecycle so
    # unique 5-minute token ids cannot accumulate forever in memory.
    live_presign_warmed_assets.intersection_update(keep_assets)
    for asset in list(live_presign_warm_ms):
        if asset not in keep_assets:
            live_presign_warm_ms.pop(asset, None)
            live_presign_warm_failures.pop(asset, None)
    subscribed_assets.intersection_update(keep_assets)
    return len(old_cids)


async def memory_maintenance_loop():
    last_mem_log = 0.0
    while True:
        try:
            removed = cleanup_resolved_market_memory()
            mono = time.monotonic()
            if removed or mono - last_mem_log >= MEMORY_LOG_INTERVAL:
                rss = current_rss_mb()
                log.info(
                    "MEMORY | RSS=%s | removed_markets=%d | markets=%d | books=%d | state=%d | assets=%d",
                    f"{rss:.1f} MB" if rss is not None else "n/a",
                    removed, len(markets), len(books), len(strategy_state), len(subscribed_assets),
                )
                last_mem_log = mono
        except Exception:
            log.exception("Memory maintenance failed")
        await asyncio.sleep(MEMORY_CLEANUP_INTERVAL)




# ============================================================
# MULTI7 A/B/C/E SAFE67 STRATEGY ENGINE
# ============================================================

def get_variant_state(condition, variant):
    key = (condition, variant["name"])
    if key in strategy_state:
        return strategy_state[key]

    st = {
        "buys": defaultdict(int),
        "last_buy": {},
        "started_sides": set(),
        "primary_asset": None,
        "gate_decided": False,
        "gate_passed": False,
        "gate_asset": None,
        "dca_armed": False,
        "dca_filled": False,
        "stopped_out": False,
        "take_profit_closed": False,
    }

    with db() as conn:
        gate = conn.execute(
            "SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if gate:
            st["gate_decided"] = True
            st["gate_passed"] = bool(gate["passed"])
            st["gate_asset"] = str(gate["asset"]) if gate["passed"] else None

        rows = []
        for r in conn.execute(
            "SELECT trade_ms AS ms,asset,avg_price,signal_type,filled_shares "
            "FROM paper_trades WHERE condition_id=? AND variant=? AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchall():
            rows.append(dict(r))
        for r in conn.execute(
            "SELECT submitted_ms AS ms,asset,avg_price,reason AS signal_type,filled_shares "
            "FROM live_orders WHERE condition_id=? AND variant=? AND action='BUY' AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchall():
            rows.append(dict(r))
        rows.sort(key=lambda r: si(r.get("ms")))
        for r in rows:
            asset = str(r["asset"])
            st["buys"][asset] += 1
            st["last_buy"][asset] = sf(r["avg_price"])
            st["started_sides"].add(asset)
            if st["primary_asset"] is None:
                st["primary_asset"] = asset
            if str(r.get("signal_type", "")).upper() == "DCA":
                st["dca_filled"] = True

        dca = conn.execute(
            "SELECT * FROM dca_events WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()
        if dca:
            st["dca_armed"] = True
            st["dca_filled"] = bool(dca["filled_ms"]) or st["dca_filled"]

        # Hydrate TP-closed state from persisted execution rows.
        paper_bought = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM paper_trades "
            "WHERE condition_id=? AND variant=?",
            (condition, variant["name"]),
        ).fetchone()["x"])
        live_bought = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM live_orders "
            "WHERE condition_id=? AND variant=? AND action='BUY' AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchone()["x"])
        paper_tp = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM paper_exits "
            "WHERE condition_id=? AND variant=? AND reason='TAKE_PROFIT'",
            (condition, variant["name"]),
        ).fetchone()["x"])
        live_tp = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM live_orders "
            "WHERE condition_id=? AND variant=? AND action='SELL' "
            "AND reason='TAKE_PROFIT' AND filled_shares>0",
            (condition, variant["name"]),
        ).fetchone()["x"])
        total_bought = paper_bought + live_bought
        if total_bought > 0 and paper_tp + live_tp >= total_bought - 1e-8:
            st["take_profit_closed"] = True

    strategy_state[key] = st
    return st

def momentum_for(condition, asset, lookback):
    h = price_history[condition][asset]
    if len(h) <= lookback:
        return None, None
    current = h[-1][1]
    ref = h[-1 - lookback][1]
    return current - ref, ref


def store_gate_decision(condition, variant, asset, outcome, ask, ref, mom, elapsed, passed, reason):
    with db() as conn:
        conn.execute("""
            INSERT INTO gate_decisions(
                condition_id,variant,decision_ms,elapsed_sec,asset,outcome,ask,
                reference_ask,momentum,passed,reason
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (
            condition, variant["name"], now_ms(), elapsed, asset, outcome, ask,
            ref, mom, 1 if passed else 0, reason,
        ))
        conn.commit()


def store_signal(condition, variant, asset, outcome, ask, ref, mom, signal_type, elapsed):
    with db() as conn:
        conn.execute("""
            INSERT INTO signals(
                signal_ms,condition_id,variant,asset,outcome,ask,
                reference_ask,momentum,signal_type,elapsed_sec
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, variant["name"], asset, outcome,
            ask, ref, mom, signal_type, elapsed,
        ))
        conn.commit()


def arm_dca(condition, variant, ask, elapsed):
    st = get_variant_state(condition, variant)
    if st.get("dca_armed"):
        return False
    with db() as conn:
        conn.execute("""
            INSERT INTO dca_events(condition_id,variant,armed_ms,armed_elapsed_sec,armed_ask)
            VALUES(?,?,?,?,?)
            ON CONFLICT(condition_id,variant) DO NOTHING
        """, (condition, variant["name"], now_ms(), elapsed, ask))
        conn.commit()
    st["dca_armed"] = True
    return True


def mark_dca_filled(condition, variant, ask, mom, elapsed):
    st = get_variant_state(condition, variant)
    with db() as conn:
        conn.execute("""
            UPDATE dca_events
            SET filled_ms=?,filled_elapsed_sec=?,filled_ask=?,filled_momentum=?
            WHERE condition_id=? AND variant=?
        """, (now_ms(), elapsed, ask, mom, condition, variant["name"]))
        conn.commit()
    st["dca_filled"] = True


def trim_fills_to_budget(fills, max_total):
    if max_total <= 0:
        return [], 0.0
    out, spent, shares = [], 0.0, 0.0
    for price, qty in fills:
        price = sf(price)
        qty = sf(qty)
        if price <= 0 or qty <= 0:
            continue
        per_share = price + fee_usdc(1.0, price)
        affordable = max(0.0, (max_total - spent) / per_share)
        take = min(qty, affordable)
        if take <= 1e-9:
            break
        out.append((price, take))
        spent += price * take + fee_usdc(take, price)
        shares += take
        if spent >= max_total - 1e-8:
            break
    return out, shares


def stop_triggered(condition, variant_name):
    with db() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM stop_events WHERE condition_id=? AND variant=?",
            (condition, variant_name),
        ).fetchone())


def _response_json(obj):
    try:
        if hasattr(obj, "model_dump"):
            return jd(obj.model_dump(mode="json"))
        if hasattr(obj, "__dict__"):
            return jd({k: str(v) for k, v in vars(obj).items()})
        return jd({"repr": repr(obj)})
    except Exception:
        return jd({"repr": repr(obj)})


def position_totals(condition, variant_name):
    """Aggregate one strategy/market across PAPER or LIVE execution.

    A mode change is blocked while a position is open, so one market/variant
    normally has exactly one execution mode.
    """
    with db() as conn:
        p_buys = conn.execute(
            "SELECT * FROM paper_trades WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
        p_exits = conn.execute(
            "SELECT * FROM paper_exits WHERE condition_id=? AND variant=? ORDER BY id",
            (condition, variant_name),
        ).fetchall()
        l_rows = conn.execute(
            """SELECT * FROM live_orders
               WHERE condition_id=? AND variant=? AND filled_shares>0
               ORDER BY submitted_ms,id""",
            (condition, variant_name),
        ).fetchall()

    buys = []
    exits = []

    for r in p_buys:
        buys.append({
            "_ms": si(r["trade_ms"]),
            "asset": str(r["asset"]),
            "outcome": str(r["outcome"]),
            "signal_type": str(r["signal_type"]),
            "filled_shares": sf(r["filled_shares"]),
            "avg_price": sf(r["avg_price"]),
            "gross_cost": sf(r["gross_cost"]),
            "fee": sf(r["fee"]),
            "total_cost": sf(r["total_cost"]),
            "mode": "PAPER",
        })
    for r in p_exits:
        exits.append({
            "_ms": si(r["exit_ms"]),
            "asset": str(r["asset"]),
            "outcome": str(r["outcome"]),
            "reason": str(r["reason"]),
            "filled_shares": sf(r["filled_shares"]),
            "avg_price": sf(r["avg_price"]),
            "gross_proceeds": sf(r["gross_proceeds"]),
            "fee": sf(r["fee"]),
            "net_proceeds": sf(r["net_proceeds"]),
            "mode": "PAPER",
        })

    for r in l_rows:
        action = str(r["action"]).upper()
        if action == "BUY":
            buys.append({
                "_ms": si(r["submitted_ms"]),
                "asset": str(r["asset"]),
                "outcome": str(r["outcome"]),
                "signal_type": str(r["reason"]),
                "filled_shares": sf(r["filled_shares"]),
                "avg_price": sf(r["avg_price"]),
                "gross_cost": sf(r["gross_amount"]),
                "fee": sf(r["fee_estimate"]),
                "total_cost": sf(r["net_or_total"]),
                "mode": "LIVE",
            })
        elif action == "SELL":
            exits.append({
                "_ms": si(r["submitted_ms"]),
                "asset": str(r["asset"]),
                "outcome": str(r["outcome"]),
                "reason": str(r["reason"]),
                "filled_shares": sf(r["filled_shares"]),
                "avg_price": sf(r["avg_price"]),
                "gross_proceeds": sf(r["gross_amount"]),
                "fee": sf(r["fee_estimate"]),
                "net_proceeds": sf(r["net_or_total"]),
                "mode": "LIVE",
            })

    buys.sort(key=lambda r: r["_ms"])
    exits.sort(key=lambda r: r["_ms"])

    bought = sum(sf(r["filled_shares"]) for r in buys)
    exited = sum(sf(r["filled_shares"]) for r in exits)
    buy_cost = sum(sf(r["total_cost"]) for r in buys)
    exit_net = sum(sf(r["net_proceeds"]) for r in exits)
    primary_asset = str(buys[0]["asset"]) if buys else None
    primary_outcome = str(buys[0]["outcome"]) if buys else None
    dca_trades = sum(1 for r in buys if str(r["signal_type"]).upper() == "DCA")

    modes = {str(r.get("mode", "")).upper() for r in buys + exits}
    execution_mode = "LIVE" if "LIVE" in modes else ("PAPER" if "PAPER" in modes else None)

    return {
        "buys": buys,
        "exits": exits,
        "bought": bought,
        "exited": exited,
        "remaining": max(0.0, bought - exited),
        "buy_cost": buy_cost,
        "exit_net": exit_net,
        "primary_asset": primary_asset,
        "primary_outcome": primary_outcome,
        "dca_trades": dca_trades,
        "has_dca": dca_trades > 0,
        "execution_mode": execution_mode,
    }


async def ensure_sell_book(asset):
    """Refresh only when the bid side used for TP is missing/stale."""
    b = books.get(asset)
    if b and b.get("bids"):
        age = now_ms() - si(b.get("received_ms"))
        if age <= MAX_BOOK_AGE_MS:
            return age
    await refresh_book(asset)
    b = books.get(asset)
    if not b or not b.get("bids"):
        return None
    return now_ms() - si(b.get("received_ms"))


def projected_full_exit(condition, variant_name):
    """Executable whole-position NET PnL if all remaining shares sell now."""
    pos = position_totals(condition, variant_name)
    remaining = pos["remaining"]
    asset = pos["primary_asset"]
    if not asset or remaining <= 1e-8:
        return None

    fills, filled = simulate_sell(asset, remaining)
    if filled < remaining - 1e-8:
        return None

    gross = sum(sf(px) * sf(q) for px, q in fills)
    fee = sum(fee_usdc(sf(q), sf(px)) for px, q in fills)
    net = gross - fee
    avg = gross / filled if filled > 1e-9 else None
    total_pnl = pos["exit_net"] + net - pos["buy_cost"]

    return {
        "pos": pos,
        "asset": asset,
        "remaining": remaining,
        "fills": fills,
        "filled": filled,
        "gross": gross,
        "fee": fee,
        "net": net,
        "avg": avg,
        "total_pnl": total_pnl,
    }


def take_profit_latched(condition, variant_name):
    """True after a LIVE TP has actually sold at least some shares."""
    with db() as conn:
        return bool(conn.execute(
            """SELECT 1 FROM live_orders
               WHERE condition_id=? AND variant=? AND action='SELL'
                 AND reason='TAKE_PROFIT' AND filled_shares>0
               LIMIT 1""",
            (condition, variant_name),
        ).fetchone())


def finalize_take_profit_result(condition, variant, market):
    """Persist a fully flattened TP result so later settlement cannot pay it twice."""
    name = variant["name"]
    pos = position_totals(condition, name)
    if not pos["buys"] or pos["remaining"] > 1e-8:
        return False

    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (condition, name),
        ).fetchone():
            return False

        up_asset = str(market["up_asset"])
        down_asset = str(market["down_asset"])
        buys = pos["buys"]
        exits = pos["exits"]

        up_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == up_asset)
        down_bought = sum(sf(r["filled_shares"]) for r in buys if str(r["asset"]) == down_asset)
        up_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == up_asset)
        down_exited = sum(sf(r["filled_shares"]) for r in exits if str(r["asset"]) == down_asset)
        pnl = pos["exit_net"] - pos["buy_cost"]
        mode = pos.get("execution_mode") or strategy_mode(name)

        conn.execute("""
            INSERT INTO market_results(
                condition_id,variant,winning_asset,winning_outcome,buy_cost,
                exit_proceeds,payout,pnl,buy_trades,exit_trades,up_bought,
                down_bought,up_exited,down_exited,stopped_out,execution_mode,settled_ms
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            condition, name, "", "TAKE_PROFIT", pos["buy_cost"],
            pos["exit_net"], 0.0, pnl, len(buys), len(exits),
            up_bought, down_bought, up_exited, down_exited, 0, mode, now_ms(),
        ))
        conn.commit()

    st = get_variant_state(condition, variant)
    st["take_profit_closed"] = True
    return True


async def execute_paper_take_profit(market, variant, candidate, age, target):
    cid = market["condition_id"]
    name = variant["name"]
    pos = position_totals(cid, name)
    if pos["execution_mode"] != "PAPER" or pos["remaining"] <= 1e-8:
        return False
    if abs(pos["remaining"] - candidate["remaining"]) > 1e-7:
        return False

    asset = candidate["asset"]
    outcome = pos["primary_outcome"] or (
        "Up" if asset == str(market["up_asset"]) else "Down"
    )
    trigger_bid = best_bid(asset)
    cash_before = paper_cash(name)
    cash_after = cash_before + candidate["net"]
    book_received_ms = si((books.get(asset) or {}).get("received_ms"))

    with db() as conn:
        # Duplicate/race guard.
        bought = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM paper_trades "
            "WHERE condition_id=? AND variant=?",
            (cid, name),
        ).fetchone()["x"])
        exited = sf(conn.execute(
            "SELECT COALESCE(SUM(filled_shares),0) x FROM paper_exits "
            "WHERE condition_id=? AND variant=?",
            (cid, name),
        ).fetchone()["x"])
        remaining_now = max(0.0, bought - exited)
        if remaining_now <= 1e-8 or abs(remaining_now - candidate["remaining"]) > 1e-7:
            return False

        conn.execute("""
            INSERT INTO paper_exits(
                exit_ms,condition_id,variant,asset,outcome,reason,trigger_price,
                requested_shares,filled_shares,avg_price,gross_proceeds,fee,
                net_proceeds,book_age_ms,book_received_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), cid, name, asset, outcome, "TAKE_PROFIT", trigger_bid,
            candidate["remaining"], candidate["filled"], candidate["avg"],
            candidate["gross"], candidate["fee"], candidate["net"], age,
            book_received_ms,
            jd([{"price": px, "shares": q} for px, q in candidate["fills"]]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{name}", str(cash_after)),
        )
        conn.commit()

    finalize_take_profit_result(cid, variant, market)
    log.info(
        "🟢 PAPER TP %-25s | %.4fsh @ %.4f | NET PnL=%+.4f target=%+.4f",
        name, candidate["filled"], candidate["avg"],
        candidate["total_pnl"], target,
    )
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        await tg_send(
            f"💵 PAPER TAKE PROFIT {variant['symbol']} {variant['code']}\n"
            f"{candidate['filled']:.4f}sh @ {candidate['avg']:.4f}\n"
            f"NET PnL: ${candidate['total_pnl']:+.2f} | target ${target:.2f}"
        )
    return True


async def maybe_take_profit(market, variant, elapsed):
    """Monitor PAPER/LIVE positions and close at current whole-position NET TP."""
    cid = market["condition_id"]
    name = variant["name"]
    st = get_variant_state(cid, variant)
    if not st["started_sides"] or st.get("take_profit_closed"):
        return False

    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (cid, name),
        ).fetchone():
            return False

    pos = position_totals(cid, name)
    if not pos["buys"] or pos["remaining"] <= 1e-8:
        return False

    mode = pos.get("execution_mode") or strategy_mode(name)
    latched = mode == "LIVE" and take_profit_latched(cid, name)
    target = take_profit_usdc()

    # Telegram may turn TP OFF or raise/lower it immediately. The one exception:
    # once a real LIVE TP has already partially filled, liquidation is latched
    # and must continue toward flat even if the setting is later changed/OFF.
    if target is None and not latched:
        return False

    candidate = projected_full_exit(cid, name)
    if not latched:
        if candidate is None or candidate["total_pnl"] + 1e-12 < target:
            return False

        # Threshold touch must survive a fresh bid-book check.
        await ensure_sell_book(candidate["asset"])
        candidate = projected_full_exit(cid, name)
        if candidate is None or candidate["total_pnl"] + 1e-12 < target:
            return False
    else:
        # A real TP that partially filled is a committed liquidation attempt.
        # Continue toward flat; ambiguous submissions remain fail-closed inside
        # execute_live_fak and will not be duplicated automatically.
        await ensure_sell_book(pos["primary_asset"])
        candidate = projected_full_exit(cid, name)

    if mode == "PAPER":
        if candidate is None:
            return False
        age = now_ms() - si((books.get(candidate["asset"]) or {}).get("received_ms"))
        return await execute_paper_take_profit(
            market, variant, candidate, age, target
        )

    if mode != "LIVE":
        return False

    # For the initial trigger require full visible depth. After a partial LIVE
    # fill, the latch may continue with whatever visible depth is currently
    # available; FAK itself caps execution to the visible snapshot.
    remaining = position_totals(cid, name)["remaining"]
    if remaining <= 1e-8:
        finalize_take_profit_result(cid, variant, market)
        return True

    if not latched and candidate is None:
        return False

    outcome = pos["primary_outcome"] or (
        "Up" if pos["primary_asset"] == str(market["up_asset"]) else "Down"
    )

    # A BUY can be reported matched before the newly acquired conditional-token
    # balance is visible to a SELL. Give it a short propagation window. This only
    # delays LIVE TP; PAPER behavior is unchanged.
    live_buy_ms = [si(x.get("_ms")) for x in pos["buys"] if str(x.get("mode", "")).upper() == "LIVE"]
    if live_buy_ms:
        age_since_buy = now_ms() - max(live_buy_ms)
        if age_since_buy < LIVE_TP_MIN_HOLD_MS:
            return False

    retry_key = (cid, name, "TAKE_PROFIT")
    if now_ms() < si(live_tp_retry_after_ms.get(retry_key)):
        return False

    result = await execute_live_fak(
        cid, variant, pos["primary_asset"], outcome,
        "TAKE_PROFIT", "SELL", remaining,
    )

    filled = sf(result.get("filled"))
    if filled <= 1e-9:
        if result.get("status") == "REJECTED_BALANCE_ALLOWANCE":
            live_tp_balance_reject_count[retry_key] += 1
            live_tp_retry_after_ms[retry_key] = now_ms() + LIVE_TP_BALANCE_RETRY_DELAY_MS
            count = live_tp_balance_reject_count[retry_key]
            # One Telegram message is enough; subsequent retries stay quiet but
            # remain visible in logs/SQLite.
            if count == 1 and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"⏳ LIVE TP WAITING FOR BALANCE {variant['symbol']} {variant['code']}\n"
                    f"Bought shares are tracked, but CLOB has not exposed enough "
                    f"outcome-token balance/allowance for SELL yet.\n"
                    f"Retrying every ~{LIVE_TP_BALANCE_RETRY_DELAY_MS/1000:.2f}s; "
                    "this rejection is NOT ambiguous and does not block TP."
                )
        return False

    # Any successful SELL clears temporary balance-sync backoff state.
    live_tp_retry_after_ms.pop(retry_key, None)
    live_tp_balance_reject_count.pop(retry_key, None)

    after = position_totals(cid, name)
    if after["remaining"] <= 1e-8:
        finalize_take_profit_result(cid, variant, market)
        pnl = after["exit_net"] - after["buy_cost"]
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            target_text = format_take_profit(target)
            await tg_send(
                f"💵 LIVE TAKE PROFIT COMPLETE {variant['symbol']} {variant['code']}\n"
                f"NET PnL estimate: ${pnl:+.2f} | target {target_text}\n"
                "Position fully closed."
            )
    else:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            await tg_send(
                f"⚠️ LIVE TP PARTIAL {variant['symbol']} {variant['code']}\n"
                f"Filled {filled:.4f}sh; remaining {after['remaining']:.4f}sh.\n"
                "TP is latched; bot will continue liquidation unless submission becomes ambiguous."
            )
    return True


def is_definite_fak_no_match_error(exc):
    """True only for the CLOB's deterministic zero-fill FAK no-match rejection.

    This is materially different from a timeout/503/transport failure: the
    message itself says the FAK found nothing to match and was killed, so there
    is no possibly-live resting order and no unknown fill to duplicate.
    """
    text = f"{type(exc).__name__}: {exc}".lower()
    return (
        "no orders found to match with fak order" in text
        and "fak" in text
    )


def is_definite_balance_allowance_rejection(exc):
    """True for an explicit CLOB rejection before a SELL is accepted.

    Typical message after a just-matched BUY:
      RequestRejectedError: not enough balance / allowance: ... balance: 0 ...

    This is not ambiguous: CLOB rejected the order, so there is no unknown SELL
    fill to duplicate. The common short-lived case is outcome-token balance cache
    propagation immediately after BUY.
    """
    text = f"{type(exc).__name__}: {exc}".lower()
    return (
        "not enough balance" in text
        or "not enough balance / allowance" in text
        or "insufficient balance" in text
    )


def live_action_ambiguous(condition, variant_name, action, reason):
    with db() as conn:
        return bool(conn.execute(
            """SELECT 1 FROM live_orders
               WHERE condition_id=? AND variant=? AND action=? AND reason=?
                 AND status IN ('AMBIGUOUS','DELAYED_AMBIGUOUS')
               LIMIT 1""",
            (condition, variant_name, str(action).upper(), str(reason)),
        ).fetchone())


def _visible_fak_limit(asset, wanted, side, max_buy_price=None, min_sell_price=None):
    """Worst visible price needed for up to `wanted` shares, optionally capped.

    BUY can be capped by max_buy_price so a fast PRE-JUMP order never chases
    beyond the configured slippage budget. SELL keeps the previous behaviour.
    """
    b = books.get(asset) or {}
    side = str(side).upper()
    levels = b.get("asks") if side == "BUY" else b.get("bids")
    if not levels:
        return None, 0.0

    remaining = float(wanted)
    filled_visible = 0.0
    worst = None
    prices = sorted(levels) if side == "BUY" else sorted(levels, reverse=True)
    for px in prices:
        px_f = sf(px)
        if side == "BUY" and max_buy_price is not None and px_f > sf(max_buy_price) + 1e-12:
            break
        if side == "SELL" and min_sell_price is not None and px_f < sf(min_sell_price) - 1e-12:
            break
        q = max(0.0, sf(levels[px]))
        take = min(q, remaining)
        if take > 0:
            worst = px_f
            filled_visible += take
            remaining -= take
        if remaining <= 1e-9:
            break
    return worst, filled_visible


def _entry_price_cap(reference_ask):
    """Maximum LIVE BUY price allowed from the original accepted signal ask."""
    if reference_ask is None:
        return None
    return min(PREJUMP_PRICE_MAX, sf(reference_ask) + LIVE_ENTRY_MAX_SLIPPAGE)


def _asset_tick_size(asset):
    """Return a positive Decimal tick size for a Polymarket outcome token."""
    b = books.get(asset) or {}
    raw = sf(b.get("tick_size"), LIVE_PRICE_TICK_FALLBACK)
    if raw <= 0:
        raw = LIVE_PRICE_TICK_FALLBACK
    return Decimal(str(raw))


def _normalize_live_limit_price(asset, price, side):
    """Align an order limit to the token tick without violating the price guard.

    BUY is floored so normalization can never exceed the configured slippage cap.
    SELL is ceiled so normalization can never sell below the intended minimum.
    """
    tick = _asset_tick_size(asset)
    px = Decimal(str(price))
    if tick <= 0:
        tick = Decimal(str(LIVE_PRICE_TICK_FALLBACK))
    rounding = ROUND_FLOOR if str(side).upper() == "BUY" else ROUND_CEILING
    units = (px / tick).to_integral_value(rounding=rounding)
    normalized = units * tick
    # normalize() can produce exponent notation; the caller formats with 'f'.
    return normalized.normalize()


async def _live_entry_retry_valid(condition, variant, asset, outcome, reference_ask):
    """Revalidate a deterministic NO_MATCH before one optional fast retry.

    The retry is allowed only while the same PRE-JUMP direction is still strong,
    the market is still inside its entry time window, and a fresh REST ask is
    within both the strategy price band and the original signal slippage cap.
    """
    market = markets.get(condition)
    if not market:
        return False, "market_missing"

    elapsed = time.time() - sf(market.get("start_ts"))
    if not (PREJUMP_MIN_ELAPSED <= elapsed <= PREJUMP_MAX_ELAPSED):
        return False, "outside_entry_window"

    feature = latest_feature(variant["symbol"])
    if not feature or si(feature.get("fresh_venues")) < PREJUMP_MIN_VENUES:
        return False, "external_sources_not_fresh"

    ext_score = sf(feature.get("ext_score"))
    current_outcome = "Up" if ext_score > 0 else "Down"
    if current_outcome != outcome:
        return False, "direction_changed"

    directional = directional_external(feature, outcome)
    threshold = prejump_score()
    if directional["score"] + 1e-12 < threshold:
        return False, "score_faded"
    if directional["same_votes"] < PREJUMP_MIN_VENUES:
        return False, "venue_votes_faded"
    if PREJUMP_REQUIRE_BINANCE_BYBIT and not (
        directional["binance_same"] and directional["bybit_same"]
    ):
        return False, "binance_bybit_confirmation_faded"

    # Price/book validity is checked again inside execute_live_fak immediately
    # before submission using a forced REST snapshot. Avoid doing two sequential
    # REST calls here because PRE-JUMP latency matters.
    return True, "ok"


def _entry_latency_line(condition, variant_name):
    root = live_entry_latency.get((condition, variant_name)) or {}
    bits = []
    path = str(root.get("path") or "")
    if path:
        bits.append(f"path={path}")
    if root.get("event_to_signal_ms") is not None:
        bits.append(f"event→signal {si(root.get('event_to_signal_ms'))}ms")

    attempts = root.get("attempts") or []
    for a in attempts[-2:]:
        label = str(a.get("label") or "attempt").upper()
        retry = label.startswith("RETRY")
        detail = []
        if a.get("presign_warmed") is not None:
            warm_txt = "yes" if a.get("presign_warmed") else "no"
            if a.get("presign_warm_ms") is not None:
                warm_txt += f"/{si(a.get('presign_warm_ms'))}ms"
            detail.append(f"warm {warm_txt}")
        if retry:
            if a.get("attempt_to_book_ms") is not None:
                detail.append(f"start→book {si(a.get('attempt_to_book_ms'))}ms")
        elif a.get("signal_to_book_ms") is not None:
            detail.append(f"sig→book {si(a.get('signal_to_book_ms'))}ms")
        if a.get("build_sign_ms") is not None:
            detail.append(f"build/sign {si(a.get('build_sign_ms'))}ms")
        if retry:
            if a.get("attempt_to_submit_ms") is not None:
                detail.append(f"start→submit {si(a.get('attempt_to_submit_ms'))}ms")
        elif a.get("signal_to_submit_ms") is not None:
            detail.append(f"sig→submit {si(a.get('signal_to_submit_ms'))}ms")
        if a.get("api_response_ms") is not None:
            detail.append(f"API {si(a.get('api_response_ms'))}ms")
        if retry:
            if a.get("attempt_to_response_ms") is not None:
                detail.append(f"start→resp {si(a.get('attempt_to_response_ms'))}ms")
        elif a.get("signal_to_response_ms") is not None:
            detail.append(f"sig→resp {si(a.get('signal_to_response_ms'))}ms")
        if detail:
            bits.append(f"{label}[" + ", ".join(detail) + "]")
    return " | ".join(bits)


async def execute_live_fak(
    condition, variant, asset, outcome, reason, action, wanted,
    reference_price=None, force_rest=False, signal_detected_ms=None,
    event_received_ms=None, evaluation_path=None, attempt_label=None,
    ws_only=False,
):
    """Place an exact-share FAK order using a freshly checked visible book.

    PRE-JUMP BUY may use reference_price + LIVE_ENTRY_MAX_SLIPPAGE as a hard
    ceiling. Deterministic FAK NO_MATCH is returned as retry-safe; transport or
    unknown submission failures remain fail-closed/AMBIGUOUS.
    """
    name = variant["name"]
    symbol = variant["symbol"]
    action = str(action).upper()
    wanted = sf(wanted)

    latency_ctx = None
    if action == "BUY" and reason == "ENTRY":
        signal_detected_ms = si(signal_detected_ms, now_ms()) or now_ms()
        event_received_ms = si(event_received_ms, 0)
        key = (condition, name)
        root = live_entry_latency.get(key)
        if not root or str(attempt_label or "first").lower() == "first":
            root = {
                "path": str(evaluation_path or "timer"),
                "signal_ms": signal_detected_ms,
                "event_received_ms": event_received_ms or None,
                "event_to_signal_ms": (max(0, signal_detected_ms - event_received_ms) if event_received_ms else None),
                "attempts": [],
            }
            live_entry_latency[key] = root
        latency_ctx = {
            "label": str(attempt_label or ("retry" if force_rest else "first")),
            "attempt_start_ms": now_ms(),
            "signal_ms": si(root.get("signal_ms"), signal_detected_ms),
            "presign_warmed": str(asset) in live_presign_warmed_assets,
            "presign_warm_ms": live_presign_warm_ms.get(str(asset)),
        }
        root.setdefault("attempts", []).append(latency_ctx)

    if not LIVE_MASTER_ENABLE:
        log.error("LIVE BLOCK %s: LIVE_MASTER_ENABLE=0", name)
        return {"ok": False, "filled": 0.0, "error": "LIVE_MASTER_ENABLE=0"}
    if not live_client_ready or live_client is None:
        log.error("LIVE BLOCK %s: wallet client not ready (%s)", name, live_client_error)
        return {"ok": False, "filled": 0.0, "error": live_client_error or "wallet_not_ready"}
    if not _valid_user_shares(wanted):
        return {"ok": False, "filled": 0.0, "error": f"invalid shares {wanted}"}

    lock = live_order_locks[(condition, name)]
    async with lock:
        # If a network/API exception happened after submission, we cannot know
        # safely whether the exchange accepted the previous order. Never retry
        # the same action automatically: missing a trade is safer than duplicating
        # a real-money order. The block expires naturally with this 5-minute market.
        if live_action_ambiguous(condition, name, action, reason):
            log.error("LIVE FAIL-CLOSED %s %s %s: previous submission is ambiguous", name, action, reason)
            return {"ok": False, "filled": 0.0, "error": "previous_submission_ambiguous"}

        # ULTRA-LOW-LATENCY PRE-JUMP execution:
        # - FIRST uses the already-validated WS book from signal acceptance;
        # - deterministic NO_MATCH retry uses the latest WS book immediately;
        # - REST is only used when explicitly requested for rollback/diagnostics.
        # A stale WS retry can safely no-match because FAK + hard cap cannot chase.
        if action == "BUY" and reason == "ENTRY":
            if force_rest:
                await refresh_book(asset)
            elif ws_only:
                # Never add an HTTP RTT on the immediate deterministic retry.
                pass
            else:
                # Signal acceptance already validated freshness. Keep the fallback
                # for direct/internal calls where that guarantee may be absent.
                b0 = books.get(asset) or {}
                age0 = now_ms() - si(b0.get("received_ms")) if b0.get("received_ms") else 999999
                if not b0.get("asks") or age0 > MAX_BOOK_AGE_MS:
                    await ensure_book(asset)
        else:
            await ensure_book(asset)

        if latency_ctx is not None:
            latency_ctx["book_ready_ms"] = now_ms()
            latency_ctx["signal_to_book_ms"] = max(0, latency_ctx["book_ready_ms"] - latency_ctx["signal_ms"])
            latency_ctx["attempt_to_book_ms"] = max(0, latency_ctx["book_ready_ms"] - latency_ctx["attempt_start_ms"])

        if action == "BUY" and reason == "ENTRY":
            best_now = best_ask(asset)
            cap = _entry_price_cap(reference_price)
            if best_now is None:
                return {"ok": False, "filled": 0.0, "error": "no_visible_liquidity"}
            if best_now < PREJUMP_PRICE_MIN - 1e-12 or best_now > PREJUMP_PRICE_MAX + 1e-12:
                return {
                    "ok": False, "filled": 0.0,
                    "error": f"entry_price_outside_band:{best_now:.4f}",
                }
            if cap is not None and best_now > cap + 1e-12:
                return {
                    "ok": False, "filled": 0.0,
                    "error": f"entry_price_beyond_slippage:{best_now:.4f}>{cap:.4f}",
                }
            _worst, visible = _visible_fak_limit(
                asset, wanted, action, max_buy_price=cap
            )
            if visible <= 1e-9:
                return {"ok": False, "filled": 0.0, "error": "no_visible_liquidity_within_slippage"}
            # When a PRE-JUMP reference price is supplied, submit at its hard
            # slippage cap so one fast tick can still match. Direct/internal BUY
            # calls without a reference retain the previous visible-worst limit.
            limit_price = cap if cap is not None else _worst
        else:
            limit_price, visible = _visible_fak_limit(asset, wanted, action)
            if limit_price is None or visible <= 1e-9:
                return {"ok": False, "filled": 0.0, "error": "no_visible_liquidity"}

        # Align the price to the actual Polymarket token tick. This is critical
        # when reference_price + slippage creates e.g. 0.645 while tick_size=0.01.
        # BUY rounds DOWN, so this can never exceed the configured slippage cap.
        normalized_limit = _normalize_live_limit_price(asset, limit_price, action)
        limit_price = float(normalized_limit)
        limit_str = format(normalized_limit, "f")
        size_str = format(Decimal(str(wanted)), "f")
        submitted = now_ms()

        # Stage 1: build/sign locally. Any exception here is definitely BEFORE
        # submission, therefore it is safe and must never be marked AMBIGUOUS.
        if latency_ctx is not None:
            latency_ctx["build_start_ms"] = now_ms()
        try:
            signed = await live_client.create_limit_order(
                token_id=str(asset),
                price=limit_str,
                size=size_str,
                side=action,
                post_only=False,
            )
            fak_order = replace(signed, order_type="FAK", post_only=False)
            if latency_ctx is not None:
                latency_ctx["build_end_ms"] = now_ms()
                latency_ctx["build_sign_ms"] = max(0, latency_ctx["build_end_ms"] - latency_ctx.get("build_start_ms", latency_ctx["build_end_ms"]))
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            with db() as conn:
                conn.execute("""
                    INSERT INTO live_orders(
                        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    submitted, condition, name, symbol, asset, outcome, action, reason,
                    wanted, limit_price, "", "REJECTED_LOCAL", 0.0, None,
                    0.0, 0.0, 0.0, "[]", "{}", error,
                ))
                conn.commit()
            log.warning(
                "LIVE LOCAL REJECT %s | %s %s %s %.4fsh limit=%s tick=%s | %s",
                name, action, reason, outcome, wanted, limit_str,
                format(_asset_tick_size(asset), "f"), error,
            )
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"⛔ LIVE ORDER REJECTED {symbol}\n"
                    f"{action} {reason}: {error}\n"
                    f"limit={limit_str} | tick={format(_asset_tick_size(asset), 'f')}\n"
                    "Order was rejected before submission; no real order was created."
                )
            return {
                "ok": False, "filled": 0.0, "error": error,
                "retryable": False, "status": "REJECTED_LOCAL",
            }

        # Stage 2: from this point onward a submission may occur. Unknown transport
        # failures stay fail-closed; only deterministic FAK NO_MATCH is retry-safe.
        try:
            if latency_ctx is not None:
                latency_ctx["submit_ms"] = now_ms()
                latency_ctx["signal_to_submit_ms"] = max(0, latency_ctx["submit_ms"] - latency_ctx["signal_ms"])
                latency_ctx["attempt_to_submit_ms"] = max(0, latency_ctx["submit_ms"] - latency_ctx["attempt_start_ms"])
            submitted = now_ms()
            if sdk_post_order_with_allowance_recovery is not None:
                response = await sdk_post_order_with_allowance_recovery(live_client, fak_order)
            else:
                # Test/offline fallback; production requirements pin the SDK version
                # that provides allowance-recovery placement.
                response = await live_client.post_order(fak_order)

            if latency_ctx is not None:
                latency_ctx["response_ms"] = now_ms()
                latency_ctx["api_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx.get("submit_ms", latency_ctx["response_ms"]))
                latency_ctx["signal_to_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx["signal_ms"])
                latency_ctx["attempt_to_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx["attempt_start_ms"])

            ok = bool(getattr(response, "ok", False))
            if not ok:
                error = f"{getattr(response, 'code', 'rejected')}: {getattr(response, 'message', '')}".strip()
                no_match = is_definite_fak_no_match_error(error)
                balance_reject = (
                    action == "SELL" and reason == "TAKE_PROFIT"
                    and is_definite_balance_allowance_rejection(error)
                )
                reject_status = (
                    "REJECTED_NO_MATCH" if no_match else
                    "REJECTED_BALANCE_ALLOWANCE" if balance_reject else
                    "REJECTED"
                )
                with db() as conn:
                    conn.execute("""
                        INSERT INTO live_orders(
                            submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                            requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                            gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        submitted, condition, name, symbol, asset, outcome, action, reason,
                        wanted, limit_price, "", reject_status, 0.0, None, 0.0, 0.0, 0.0,
                        "[]", _response_json(response), error,
                    ))
                    conn.commit()
                log.warning("LIVE REJECT %s %s %s | %s", name, action, reason, error)
                return {
                    "ok": False, "filled": 0.0, "error": error,
                    "retryable": bool(no_match or balance_reject), "status": reject_status,
                }

            making = sf(getattr(response, "making_amount", 0))
            taking = sf(getattr(response, "taking_amount", 0))
            status = str(getattr(response, "status", ""))
            order_id = str(getattr(response, "order_id", ""))
            trade_ids = tuple(getattr(response, "trade_ids", ()) or ())

            # CLOB order denomination:
            # BUY makes collateral and takes shares; SELL makes shares and takes collateral.
            if action == "BUY":
                filled = taking
                gross = making
            else:
                filled = making
                gross = taking

            avg = gross / filled if filled > 1e-9 else 0.0
            fee = fee_usdc(filled, avg) if filled > 1e-9 else 0.0
            net_or_total = gross + fee if action == "BUY" else gross - fee

            stored_status = status
            if filled <= 1e-9 and status.lower() in {"delayed", "live", "matched"}:
                stored_status = "DELAYED_AMBIGUOUS"

            with db() as conn:
                conn.execute("""
                    INSERT INTO live_orders(
                        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    submitted, condition, name, symbol, asset, outcome, action, reason,
                    wanted, limit_price, order_id, stored_status, filled, avg, gross, fee,
                    net_or_total, jd(list(trade_ids)), _response_json(response), "",
                ))
                conn.commit()

            if stored_status == "DELAYED_AMBIGUOUS":
                log.error("LIVE AMBIGUOUS %s %s %s | order_id=%s status=%s", name, action, reason, order_id, status)
                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    await tg_send(
                        f"⚠️ LIVE AMBIGUOUS {symbol}\n"
                        f"{action} {reason}: exchange returned {status} without a measurable fill.\n"
                        "This market/action is fail-closed: the bot will NOT retry automatically."
                    )

            if filled > 1e-9 and action == "BUY":
                st = get_variant_state(condition, variant)
                st["buys"][asset] += 1
                st["last_buy"][asset] = avg
                st["started_sides"].add(asset)
                if st["primary_asset"] is None:
                    st["primary_asset"] = asset

            if filled > 1e-9:
                log.warning(
                    "🔴 LIVE %s %-20s %-7s %s | %.4fsh @ %.4f | limit %.4f | status=%s",
                    action, name, reason, outcome, filled, avg, limit_price, status,
                )
                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                    timing = _entry_latency_line(condition, name) if action == "BUY" and reason == "ENTRY" else ""
                    await tg_send(
                        f"🔴 LIVE {action} {symbol}\n"
                        f"{reason} {outcome}: {filled:.4f}sh @ {avg:.4f}\n"
                        f"limit {limit_price:.4f} | {status}"
                        + (f"\n⏱ {timing}" if timing else "")
                    )
            return {
                "ok": True,
                "filled": filled,
                "avg": avg,
                "gross": gross,
                "fee": fee,
                "net_or_total": net_or_total,
                "status": status,
                "order_id": order_id,
            }

        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            if latency_ctx is not None and latency_ctx.get("submit_ms") is not None:
                latency_ctx["response_ms"] = now_ms()
                latency_ctx["api_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx["submit_ms"])
                latency_ctx["signal_to_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx["signal_ms"])
                latency_ctx["attempt_to_response_ms"] = max(0, latency_ctx["response_ms"] - latency_ctx["attempt_start_ms"])

            # IMPORTANT: a FAK "no orders found to match" rejection is a
            # deterministic zero-fill/kill, not an ambiguous submission. Record
            # it as REJECTED_NO_MATCH for both ENTRY BUY and TAKE_PROFIT SELL.
            # BUY may receive one tightly controlled immediate retry in
            # execute_order(); SELL TP keeps its existing later-cycle retry.
            retryable_no_match = is_definite_fak_no_match_error(e)
            retryable_balance = (
                action == "SELL" and reason == "TAKE_PROFIT"
                and is_definite_balance_allowance_rejection(e)
            )

            if retryable_balance:
                with db() as conn:
                    conn.execute("""
                        INSERT INTO live_orders(
                            submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                            requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                            gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        submitted, condition, name, symbol, asset, outcome, action, reason,
                        wanted, limit_price, "", "REJECTED_BALANCE_ALLOWANCE", 0.0, None,
                        0.0, 0.0, 0.0, "[]", "{}", error,
                    ))
                    conn.commit()
                log.warning(
                    "LIVE TP BALANCE-SYNC REJECT %s | %s %.4fsh | %s",
                    name, outcome, wanted, error,
                )
                return {
                    "ok": False, "filled": 0.0, "error": error,
                    "retryable": True, "status": "REJECTED_BALANCE_ALLOWANCE",
                }

            if retryable_no_match:
                with db() as conn:
                    prior = si(conn.execute(
                        """SELECT COUNT(*) c FROM live_orders
                           WHERE condition_id=? AND variant=? AND action=?
                             AND reason=? AND status='REJECTED_NO_MATCH'""",
                        (condition, name, action, reason),
                    ).fetchone()["c"])
                    conn.execute("""
                        INSERT INTO live_orders(
                            submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                            requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                            gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        submitted, condition, name, symbol, asset, outcome, action, reason,
                        wanted, limit_price, "", "REJECTED_NO_MATCH", 0.0, None,
                        0.0, 0.0, 0.0, "[]", "{}", error,
                    ))
                    conn.commit()

                log.warning(
                    "LIVE FAK NO-MATCH %s | %s %s %s %.4fsh limit=%.4f | retry-safe",
                    name, action, reason, outcome, wanted, limit_price,
                )
                if (
                    action == "SELL" and reason == "TAKE_PROFIT"
                    and prior == 0 and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
                ):
                    await tg_send(
                        f"⏳ LIVE TP NO MATCH {symbol} / {variant['code']}\n"
                        f"SELL TAKE_PROFIT {outcome}: 0sh filled.\n"
                        "FAK was killed with no match, so this is NOT treated as ambiguous.\n"
                        "Bot will retry on later TP cycles only while the current NET TP "
                        "condition is still satisfied."
                    )
                return {
                    "ok": False,
                    "filled": 0.0,
                    "error": error,
                    "retryable": True,
                    "status": "REJECTED_NO_MATCH",
                }

            # Any other exception may have happened after submission and is
            # still fail-closed exactly as before. We cannot safely know whether
            # the exchange accepted/fillled it, so automatic duplicate retry is
            # blocked for that market/action.
            with db() as conn:
                conn.execute("""
                    INSERT INTO live_orders(
                        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
                        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
                        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    submitted, condition, name, symbol, asset, outcome, action, reason,
                    wanted, limit_price, "", "AMBIGUOUS", 0.0, None, 0.0, 0.0, 0.0,
                    "[]", "{}", error,
                ))
                conn.commit()
            log.exception("LIVE order failed | %s %s %s", name, action, reason)
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"⚠️ LIVE ORDER AMBIGUOUS {symbol}\n"
                    f"{action} {reason}: {error}\n"
                    "Automatic retry for this market/action is blocked to prevent a duplicate real order."
                )
            return {"ok": False, "filled": 0.0, "error": error}



async def execute_paper(condition, variant, asset, outcome, signal_type):
    age = await ensure_book(asset)

    wanted = requested_shares(variant, signal_type)
    if not _valid_user_shares(wanted):
        log.warning("PAPER BLOCK %s %s invalid shares %.4f", variant["name"], signal_type, wanted)
        return False

    fills, filled = simulate_buy(asset, wanted)
    if filled <= 0:
        return False

    name = variant["name"]
    cash = paper_cash(name)
    available = max(0.0, cash - MIN_FREE_CASH)
    gross = sum(p * q for p, q in fills)
    fee = sum(fee_usdc(q, p) for p, q in fills)
    total = gross + fee

    if total > available + 1e-8:
        fills, filled = trim_fills_to_budget(fills, available)
        if filled <= 1e-8:
            log.warning("CASH BLOCK %s %s %s | cash=%.2f", name, signal_type, outcome, cash)
            return False
        gross = sum(p * q for p, q in fills)
        fee = sum(fee_usdc(q, p) for p, q in fills)
        total = gross + fee

    avg = gross / filled
    after = cash - total
    with db() as conn:
        conn.execute("""
            INSERT INTO paper_trades(
                trade_ms,condition_id,variant,asset,outcome,signal_type,
                requested_shares,filled_shares,avg_price,gross_cost,fee,
                total_cost,book_age_ms,fills_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), condition, name, asset, outcome, signal_type,
            wanted, filled, avg, gross, fee, total, age,
            jd([{"price": p, "shares": q} for p, q in fills]),
        ))
        conn.execute(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (f"paper_cash:{name}", str(after)),
        )
        conn.commit()

    st = get_variant_state(condition, variant)
    st["buys"][asset] += 1
    st["last_buy"][asset] = avg
    st["started_sides"].add(asset)
    if st["primary_asset"] is None:
        st["primary_asset"] = asset

    log.info(
        "PAPER BUY %-28s %-7s %-4s | %.2fsh @ %.4f fee=%.4f | cash %.2f -> %.2f",
        name, signal_type, outcome, filled, avg, fee, cash, after,
    )
    return True


def _prejump_signal_snapshot(condition, variant_name):
    """Read the already-accepted PRE-JUMP signal for post-execution diagnostics only."""
    with db() as conn:
        row = conn.execute(
            """SELECT signal_ms,symbol,outcome,pm_ask,pm_bid,pm_momentum,ext_score,
                      same_votes,opposing_votes,fresh_venues,elapsed_sec,threshold
               FROM prejump_signals
               WHERE condition_id=? AND variant=?
               ORDER BY id DESC LIMIT 1""",
            (condition, variant_name),
        ).fetchone()
    return dict(row) if row else {}


async def _notify_live_entry_not_filled(
    condition, variant, asset, outcome, reference_price, error, stage="EXECUTION"
):
    """Telegram-only post-failure diagnostics. Never runs before an ENTRY attempt."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return

    name = variant["name"]
    # AMBIGUOUS and local-validation failures already have dedicated alerts.
    if live_action_ambiguous(condition, name, "BUY", "ENTRY"):
        return

    snap = _prejump_signal_snapshot(condition, name)
    signal_ask = sf(snap.get("pm_ask"), sf(reference_price))
    current_ask = best_ask(asset)
    cap = _entry_price_cap(reference_price)
    score = snap.get("ext_score")
    mom = snap.get("pm_momentum")
    votes = snap.get("same_votes")
    fresh = snap.get("fresh_venues")
    elapsed = snap.get("elapsed_sec")

    signal_bits = []
    if score is not None:
        signal_bits.append(f"score {sf(score):.3f}")
    if mom is not None:
        signal_bits.append(f"mom {sf(mom):+.3f}")
    if votes is not None:
        signal_bits.append(f"votes {si(votes)}")
    if fresh is not None:
        signal_bits.append(f"fresh {si(fresh)}")
    if elapsed is not None:
        signal_bits.append(f"t={sf(elapsed):.2f}s")

    price_bits = [f"signal ask {signal_ask:.3f}"]
    if current_ask is not None:
        price_bits.append(f"live ask {sf(current_ask):.3f}")
    if cap is not None:
        price_bits.append(f"cap {sf(cap):.3f}")

    timing = _entry_latency_line(condition, name)
    await tg_send(
        f"🔎 LIVE ENTRY MISSED {variant['symbol']}\n"
        f"Signal SEEN: {outcome}"
        + (" | " + " | ".join(signal_bits) if signal_bits else "")
        + "\n"
        + " | ".join(price_bits)
        + (f"\n⏱ {timing}" if timing else "")
        + f"\n{stage}: {str(error or 'not_filled')[:500]}\n"
        + "No position opened. PRE-JUMP ENTRY rules were not changed."
    )


async def execute_order(
    condition, variant, asset, outcome, signal_type, reference_price=None,
    signal_detected_ms=None, event_received_ms=None, evaluation_path=None,
):
    pos_before = position_totals(condition, variant["name"])
    if pos_before["buys"] and pos_before["remaining"] <= 1e-8:
        return False

    mode = strategy_mode(variant["name"])
    if mode == "OFF":
        return False
    if mode == "PAPER":
        return await execute_paper(condition, variant, asset, outcome, signal_type)

    wanted = requested_shares(variant, signal_type)
    result = await execute_live_fak(
        condition, variant, asset, outcome, signal_type, "BUY", wanted,
        reference_price=reference_price, force_rest=False,
        signal_detected_ms=signal_detected_ms, event_received_ms=event_received_ms,
        evaluation_path=evaluation_path, attempt_label="first", ws_only=False,
    )
    if sf(result.get("filled")) > 1e-9:
        return True

    # Retry ONLY deterministic zero-fill FAK NO_MATCH. Unknown/network/API
    # failures remain fail-closed inside execute_live_fak. v20.11 revalidates the
    # accepted direction and retries immediately from the latest WS book. No REST
    # RTT is inserted unless LIVE_ENTRY_RETRY_FORCE_REST=1 is explicitly set.
    if not result.get("retryable") or LIVE_ENTRY_NO_MATCH_RETRIES <= 0:
        if str(result.get("status") or "") != "REJECTED_LOCAL":
            await _notify_live_entry_not_filled(
                condition, variant, asset, outcome, reference_price,
                result.get("error"), stage=str(result.get("status") or "EXECUTION"),
            )
        return False

    last_reason = "no_match"
    for attempt in range(1, LIVE_ENTRY_NO_MATCH_RETRIES + 1):
        if LIVE_ENTRY_RETRY_DELAY_MS:
            await asyncio.sleep(LIVE_ENTRY_RETRY_DELAY_MS / 1000.0)
        valid, last_reason = await _live_entry_retry_valid(
            condition, variant, asset, outcome, reference_price
        )
        if not valid:
            log.warning(
                "LIVE ENTRY RETRY CANCEL %s %s | attempt=%d | %s",
                variant["name"], outcome, attempt, last_reason,
            )
            break

        retry = await execute_live_fak(
            condition, variant, asset, outcome, signal_type, "BUY", wanted,
            reference_price=reference_price, force_rest=LIVE_ENTRY_RETRY_FORCE_REST,
            signal_detected_ms=signal_detected_ms, event_received_ms=event_received_ms,
            evaluation_path=evaluation_path, attempt_label=f"retry{attempt}",
            ws_only=not LIVE_ENTRY_RETRY_FORCE_REST,
        )
        if sf(retry.get("filled")) > 1e-9:
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"✅ LIVE ENTRY RETRY FILLED {variant['symbol']}\n"
                    f"{outcome}: {sf(retry.get('filled')):.4f}sh @ "
                    f"{sf(retry.get('avg')):.4f} | retry {attempt}/{LIVE_ENTRY_NO_MATCH_RETRIES}"
                )
            return True
        if not retry.get("retryable"):
            last_reason = str(retry.get("error") or "retry_not_safe")
            break
        last_reason = "no_match_again"

    await _notify_live_entry_not_filled(
        condition, variant, asset, outcome, reference_price,
        f"deterministic FAK NO_MATCH; retry stopped: {last_reason}",
        stage="NO_MATCH_RETRY",
    )
    return False


def pm_fast_momentum(condition, asset, seconds=1.0):
    h = fast_pm_history[condition][asset]
    if len(h) < 2:
        return None
    target = h[-1][0] - int(float(seconds) * 1000)
    prior = min(h, key=lambda x: abs(x[0] - target))
    if abs(prior[0] - target) > 700:
        return None
    return sf(h[-1][1]) - sf(prior[1])


def store_prejump_signal(market, variant, asset, outcome, ask, bid, mom, elapsed, threshold, feature, directional):
    with db() as conn:
        conn.execute("""
            INSERT INTO prejump_signals(
                signal_ms,condition_id,variant,symbol,asset,outcome,pm_ask,pm_bid,
                pm_momentum,ext_score,same_votes,opposing_votes,fresh_venues,
                elapsed_sec,threshold,features_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), market["condition_id"], variant["name"], variant["symbol"],
            asset, outcome, ask, bid, mom, directional["score"],
            directional["same_votes"], directional["opposing_votes"],
            directional["fresh_venues"], elapsed, threshold, jd(feature),
        ))
        conn.commit()


async def _evaluate_prejump_variant_unlocked(market, variant, elapsed, feature):
    """One-shot PRE-JUMP gate using the lab's forward-tested 0.40 family."""
    cid = market["condition_id"]
    st = get_variant_state(cid, variant)
    if st.get("stopped_out") or st.get("take_profit_closed"):
        return False
    if st["gate_decided"] or st["started_sides"]:
        return False
    if not (PREJUMP_MIN_ELAPSED <= elapsed <= PREJUMP_MAX_ELAPSED):
        return False
    if not feature or si(feature.get("fresh_venues")) < PREJUMP_MIN_VENUES:
        return False

    threshold = prejump_score()
    ext_score = sf(feature.get("ext_score"))
    if abs(ext_score) + 1e-12 < threshold:
        return False

    outcome = "Up" if ext_score > 0 else "Down"
    asset = market["up_asset"] if outcome == "Up" else market["down_asset"]
    directional = directional_external(feature, outcome)
    if directional["score"] + 1e-12 < threshold:
        return False
    if directional["same_votes"] < PREJUMP_MIN_VENUES:
        return False
    if PREJUMP_REQUIRE_BINANCE_BYBIT and not (
        directional["binance_same"] and directional["bybit_same"]
    ):
        return False

    # Entry is accepted only on a fresh, non-crossed Polymarket book.
    bid, ask, age = await _refresh_entry_book_if_needed(asset)
    if ask is None or not (PREJUMP_PRICE_MIN <= ask <= PREJUMP_PRICE_MAX):
        return False

    # Re-sample the latest ask after any REST refresh. The 1s momentum gate is
    # deliberately identical to the research bot: -0.01..+0.05.
    fast_pm_history[cid][asset].append((now_ms(), ask))
    mom = pm_fast_momentum(cid, asset, 1.0)
    if mom is None or not (PREJUMP_PM_MOM_MIN <= mom <= PREJUMP_PM_MOM_MAX):
        return False

    # Token-specific SAFE filters. IMPORTANT: these are FIRST-base-signal
    # filters. The base candidate above is still the normal PRE-JUMP family
    # (global score/ask/momentum/votes/time). Once the first otherwise-valid
    # candidate appears for a filtered token, it is either accepted immediately
    # or the 5-minute market is skipped permanently. No later "better" signal
    # is allowed to resurrect the market.
    symbol = str(variant.get("symbol", "")).upper()
    safe_ok = True
    safe_reason = ""
    safe_detail = ""

    if symbol == "BTC":
        score_ok = directional["score"] + 1e-12 >= BTC_PREJUMP_SCORE
        ask_ok = ask <= BTC_PREJUMP_PRICE_MAX + 1e-12
        safe_ok = score_ok and ask_ok
        if not safe_ok:
            if not score_ok and not ask_ok:
                safe_reason = "BTC_FIRST_SIGNAL_FILTER_SCORE_AND_ASK"
            elif not score_ok:
                safe_reason = "BTC_FIRST_SIGNAL_FILTER_SCORE"
            else:
                safe_reason = "BTC_FIRST_SIGNAL_FILTER_ASK"
        safe_detail = (
            f"score={directional['score']:+.3f} need>={BTC_PREJUMP_SCORE:.3f} | "
            f"ask={ask:.3f} max={BTC_PREJUMP_PRICE_MAX:.2f}"
        )
    elif symbol == "SOL":
        safe_ok = mom <= SOL_PREJUMP_PM_MOM_MAX + 1e-12
        safe_reason = "SOL_FIRST_SIGNAL_FILTER_MOM_MAX" if not safe_ok else ""
        safe_detail = f"pmMom={mom:+.3f} max={SOL_PREJUMP_PM_MOM_MAX:+.3f}"
    elif symbol == "XRP":
        safe_ok = directional["score"] + 1e-12 >= XRP_PREJUMP_SCORE
        safe_reason = "XRP_FIRST_SIGNAL_FILTER_SCORE" if not safe_ok else ""
        safe_detail = f"score={directional['score']:+.3f} need>={XRP_PREJUMP_SCORE:.3f}"
    elif symbol == "BNB":
        safe_ok = mom + 1e-12 >= BNB_PREJUMP_PM_MOM_MIN
        safe_reason = "BNB_FIRST_SIGNAL_FILTER_MOM_MIN" if not safe_ok else ""
        safe_detail = f"pmMom={mom:+.3f} min={BNB_PREJUMP_PM_MOM_MIN:+.3f}"
    elif symbol == "DOGE":
        spread = None if bid is None else ask - bid
        safe_ok = spread is not None and spread >= -1e-12 and spread <= DOGE_PREJUMP_MAX_SPREAD + 1e-12
        safe_reason = "DOGE_FIRST_SIGNAL_FILTER_SPREAD" if not safe_ok else ""
        spread_text = "n/a" if spread is None else f"{spread:.3f}"
        safe_detail = f"spread={spread_text} max={DOGE_PREJUMP_MAX_SPREAD:.3f} | bid={bid} ask={ask:.3f}"
    elif symbol == "ETH":
        score_ok = directional["score"] + 1e-12 >= ETH_PREJUMP_SCORE
        mom_ok = mom <= ETH_PREJUMP_PM_MOM_MAX + 1e-12
        safe_ok = score_ok and mom_ok
        if not safe_ok:
            if not score_ok and not mom_ok:
                safe_reason = "ETH_FIRST_SIGNAL_FILTER_SCORE_AND_MOM"
            elif not score_ok:
                safe_reason = "ETH_FIRST_SIGNAL_FILTER_SCORE"
            else:
                safe_reason = "ETH_FIRST_SIGNAL_FILTER_MOM_MAX"
        safe_detail = (
            f"score={directional['score']:+.3f} need>={ETH_PREJUMP_SCORE:.3f} | "
            f"pmMom={mom:+.3f} max={ETH_PREJUMP_PM_MOM_MAX:+.3f}"
        )

    if not safe_ok:
        st["gate_decided"] = True
        st["gate_passed"] = False
        st["gate_asset"] = asset
        ref = ask - mom
        store_gate_decision(cid, variant, asset, outcome, ask, ref, mom, elapsed, False, safe_reason)
        log.warning(
            "%s FIRST-SIGNAL SAFE SKIP %s | %s | elapsed=%.2fs | %s",
            symbol, outcome, safe_detail, elapsed, safe_reason,
        )
        return False

    # One signal / one execution attempt per market. Mark the gate BEFORE a
    # LIVE submission: a timeout/ambiguous response must never cause a duplicate.
    signal_detected_ms = now_ms()
    event_received_ms = si(feature.get("_event_received_ms"), 0) if feature else 0
    evaluation_path = str((feature or {}).get("_evaluation_path") or "timer")
    st["gate_decided"] = True
    st["gate_passed"] = True
    st["gate_asset"] = asset
    ref = ask - mom
    reason = "PREJUMP_EXTERNAL_OK_EARLY" if elapsed < 5.0 else "PREJUMP_EXTERNAL_OK"
    store_gate_decision(cid, variant, asset, outcome, ask, ref, mom, elapsed, True, reason)
    store_signal(cid, variant, asset, outcome, ask, ref, mom, "ENTRY", elapsed)
    store_prejump_signal(
        market, variant, asset, outcome, ask, bid, mom, elapsed,
        threshold, feature, directional,
    )

    log.warning(
        "PREJUMP SIGNAL %-4s %s ask=%.3f bid=%s pmMom=%+.3f | ext=%+.3f threshold=%.2f votes=%d | elapsed=%.2fs",
        variant["symbol"], outcome, ask,
        f"{bid:.3f}" if bid is not None else "n/a", mom,
        directional["score"], threshold, directional["same_votes"], elapsed,
    )
    filled = await execute_order(
        cid, variant, asset, outcome, "ENTRY", reference_price=ask,
        signal_detected_ms=signal_detected_ms,
        event_received_ms=event_received_ms or None,
        evaluation_path=evaluation_path,
    )
    if not filled:
        log.warning(
            "PREJUMP NO FILL %-4s %s | gate remains closed for this market (duplicate-safe)",
            variant["symbol"], outcome,
        )
    return bool(filled)


async def evaluate_prejump_variant(market, variant, elapsed, feature):
    """Race-safe entry gate shared by timer and event-driven evaluators."""
    key = (market["condition_id"], variant["name"])
    async with prejump_eval_locks[key]:
        return await _evaluate_prejump_variant_unlocked(market, variant, elapsed, feature)


def record_position_trajectory(market, variant, elapsed):
    cid = market["condition_id"]
    with db() as conn:
        if conn.execute(
            "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
            (cid, variant["name"]),
        ).fetchone():
            return False

    pos = position_totals(cid, variant["name"])
    if not pos["buys"]:
        return False

    primary_asset = pos["primary_asset"]
    primary_outcome = pos["primary_outcome"]
    opposite_asset = str(market["down_asset"] if primary_asset == str(market["up_asset"]) else market["up_asset"])
    remaining = pos["remaining"]

    p_bid = best_bid(primary_asset)
    p_ask = best_ask(primary_asset)
    o_bid = best_bid(opposite_asset)
    o_ask = best_ask(opposite_asset)

    mark_fills, mark_filled = simulate_sell(primary_asset, remaining) if remaining > 1e-9 else ([], 0.0)
    mark_gross = sum(sf(px) * sf(q) for px, q in mark_fills)
    mark_fee = sum(fee_usdc(sf(q), sf(px)) for px, q in mark_fills)
    mark_net = mark_gross - mark_fee
    mark_avg = mark_gross / mark_filled if mark_filled > 1e-9 else None

    # Total PnL if all remaining shares could be liquidated now.
    unrealized = None
    if remaining <= 1e-9:
        unrealized = pos["exit_net"] - pos["buy_cost"]
    elif mark_filled >= remaining - 1e-8:
        unrealized = pos["exit_net"] + mark_net - pos["buy_cost"]

    with db() as conn:
        prev = conn.execute("""
            SELECT MAX(unrealized_total_pnl) mfe, MIN(unrealized_total_pnl) mae
            FROM position_trajectory
            WHERE condition_id=? AND variant=? AND unrealized_total_pnl IS NOT NULL
        """, (cid, variant["name"])).fetchone()
        prev_mfe = sf(prev["mfe"]) if prev and prev["mfe"] is not None else None
        prev_mae = sf(prev["mae"]) if prev and prev["mae"] is not None else None
        mfe = prev_mfe if unrealized is None else (unrealized if prev_mfe is None else max(prev_mfe, unrealized))
        mae = prev_mae if unrealized is None else (unrealized if prev_mae is None else min(prev_mae, unrealized))

        conn.execute("""
            INSERT INTO position_trajectory(
                sample_ms,condition_id,variant,elapsed_sec,primary_asset,primary_outcome,
                opposite_asset,bought_shares,exited_shares,remaining_shares,gross_entry_cost,
                entry_fees,total_buy_cost,exit_net_so_far,primary_best_bid,primary_best_ask,
                opposite_best_bid,opposite_best_ask,mark_filled_shares,mark_avg_price,mark_fee,
                mark_net_proceeds,unrealized_total_pnl,mfe_pnl,mae_pnl,stop_triggered
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_ms(), cid, variant["name"], elapsed, primary_asset, primary_outcome,
            opposite_asset, pos["bought"], pos["exited"], remaining,
            sum(sf(r["gross_cost"]) for r in pos["buys"]),
            sum(sf(r["fee"]) for r in pos["buys"]), pos["buy_cost"], pos["exit_net"],
            p_bid, p_ask, o_bid, o_ask, mark_filled, mark_avg, mark_fee, mark_net,
            unrealized, mfe, mae, 0,
        ))
        conn.commit()
    return True


async def _event_driven_evaluate_symbol_once(symbol, trigger_ms=None):
    """Evaluate LIVE PRE-JUMP immediately after an external WS update.

    Signal thresholds are unchanged. PAPER is intentionally left on the timer path
    so the research comparison is not silently changed.
    """
    if not EVENT_DRIVEN_LIVE_ENTRY or not trading_enabled():
        return False
    live_variants = [v for v in STRATEGIES_BY_SYMBOL.get(symbol, []) if strategy_mode(v["name"]) == "LIVE"]
    if not live_variants:
        return False

    t_ms = now_ms()
    t_s = time.time()
    feature = build_external_snapshot(symbol, t_ms)
    feature["_evaluation_path"] = "event"
    feature["_event_received_ms"] = si(trigger_ms, t_ms) or t_ms
    # Do not append venue_sample_history here: its 1/3/10s reference cadence remains
    # the stable 100ms timer cadence. Current trade/book/flow state is nevertheless
    # fresh because build_external_snapshot reads the just-updated in-memory feeds.
    feature_history[symbol].append(feature)

    any_eval = False
    for cid, market in list(markets.items()):
        if market_symbol(market) != symbol:
            continue
        elapsed = t_s - market["start_ts"]
        if not (PREJUMP_MIN_ELAPSED <= elapsed <= PREJUMP_MAX_ELAPSED):
            continue
        for variant in live_variants:
            if variant not in strategies_for_market(market):
                continue
            any_eval = True
            await evaluate_prejump_variant(market, variant, elapsed, feature)
    return any_eval


async def event_driven_symbol_loop(symbol):
    ev = external_eval_events[symbol]
    while True:
        await ev.wait()
        ev.clear()
        try:
            if EVENT_DRIVEN_MIN_INTERVAL_MS:
                elapsed_ms = (time.monotonic() - external_eval_last_run[symbol]) * 1000.0
                wait_ms = EVENT_DRIVEN_MIN_INTERVAL_MS - elapsed_ms
                if wait_ms > 0:
                    await asyncio.sleep(wait_ms / 1000.0)
            trigger_ms = external_eval_received_ms.get(symbol) or now_ms()
            await _event_driven_evaluate_symbol_once(symbol, trigger_ms)
            external_eval_last_run[symbol] = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("event-driven PRE-JUMP evaluator failed | %s", symbol)


async def strategy_loop():
    """100ms fallback scorer + PAPER path; event-driven loop handles LIVE first."""
    while True:
        started = time.monotonic()
        t_s = time.time()
        t_ms = now_ms()
        try:
            # Build one external snapshot per symbol and update history used by
            # 1/3/10-second returns/depth changes.
            current_features = {}
            for symbol in SYMBOLS:
                feature = build_external_snapshot(symbol, t_ms)
                for venue in ("binance", "bybit", "coinbase"):
                    append_venue_feature_history(venue, symbol, feature.get(venue))
                feature["_evaluation_path"] = "timer"
                feature_history[symbol].append(feature)
                current_features[symbol] = feature

            for cid, market in list(markets.items()):
                elapsed = t_s - market["start_ts"]
                if not (-2 <= elapsed <= 305):
                    continue

                # 100ms Polymarket ask sampling for the 1-second momentum gate.
                for asset in (market["up_asset"], market["down_asset"]):
                    ask = best_ask(asset)
                    if ask is not None:
                        fast_pm_history[cid][asset].append((t_ms, ask))

                variants = strategies_for_market(market)
                for variant in variants:
                    key = (cid, variant["name"])

                    # Existing positions are always TP-monitored, even when STOP
                    # blocks new entries or the token mode is later set OFF.
                    if 0 <= elapsed <= 305:
                        if t_s - last_tp_check[key] >= TP_CHECK_INTERVAL:
                            await maybe_take_profit(market, variant, elapsed)
                            last_tp_check[key] = t_s
                        if t_s - last_trajectory_sample[key] >= TRAJECTORY_INTERVAL:
                            record_position_trajectory(market, variant, elapsed)
                            last_trajectory_sample[key] = t_s

                    if not trading_enabled():
                        continue
                    # OFF must not consume the one-shot gate. This lets the user
                    # arm a token later in the same market and only react to a
                    # signal that is qualifying at that later moment.
                    if strategy_mode(variant["name"]) == "OFF":
                        continue
                    if not (PREJUMP_MIN_ELAPSED <= elapsed <= PREJUMP_MAX_ELAPSED):
                        continue
                    feature = current_features.get(variant["symbol"])
                    await evaluate_prejump_variant(market, variant, elapsed, feature)

        except Exception:
            log.exception("PRE-JUMP strategy loop failed")

        spent = time.monotonic() - started
        await asyncio.sleep(max(0.02, FAST_INTERVAL - spent))


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
                row = conn.execute(
                    "SELECT * FROM discovered_markets WHERE condition_id=?", (cid,)
                ).fetchone()
                if not row:
                    return
                market = dict(row)

        symbol = market_symbol(market)
        pair = STRATEGIES_BY_SYMBOL.get(symbol, [])
        messages = []

        for variant in pair:
            name = variant["name"]
            with db() as conn:
                if conn.execute(
                    "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                    (cid, name),
                ).fetchone():
                    continue

            pos = position_totals(cid, name)
            buys = pos["buys"]
            exits = pos["exits"]
            buy_cost = pos["buy_cost"]
            exit_proceeds = pos["exit_net"]

            up_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(market["up_asset"])
            )
            down_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(market["down_asset"])
            )
            up_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(market["up_asset"])
            )
            down_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(market["down_asset"])
            )
            winning_bought = sum(
                sf(r["filled_shares"]) for r in buys
                if str(r["asset"]) == str(winning_asset)
            )
            winning_exited = sum(
                sf(r["filled_shares"]) for r in exits
                if str(r["asset"]) == str(winning_asset)
            )

            payout = max(0.0, winning_bought - winning_exited)
            pnl = exit_proceeds + payout - buy_cost
            execution_mode = pos.get("execution_mode") or strategy_mode(name)

            with db() as conn:
                stopped = 0

                conn.execute("""
                    INSERT INTO market_results(
                        condition_id,variant,winning_asset,winning_outcome,buy_cost,
                        exit_proceeds,payout,pnl,buy_trades,exit_trades,up_bought,
                        down_bought,up_exited,down_exited,stopped_out,execution_mode,settled_ms
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    cid, name, winning_asset, winning_outcome, buy_cost,
                    exit_proceeds, payout, pnl, len(buys), len(exits), up_bought,
                    down_bought, up_exited, down_exited, stopped, execution_mode, now_ms(),
                ))

                cash_after = None
                # Only the PAPER ledger receives synthetic $1/share settlement.
                # LIVE winning shares remain on the actual Polymarket wallet and are
                # not auto-redeemed by this bot.
                if execution_mode == "PAPER":
                    cash_row = conn.execute(
                        "SELECT value FROM state WHERE key=?", (f"paper_cash:{name}",)
                    ).fetchone()
                    cash_before = sf(
                        cash_row["value"] if cash_row else PAPER_START_BALANCE,
                        PAPER_START_BALANCE,
                    )
                    cash_after = cash_before + payout
                    conn.execute(
                        "INSERT INTO state(key,value) VALUES(?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (f"paper_cash:{name}", str(cash_after)),
                    )
                conn.commit()

            if buys:
                mode_tag = "🔴 LIVE" if execution_mode == "LIVE" else "🟢 PAPER"
                tail = f" | paper cash ${cash_after:.2f}" if cash_after is not None else " | payout not auto-redeemed"
                messages.append(
                    f"{mode_tag} {variant['short']}: PnL~{pnl:+.2f}{tail}"
                    + (" | STOP" if stopped else "")
                )

        with db() as conn:
            conn.execute("""
                UPDATE discovered_markets
                SET resolved=1,winning_asset=?,winning_outcome=?
                WHERE condition_id=?
            """, (winning_asset, winning_outcome, cid))
            conn.commit()

        if cid in markets:
            markets[cid]["resolved"] = 1
        if messages:
            log.info("RESOLVED %s %s | %s", symbol, cid[-6:], " | ".join(messages))
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await tg_send(
                    f"✅ {symbol} MARKET SETTLED | {winning_outcome or winning_asset[-8:]}\n"
                    + "\n".join(messages)
                )


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
        others = [prices[i] for i in range(len(prices)) if i != best_idx]
        second = max(others) if others else -1
        closed = bool(market_row.get("closed", False))
        resolved_flag = bool(
            market_row.get("resolved", False)
            or market_row.get("umaResolutionStatus") == "resolved"
        )
        if best >= 0.999 and second <= 0.001 and (closed or resolved_flag or best >= 0.9999):
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
    embedded = event["markets"]
    for m in embedded:
        if isinstance(m, dict):
            cid = str(m.get("conditionId") or m.get("condition_id") or "")
            if cid == str(condition_id):
                return m
    if len(embedded) == 1 and isinstance(embedded[0], dict):
        return embedded[0]
    return None


async def resolution_fallback_loop():
    while True:
        try:
            cutoff = now_ts() - 10
            with db() as conn:
                rows = conn.execute("""
                    SELECT condition_id,slug,question,end_ts
                    FROM discovered_markets
                    WHERE resolved=0 AND end_ts<?
                    ORDER BY end_ts LIMIT 50
                """, (cutoff,)).fetchall()

            for row in rows:
                cid = str(row["condition_id"])
                slug = str(row["slug"] or "")
                if not slug:
                    continue
                m = await fetch_resolved_market_by_slug(slug, cid)
                if not m:
                    continue
                winning_asset, winning_outcome = resolve_winner_from_market(m)
                if winning_asset:
                    log.info("RESOLUTION FALLBACK %s | winner=%s", slug, winning_outcome or winning_asset[-8:])
                    await settle_market(cid, winning_asset, winning_outcome)
        except Exception:
            log.exception("Resolution fallback failed")
        await asyncio.sleep(10)




# ============================================================
# PAPER/LIVE ACCOUNTS + TELEGRAM CONTROL
# ============================================================

pending_live_confirmations = {}


def strategy_for(symbol, code):
    symbol = str(symbol).upper()
    code = str(code).upper()
    for v in STRATEGIES_BY_SYMBOL.get(symbol, []):
        if v.get("code") == code:
            return v
    return None


def open_condition_ids(strategy_name):
    with db() as conn:
        rows = conn.execute("""
            SELECT condition_id FROM paper_trades WHERE variant=? AND filled_shares>0
            UNION
            SELECT condition_id FROM live_orders WHERE variant=? AND filled_shares>0
        """, (strategy_name, strategy_name)).fetchall()
    out = []
    for r in rows:
        cid = str(r["condition_id"])
        with db() as conn:
            settled = conn.execute(
                "SELECT 1 FROM market_results WHERE condition_id=? AND variant=?",
                (cid, strategy_name),
            ).fetchone()
        if not settled and position_totals(cid, strategy_name)["remaining"] > 1e-8:
            out.append(cid)
    return out


def strategy_has_open_position(strategy_name):
    return bool(open_condition_ids(strategy_name))


def open_cost_basis(strategy_name):
    total = 0.0
    for cid in open_condition_ids(strategy_name):
        pos = position_totals(cid, strategy_name)
        if pos["bought"] > 1e-9 and pos["remaining"] > 1e-9:
            total += pos["buy_cost"] * pos["remaining"] / pos["bought"]
    return total


def account_stats(strategy_name):
    cash = paper_cash(strategy_name)
    initial = paper_initial(strategy_name)
    with db() as conn:
        realized = sf(conn.execute(
            "SELECT COALESCE(SUM(pnl),0) p FROM market_results WHERE variant=?", (strategy_name,)
        ).fetchone()["p"])
        traded = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0", (strategy_name,)
        ).fetchone()["c"])
        wins = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl>0", (strategy_name,)
        ).fetchone()["c"])
        losses = si(conn.execute(
            "SELECT COUNT(*) c FROM market_results WHERE variant=? AND (buy_trades+exit_trades)>0 AND pnl<0", (strategy_name,)
        ).fetchone()["c"])
        paper_buys = si(conn.execute(
            "SELECT COUNT(*) c FROM paper_trades WHERE variant=? AND filled_shares>0", (strategy_name,)
        ).fetchone()["c"])
        live_buys = si(conn.execute(
            "SELECT COUNT(*) c FROM live_orders WHERE variant=? AND action='BUY' AND filled_shares>0", (strategy_name,)
        ).fetchone()["c"])
        paper_fees = sf(conn.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM paper_trades WHERE variant=?", (strategy_name,)
        ).fetchone()["f"]) + sf(conn.execute(
            "SELECT COALESCE(SUM(fee),0) f FROM paper_exits WHERE variant=?", (strategy_name,)
        ).fetchone()["f"])
        tp_exits = si(conn.execute(
            "SELECT COUNT(*) c FROM paper_exits WHERE variant=? AND reason='TAKE_PROFIT'",
            (strategy_name,),
        ).fetchone()["c"]) + si(conn.execute(
            "SELECT COUNT(*) c FROM live_orders WHERE variant=? AND action='SELL' "
            "AND reason='TAKE_PROFIT' AND filled_shares>0",
            (strategy_name,),
        ).fetchone()["c"])
        live_fee_est = sf(conn.execute(
            "SELECT COALESCE(SUM(fee_estimate),0) f FROM live_orders WHERE variant=? AND filled_shares>0", (strategy_name,)
        ).fetchone()["f"])
        avg_win = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl>0", (strategy_name,)
        ).fetchone()["x"])
        avg_loss = sf(conn.execute(
            "SELECT COALESCE(AVG(pnl),0) x FROM market_results WHERE variant=? AND pnl<0", (strategy_name,)
        ).fetchone()["x"])
        gate_pass = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=1", (strategy_name,)
        ).fetchone()["c"])
        gate_skip = si(conn.execute(
            "SELECT COUNT(*) c FROM gate_decisions WHERE variant=? AND passed=0", (strategy_name,)
        ).fetchone()["c"])
    return {
        "initial": initial, "cash": cash, "open_cost": open_cost_basis(strategy_name),
        "realized": realized, "traded_markets": traded, "wins": wins, "losses": losses,
        "buy_trades": paper_buys + live_buys, "fees": paper_fees + live_fee_est,
        "take_profit_exits": tp_exits,
        "avg_win": avg_win, "avg_loss": avg_loss, "gate_pass": gate_pass, "gate_skip": gate_skip,
    }


def keyboard():
    return {
        "keyboard": [
            [{"text": "▶️ START"}, {"text": "⏹ STOP"}],
            [{"text": "🎛 MODES"}, {"text": "📐 SIZES"}],
            [{"text": "💰 BALANCE"}, {"text": "📈 POSITIONS"}],
            [{"text": "📊 STATISTICS"}, {"text": "📜 TRADES"}],
            [{"text": "🎯 TAKE PROFIT"}, {"text": "🔐 WALLET"}],
            [{"text": "➖ SCORE"}, {"text": "🎚 SCORE"}, {"text": "➕ SCORE"}],
            [{"text": "🚨 EMERGENCY STOP"}],
        ],
        "resize_keyboard": True,
    }


async def tg_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or session is None:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": str(text)[:4096], "reply_markup": keyboard()},
            timeout=aiohttp.ClientTimeout(total=20),
        ) as r:
            if r.status != 200:
                log.warning("Telegram message failed: %s", await r.text())
                return False
        return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def strategy_for(symbol, code=None):
    symbol = str(symbol).upper()
    arr = STRATEGIES_BY_SYMBOL.get(symbol, [])
    return arr[0] if arr else None


def strategy_status_line(v):
    return (
        f"{v['symbol']}: {strategy_mode(v['name'])} | "
        f"ENTRY {entry_shares(v):g}sh"
    )


def _source_summary():
    t = now_ms()
    parts = []
    for venue, enabled in (("binance", ENABLE_BINANCE), ("bybit", ENABLE_BYBIT), ("coinbase", ENABLE_COINBASE)):
        if not enabled:
            parts.append(f"{venue}=OFF")
            continue
        configured = [s for s in SYMBOLS if venue != "coinbase" or s in COINBASE_PRODUCTS]
        fresh = 0
        for s in configured:
            h = source_health[venue][s]
            age = t - si(h.get("last_ms")) if h.get("last_ms") else 999999
            if h.get("connected") and age <= SOURCE_FRESH_MS:
                fresh += 1
        parts.append(f"{venue}={fresh}/{len(configured)}")
    return " | ".join(parts)


async def send_modes():
    wallet_flag = "READY" if live_client_ready else f"NOT READY ({live_client_error or 'no credentials'})"
    await tg_send(
        "🎛 PRE-JUMP MODES\n"
        + "\n".join(strategy_status_line(v) for v in STRATEGIES)
        + f"\n\nScore: >= {prejump_score():.2f} | window {PREJUMP_MIN_ELAPSED:g}–{PREJUMP_MAX_ELAPSED:g}s"
        + f"\nPM ask: {PREJUMP_PRICE_MIN:.2f}–{PREJUMP_PRICE_MAX:.2f} | votes >= {PREJUMP_MIN_VENUES}"
        + f"\n{safe_first_signal_filter_text()}"
        + f"\nLIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'} | wallet: {wallet_flag}"
        + "\n\nCommands:"
        + "\nMODE BTC PAPER"
        + "\nMODE BTC OFF"
        + "\nMODE BTC LIVE"
        + "\nCONFIRM LIVE BTC"
    )


async def send_sizes():
    await tg_send(
        "📐 SHARE SIZES\n"
        + "\n".join(f"{v['symbol']}: {entry_shares(v):g} shares" for v in STRATEGIES)
        + "\n\nChange one token: SIZE BTC 5"
        + "\nChange all tokens: SIZE ALL 5"
        + "\nSize cannot change while that token has an open bot-tracked position."
    )


async def send_wallet():
    live_balance = await live_collateral_balance() if live_client_ready else None
    if live_client_ready and live_client is not None:
        wallet = str(getattr(live_client, "wallet", POLYMARKET_WALLET_ADDRESS))
        signer = str(getattr(live_client, "signer", ""))
        wallet_type = str(getattr(live_client, "wallet_type", ""))
    else:
        wallet = POLYMARKET_WALLET_ADDRESS or "not configured"
        signer = "n/a"
        wallet_type = "n/a"
    bal = f"${live_balance:.2f}" if live_balance is not None else "unavailable"
    await tg_send(
        "🔐 POLYMARKET WALLET\n"
        f"SDK: {'READY' if live_client_ready else 'NOT READY'}\n"
        f"LIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'}\n"
        f"Wallet: {wallet}\nSigner: {signer}\nType: {wallet_type}\nCollateral: {bal}\n"
        f"TP: {format_take_profit(take_profit_usdc())}\n"
        f"PRE-JUMP score: >= {prejump_score():.2f}\n"
        f"Error: {live_client_error or '-'}\n\nNever send the private key in Telegram."
    )


async def send_balance():
    live_balance = await live_collateral_balance() if live_client_ready else None
    lines = ["💰 PRE-JUMP BALANCE"]
    if live_balance is not None:
        lines.append(f"LIVE collateral: ${live_balance:.2f}")
    for v in STRATEGIES:
        s = account_stats(v["name"])
        lines.append(
            f"{v['symbol']} {strategy_mode(v['name'])}: PAPER cash ${s['cash']:.2f} | "
            f"realized bot PnL {s['realized']:+.2f}"
        )
    lines.append(f"Score >= {prejump_score():.2f} | TP {format_take_profit(take_profit_usdc())}")
    await tg_send("\n".join(lines))


def format_stats(v, s):
    denom = s["wins"] + s["losses"]
    wr = 100.0 * s["wins"] / denom if denom else 0.0
    return (
        f"{v['symbol']}: {strategy_mode(v['name'])} | PnL {s['realized']:+.2f} | "
        f"W/L {s['wins']}/{s['losses']} ({wr:.1f}%) | "
        f"buys {s['buy_trades']} | TP {s['take_profit_exits']}"
    )


async def send_statistics():
    lines = [
        "📊 PRE-JUMP STATISTICS",
        f"Entries: {'ON' if trading_enabled() else 'OFF'} | base score >= {prejump_score():.2f}",
        safe_first_signal_filter_text(),
    ]
    for v in STRATEGIES:
        lines.append(format_stats(v, account_stats(v["name"])))
    lines.append(f"\nSources: {_source_summary()}")
    await tg_send("\n".join(lines))


async def send_positions():
    lines = ["📈 OPEN PRE-JUMP POSITIONS"]
    found = False
    for v in STRATEGIES:
        for cid in open_condition_ids(v["name"]):
            pos = position_totals(cid, v["name"])
            if pos["remaining"] <= 1e-8:
                continue
            found = True
            mark = projected_full_exit(cid, v["name"])
            mark_txt = f" | exitPnL {mark['total_pnl']:+.2f}" if mark else " | exitPnL n/a"
            lines.append(
                f"{v['symbol']} {pos.get('execution_mode') or strategy_mode(v['name'])} "
                f"{pos['primary_outcome']} {pos['remaining']:.4f}sh | cost ${pos['buy_cost']:.2f}{mark_txt}"
            )
    if not found:
        lines.append("None")
    await tg_send("\n".join(lines))


async def send_trades():
    with db() as conn:
        rows = conn.execute("""
            SELECT trade_ms AS ms,variant,outcome,'PAPER BUY' action,filled_shares,avg_price
            FROM paper_trades
            UNION ALL
            SELECT exit_ms AS ms,variant,outcome,'PAPER TP' action,filled_shares,avg_price
            FROM paper_exits
            UNION ALL
            SELECT submitted_ms AS ms,variant,outcome,
                   ('LIVE ' || action || ' ' || reason) action,filled_shares,avg_price
            FROM live_orders
            WHERE filled_shares>0 OR status IN ('AMBIGUOUS','DELAYED_AMBIGUOUS')
            ORDER BY ms DESC LIMIT 30
        """).fetchall()
    lines = ["📜 LAST PRE-JUMP ACTIONS"]
    for r in rows:
        v = STRATEGY_BY_NAME.get(str(r["variant"]))
        symbol = v["symbol"] if v else str(r["variant"])
        dt = datetime.fromtimestamp(sf(r["ms"])/1000.0, tz=timezone.utc).strftime("%m-%d %H:%M:%S")
        lines.append(
            f"{dt} {symbol} {r['action']} {r['outcome']} "
            f"{sf(r['filled_shares']):.4f}sh @ {sf(r['avg_price']):.4f}"
        )
    if not rows:
        lines.append("No trades yet.")
    await tg_send("\n".join(lines))


def _set_mode_direct(strategy, mode):
    mode = str(mode).upper()
    if mode not in {"PAPER", "LIVE", "OFF"}:
        return False, "invalid mode"
    current = strategy_mode(strategy["name"])
    if current == mode:
        return True, f"already {mode}"
    if mode != "OFF" and strategy_has_open_position(strategy["name"]):
        for cid in open_condition_ids(strategy["name"]):
            pos_mode = position_totals(cid, strategy["name"]).get("execution_mode")
            if pos_mode and pos_mode != mode:
                return False, f"open {pos_mode} position: switch to {mode} blocked until flat"
    state_set(f"mode:{strategy['name']}", mode)
    return True, mode


pending_live_confirmations = {}


async def request_live(symbol):
    symbol = str(symbol).upper()
    v = strategy_for(symbol)
    if not v:
        await tg_send("Unknown token. Use BTC/XRP/BNB/SOL/ETH/DOGE/HYPE.")
        return
    if not LIVE_MASTER_ENABLE:
        await tg_send("🔒 LIVE_MASTER_ENABLE=0. Set it to 1 in hosting Environment and redeploy first.")
        return
    if not live_client_ready:
        await tg_send(f"🔒 Wallet SDK is not ready: {live_client_error or 'credentials missing'}")
        return
    if strategy_has_open_position(v["name"]) and strategy_mode(v["name"]) != "LIVE":
        await tg_send(f"🔒 {symbol} has an open position; mode switch blocked until flat.")
        return
    pending_live_confirmations[symbol] = time.time() + 60
    extra = (
        f"\n{safe_first_signal_filter_text()}."
        if symbol == "BTC" else ""
    )
    await tg_send(
        f"⚠️ REAL MONEY confirmation for {symbol}.\n"
        f"Current PRE-JUMP base score >= {prejump_score():.2f}; ENTRY {entry_shares(v):g} shares."
        f"{extra}\n"
        f"Send exactly: CONFIRM LIVE {symbol}\nExpires in 60 seconds."
    )


async def confirm_live(symbol):
    symbol = str(symbol).upper()
    expiry = pending_live_confirmations.pop(symbol, 0)
    if expiry < time.time():
        await tg_send("LIVE confirmation missing or expired. Use MODE <TOKEN> LIVE again.")
        return
    v = strategy_for(symbol)
    if not v or not LIVE_MASTER_ENABLE or not live_client_ready:
        await tg_send("LIVE cannot be enabled: wallet/master not ready.")
        return
    ok, msg = _set_mode_direct(v, "LIVE")
    await tg_send(f"🔴 {symbol} = LIVE" if ok else f"LIVE switch blocked: {msg}")
    if ok and LIVE_PRESIGN_PREWARM_ENABLE:
        # Background/local only. It never posts an order and skips active-slot
        # warming while entries are ON.
        asyncio.create_task(prewarm_symbol_current_or_next(symbol))


async def send_take_profit():
    current = take_profit_usdc()
    await tg_send(
        "🎯 TAKE PROFIT\n"
        f"Current: {format_take_profit(current)}\n"
        f"ENV/default: {format_take_profit(TAKE_PROFIT_USDC)}\n\n"
        "Change immediately: TP 0.60 | TP 0.90 | TP 1.00\n"
        "Disable: TP OFF\nReturn to ENV: TP ENV\n\n"
        "Target is NET profit for the whole remaining position; entry and projected exit fees are included."
    )


def _telegram_tp_value(token):
    text = str(token or "").strip().upper()
    if text in {"OFF", "NONE", "DISABLED", "0", "0.0", "0.00"}:
        return "OFF", None
    if text in {"ENV", "DEFAULT"}:
        return "ENV", TAKE_PROFIT_USDC
    cleaned = text.replace("$", "").replace(",", ".")
    value = float(cleaned)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("must be greater than 0")
    return "VALUE", value


async def set_take_profit_from_telegram(token):
    old = take_profit_usdc()
    try:
        _, value = _telegram_tp_value(token)
        new = set_take_profit_usdc(value)
    except Exception:
        await tg_send("❌ Invalid TP. Examples: TP 0.60 | TP OFF | TP ENV")
        return
    await tg_send(
        f"✅ TAKE PROFIT UPDATED\n{format_take_profit(old)} → {format_take_profit(new)}\n"
        "Applied immediately to open not-yet-latched positions and future positions."
    )


async def send_score():
    await tg_send(
        "🎚 PRE-JUMP SCORE\n"
        f"Current entry threshold: >= {prejump_score():.2f}\n"
        f"Allowed range: {PREJUMP_SCORE_MIN:.2f}–{PREJUMP_SCORE_MAX:.2f}\n"
        f"ENV/default: {PREJUMP_SCORE_ENV:.2f}\n\n"
        "Buttons ➖/➕ change by 0.01.\n"
        "Exact value: SCORE 0.42\nReturn to ENV: SCORE ENV\n\n"
        "The bot never allows a threshold below 0.40."
    )


async def set_score_from_telegram(token):
    old = prejump_score()
    text = str(token or "").strip().upper()
    try:
        if text in {"ENV", "DEFAULT"}:
            value = PREJUMP_SCORE_ENV
        elif text.startswith("+") or text.startswith("-"):
            value = old + float(text.replace(",", "."))
        else:
            value = float(text.replace(",", "."))
        new = set_prejump_score(value)
    except Exception:
        await tg_send(
            f"❌ Invalid SCORE. Minimum is {PREJUMP_SCORE_MIN:.2f}. "
            "Examples: SCORE 0.42 | SCORE +0.01 | SCORE -0.01 | SCORE ENV"
        )
        return
    await tg_send(
        f"✅ PRE-JUMP SCORE UPDATED\n{old:.2f} → {new:.2f}\n"
        "Applied immediately to future entry signals; open positions are unchanged."
    )


async def handle_tg(text):
    raw = str(text or "").strip()
    cmd = raw.upper()
    parts = cmd.split()

    if cmd in {"/START", "▶️ START", "START"}:
        state_set("trading_enabled", "1")
        await tg_send(
            "▶️ PRE-JUMP STARTED\n"
            f"Base score >= {prejump_score():.2f} | ask {PREJUMP_PRICE_MIN:.2f}–{PREJUMP_PRICE_MAX:.2f} | "
            f"window {PREJUMP_MIN_ELAPSED:g}–{PREJUMP_MAX_ELAPSED:g}s\n"
            f"{safe_first_signal_filter_text()}\n"
            f">= {PREJUMP_MIN_VENUES} same-side venues | ENTRY default {ENTRY_ORDER_SIZE:g}sh\n"
            f"TP {format_take_profit(take_profit_usdc())} | no DCA | no stop-loss."
        )
        return

    if cmd in {"⏹ STOP", "STOP", "/STOP"}:
        state_set("trading_enabled", "0")
        await tg_send("⏹ New PRE-JUMP entries stopped globally. TP monitoring continues for open positions.")
        return

    if cmd in {"🚨 EMERGENCY STOP", "EMERGENCY STOP", "/EMERGENCY"}:
        state_set("trading_enabled", "0")
        pending_live_confirmations.clear()
        await tg_send(
            "🚨 EMERGENCY STOP ACTIVE\nNew entries are blocked immediately and pending LIVE confirmations cleared.\n"
            "Existing positions are NOT force-sold; configured take-profit monitoring continues."
        )
        return

    if cmd in {"💰 BALANCE", "BALANCE", "/BALANCE"}: await send_balance(); return
    if cmd in {"📊 STATISTICS", "STATISTICS", "/STATS"}: await send_statistics(); return
    if cmd in {"📈 POSITIONS", "POSITIONS"}: await send_positions(); return
    if cmd in {"📜 TRADES", "TRADES"}: await send_trades(); return
    if cmd in {"🎛 MODES", "MODES"}: await send_modes(); return
    if cmd in {"📐 SIZES", "SIZES"}: await send_sizes(); return
    if cmd in {"🔐 WALLET", "WALLET", "/WALLET"}: await send_wallet(); return
    if cmd in {"🎯 TAKE PROFIT", "TAKE PROFIT", "TAKEPROFIT", "TP", "/TP"}: await send_take_profit(); return
    if cmd in {"🎚 SCORE", "SCORE", "/SCORE"}: await send_score(); return
    if cmd == "➕ SCORE": await set_score_from_telegram("+0.01"); return
    if cmd == "➖ SCORE": await set_score_from_telegram("-0.01"); return

    # SCORE 0.42 / SCORE +0.01 / SCORE ENV
    if len(parts) == 2 and parts[0] in {"SCORE", "/SCORE"}:
        await set_score_from_telegram(parts[1]); return

    # TP 0.60 / TP OFF / TP ENV
    if len(parts) == 2 and parts[0] in {"TP", "/TP", "TAKEPROFIT"}:
        await set_take_profit_from_telegram(parts[1]); return
    if len(parts) == 3 and parts[0] == "TAKE" and parts[1] == "PROFIT":
        await set_take_profit_from_telegram(parts[2]); return

    # MODE BTC LIVE/PAPER/OFF
    if len(parts) == 3 and parts[0] == "MODE" and parts[1] in SYMBOLS:
        v = strategy_for(parts[1])
        mode = parts[2]
        if mode == "LIVE":
            await request_live(parts[1]); return
        if mode in {"PAPER", "OFF"}:
            ok, msg = _set_mode_direct(v, mode)
            await tg_send(f"{'🟢' if mode == 'PAPER' else '⛔'} {parts[1]}: {msg}")
            return

    # CONFIRM LIVE BTC
    if len(parts) == 3 and parts[0] == "CONFIRM" and parts[1] == "LIVE" and parts[2] in SYMBOLS:
        await confirm_live(parts[2]); return

    # SIZE BTC 5 / SIZE ALL 5
    if len(parts) == 3 and parts[0] == "SIZE":
        target = parts[1]
        try:
            shares = float(parts[2].replace(",", "."))
        except Exception:
            shares = -1
        if not _valid_user_shares(shares):
            await tg_send(f"❌ Size must be {LIVE_MIN_SHARES:g}..{LIVE_MAX_SHARES_PER_ORDER:g} shares.")
            return
        targets = STRATEGIES if target == "ALL" else ([strategy_for(target)] if target in SYMBOLS else [])
        targets = [v for v in targets if v]
        if not targets:
            await tg_send("Unknown token. Example: SIZE BTC 5 or SIZE ALL 5")
            return
        blocked = [v["symbol"] for v in targets if strategy_has_open_position(v["name"])]
        if blocked:
            await tg_send("🔒 Size change blocked; open position: " + ", ".join(blocked))
            return
        for v in targets:
            state_set(f"entry_shares:{v['name']}", f"{shares:.10g}")
        await tg_send(f"✅ ENTRY size = {shares:g}sh for " + ("ALL" if target == "ALL" else target))
        return

    await tg_send(
        "Commands:\n"
        "MODE BTC PAPER/LIVE/OFF\nCONFIRM LIVE BTC\n"
        "SIZE BTC 5 | SIZE ALL 5\n"
        "SCORE 0.42 | SCORE +0.01 | SCORE ENV\n"
        "TP 0.60 | TP OFF | TP ENV"
    )


async def telegram_loop():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured")
        return
    offset = 0
    await tg_send(
        f"🤖 {VERSION} online\n"
        f"Tokens: {', '.join(SYMBOLS)} | one PRE-JUMP strategy/token\n"
        f"Entries: {'ON' if trading_enabled() else 'OFF'} | default modes PAPER\n"
        f"Base score >= {prejump_score():.2f} | window {PREJUMP_MIN_ELAPSED:g}–{PREJUMP_MAX_ELAPSED:g}s\n"
        f"{safe_first_signal_filter_text()}\n"
        f"ENTRY default {ENTRY_ORDER_SIZE:g}sh | TP {format_take_profit(take_profit_usdc())}\n"
        f"Wallet: {'READY' if live_client_ready else 'NOT READY'} | LIVE master: {'ON' if LIVE_MASTER_ENABLE else 'OFF'}\n"
        f"Sources: {_source_summary()}"
    )
    while True:
        try:
            async with session.get(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=aiohttp.ClientTimeout(total=35),
            ) as response:
                data = await response.json()
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


async def health(request):
    t = now_ms()
    source_status = {}
    for venue in ("binance", "bybit", "coinbase"):
        source_status[venue] = {}
        for symbol in SYMBOLS:
            if venue == "coinbase" and symbol not in COINBASE_PRODUCTS:
                continue
            h = source_health[venue][symbol]
            age = t - si(h.get("last_ms")) if h.get("last_ms") else None
            source_status[venue][symbol] = {
                "connected": bool(h.get("connected")),
                "age_ms": age,
                "fresh": bool(h.get("connected")) and age is not None and age <= SOURCE_FRESH_MS,
                "errors": si(h.get("errors")),
            }
    return web.json_response({
        "ok": True,
        "version": VERSION,
        "paper_live": True,
        "trading_enabled": trading_enabled(),
        "live_master_enable": LIVE_MASTER_ENABLE,
        "live_client_ready": live_client_ready,
        "live_client_error": live_client_error,
        "symbols": SYMBOLS,
        "prejump": {
            "score": prejump_score(),
            "score_min": PREJUMP_SCORE_MIN,
            "price": [PREJUMP_PRICE_MIN, PREJUMP_PRICE_MAX],
            "pm_momentum_1s": [PREJUMP_PM_MOM_MIN, PREJUMP_PM_MOM_MAX],
            "min_same_side_venues": PREJUMP_MIN_VENUES,
            "require_binance_bybit": PREJUMP_REQUIRE_BINANCE_BYBIT,
            "elapsed_sec": [PREJUMP_MIN_ELAPSED, PREJUMP_MAX_ELAPSED],
            "fast_interval": FAST_INTERVAL,
            "first_signal_safe_filters": {
                "BTC": {"min_score": BTC_PREJUMP_SCORE, "max_signal_ask": BTC_PREJUMP_PRICE_MAX},
                "SOL": {"max_pm_momentum_1s": SOL_PREJUMP_PM_MOM_MAX},
                "XRP": {"min_score": XRP_PREJUMP_SCORE},
                "BNB": {"min_pm_momentum_1s": BNB_PREJUMP_PM_MOM_MIN},
                "DOGE": {"max_spread": DOGE_PREJUMP_MAX_SPREAD},
                "ETH": {"min_score": ETH_PREJUMP_SCORE, "max_pm_momentum_1s": ETH_PREJUMP_PM_MOM_MAX},
                "HYPE": {"filter": None, "mode": "base_prejump_only"},
                "semantics": "first otherwise-valid base PRE-JUMP candidate; skip market permanently on failure",
            },
        },
        "take_profit_usdc_net": take_profit_usdc(),
        "modes": {v["symbol"]: strategy_mode(v["name"]) for v in STRATEGIES},
        "entry_shares": {v["symbol"]: entry_shares(v) for v in STRATEGIES},
        "sources": source_status,
        "markets_tracked": len(markets),
        "assets_subscribed": len(subscribed_assets),
        "books": len(books),
        "memory_rss_mb": current_rss_mb(),
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
        "User-Agent": f"PreJumpPaperLive/{VERSION}",
        "Accept": "application/json",
    })
    await init_live_client()
    # One safe/read-only warmup before any trading task starts, so it cannot
    # compete with a latency-sensitive order on process startup.
    if LIVE_PREWARM_ENABLE:
        await prewarm_live_transport("startup")

    tasks = [
        asyncio.create_task(web_server()),
        asyncio.create_task(discovery_loop()),
        asyncio.create_task(ws_loop()),
        asyncio.create_task(strategy_loop()),
        asyncio.create_task(resolution_fallback_loop()),
        asyncio.create_task(telegram_loop()),
        asyncio.create_task(memory_maintenance_loop()),
    ]
    if LIVE_PREWARM_ENABLE or LIVE_PRESIGN_PREWARM_ENABLE:
        tasks.append(asyncio.create_task(live_prewarm_loop()))
    if EVENT_DRIVEN_LIVE_ENTRY:
        tasks += [asyncio.create_task(event_driven_symbol_loop(symbol)) for symbol in SYMBOLS]
    if ENABLE_BINANCE:
        tasks += [asyncio.create_task(binance_symbol_loop(symbol)) for symbol in SYMBOLS]
    if ENABLE_BYBIT:
        tasks += [asyncio.create_task(bybit_symbol_loop(symbol)) for symbol in SYMBOLS]
    if ENABLE_COINBASE and COINBASE_PRODUCTS:
        tasks.append(asyncio.create_task(coinbase_loop()))

    log.info(
        "%s started | symbols=%s | score>=%.2f | window=%.0f..%.0fs | fast=%.2fs | "
        "event_live=%s/%dms | slippage=%.2f | retry=%d/%dms/rest=%s | transport_prewarm=%s/%.0fs | presign_prewarm=%s/%.0fs | TP=%s | live_master=%s | wallet=%s | trading=%s",
        VERSION, ",".join(SYMBOLS), prejump_score(), PREJUMP_MIN_ELAPSED, PREJUMP_MAX_ELAPSED,
        FAST_INTERVAL, "ON" if EVENT_DRIVEN_LIVE_ENTRY else "OFF", EVENT_DRIVEN_MIN_INTERVAL_MS,
        LIVE_ENTRY_MAX_SLIPPAGE, LIVE_ENTRY_NO_MATCH_RETRIES, LIVE_ENTRY_RETRY_DELAY_MS,
        "ON" if LIVE_ENTRY_RETRY_FORCE_REST else "OFF",
        "ON" if LIVE_PREWARM_ENABLE else "OFF", LIVE_PREWARM_INTERVAL_SEC,
        "ON" if LIVE_PRESIGN_PREWARM_ENABLE else "OFF", LIVE_PRESIGN_PREWARM_LEAD_SEC,
        format_take_profit(take_profit_usdc()),
        "ON" if LIVE_MASTER_ENABLE else "OFF",
        "READY" if live_client_ready else "NOT READY",
        "ON" if trading_enabled() else "OFF",
    )

    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await close_live_client()
        if session:
            await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
