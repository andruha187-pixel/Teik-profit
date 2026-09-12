"""
Централизованная конфигурация бота. Все настройки берутся из переменных
окружения (.env), см. .env.example. Ничего не хардкодим — особенно ключи.
"""
import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _get_float(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _get_int(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


@dataclass
class Settings:
    # --- Асset / market ---
    ASSET: str = os.getenv("ASSET", "btc")                 # btc / eth / sol / xrp
    MARKET_INTERVAL_MIN: int = _get_int("MARKET_INTERVAL_MIN", 15)

    # --- Binance (источник цены/индикаторов) ---
    BINANCE_SYMBOL: str = os.getenv("BINANCE_SYMBOL", "BTCUSDT")
    BINANCE_BASE_URL: str = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
    KLINE_INTERVAL: str = os.getenv("KLINE_INTERVAL", "1m")
    ATR_PERIOD: int = _get_int("ATR_PERIOD", 14)
    EMA_FAST: int = _get_int("EMA_FAST", 9)
    EMA_SLOW: int = _get_int("EMA_SLOW", 21)
    ATR_LOOKBACK_FOR_REGIME: int = _get_int("ATR_LOOKBACK_FOR_REGIME", 60)

    # --- Polymarket / CLOB ---
    POLY_HOST: str = os.getenv("POLY_HOST", "https://clob.polymarket.com")
    GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
    POLY_CHAIN_ID: int = _get_int("POLY_CHAIN_ID", 137)
    POLY_PRIVATE_KEY: str = os.getenv("POLY_PRIVATE_KEY", "")
    POLY_FUNDER_ADDRESS: str = os.getenv("POLY_FUNDER_ADDRESS", "")
    # 0 = EOA/MetaMask, 1 = email/Magic wallet, 2 = browser wallet proxy
    POLY_SIGNATURE_TYPE: int = _get_int("POLY_SIGNATURE_TYPE", 0)

    # --- Стратегия входа ---
    MIN_ENTRY_PRICE: float = _get_float("MIN_ENTRY_PRICE", 0.87)
    MAX_ENTRY_PRICE: float = _get_float("MAX_ENTRY_PRICE", 0.95)
    MIN_MINUTES_LEFT: float = _get_float("MIN_MINUTES_LEFT", 2.0)
    MAX_MINUTES_LEFT: float = _get_float("MAX_MINUTES_LEFT", 9.0)
    ATR_DISTANCE_MULT: float = _get_float("ATR_DISTANCE_MULT", 1.3)
    ATR_SPIKE_MULT: float = _get_float("ATR_SPIKE_MULT", 2.2)
    SAFETY_SCORE_THRESHOLD: float = _get_float("SAFETY_SCORE_THRESHOLD", 75.0)
    MIN_BOOK_LIQUIDITY_USDC: float = _get_float("MIN_BOOK_LIQUIDITY_USDC", 25.0)

    # --- Латентность / исполнение (прогрев стакана и транспорта) ---
    USE_LIVE_BOOK_STREAM: bool = _get_bool("USE_LIVE_BOOK_STREAM", True)
    LIVE_ENTRY_MAX_SLIPPAGE: float = _get_float("LIVE_ENTRY_MAX_SLIPPAGE", 0.01)
    MAX_ENTRY_EXECUTION_PRICE: float = _get_float("MAX_ENTRY_EXECUTION_PRICE", 0.97)

    # --- Управление капиталом ---
    TRADE_SIZE_USDC: float = _get_float("TRADE_SIZE_USDC", 10.0)
    MAX_OPEN_POSITIONS: int = _get_int("MAX_OPEN_POSITIONS", 1)
    DAILY_LOSS_LIMIT_USDC: float = _get_float("DAILY_LOSS_LIMIT_USDC", 50.0)

    # --- Масштабирование ставки по уверенности сигнала ---
    # Ставка = TRADE_SIZE_USDC только при score >= SIZE_SCALING_MAX_SCORE.
    # На самом пороге (score == threshold) ставка = TRADE_SIZE_USDC * MIN_FRACTION.
    # Между ними — линейная интерполяция. Так пограничные сигналы (score чуть
    # выше порога) автоматически получают меньшую ставку, а не полный размер.
    SIZE_SCALING_MIN_FRACTION: float = _get_float("SIZE_SCALING_MIN_FRACTION", 0.3)
    SIZE_SCALING_MAX_SCORE: float = _get_float("SIZE_SCALING_MAX_SCORE", 95.0)

    # --- Режим работы ---
    DRY_RUN: bool = _get_bool("DRY_RUN", True)          # True = только сигналы, ордера не шлём
    POLL_INTERVAL_SECONDS: int = _get_int("POLL_INTERVAL_SECONDS", 5)

    # --- Telegram ---
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")

    # --- Storage ---
    DB_PATH: str = os.getenv("DB_PATH", "data/bot.db")


settings = Settings()
