"""
Источник данных о цене BTC — публичный REST API Binance (ключ не нужен,
данные о рынке открытые). Используется и для текущей цены, и для истории
свечей под индикаторы, и для получения цены на конкретный момент времени
(момент открытия 15-минутного рынка на Polymarket = "страйк").
"""
from __future__ import annotations
import httpx
import pandas as pd

from config import settings

_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_base", "taker_quote", "ignore",
]


async def get_klines(limit: int = 100, interval: str | None = None,
                      start_time_ms: int | None = None) -> pd.DataFrame:
    """Тянем свечи с Binance и приводим к DataFrame с числовыми колонками."""
    params = {
        "symbol": settings.BINANCE_SYMBOL,
        "interval": interval or settings.KLINE_INTERVAL,
        "limit": limit,
    }
    if start_time_ms is not None:
        params["startTime"] = start_time_ms

    url = f"{settings.BINANCE_BASE_URL}/api/v3/klines"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        raw = resp.json()

    df = pd.DataFrame(raw, columns=_COLUMNS)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df


async def get_price_at(timestamp_sec: int) -> float:
    """
    Цена BTC на начало минуты, в которую стартовал рынок на Polymarket.
    Это и есть "страйк" для 15-минутного Up/Down рынка — рынок резолвится
    как Up, если цена в конце окна >= цены в начале окна.
    """
    minute_start_ms = (timestamp_sec // 60) * 60 * 1000
    df = await get_klines(limit=1, interval="1m", start_time_ms=minute_start_ms)
    if df.empty:
        raise RuntimeError(f"Binance не вернул свечу для timestamp={timestamp_sec}")
    return float(df.iloc[0]["open"])


async def get_last_price() -> float:
    url = f"{settings.BINANCE_BASE_URL}/api/v3/ticker/price"
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params={"symbol": settings.BINANCE_SYMBOL})
        resp.raise_for_status()
        return float(resp.json()["price"])
