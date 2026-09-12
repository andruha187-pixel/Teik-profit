"""
Расчёт технических индикаторов поверх OHLC-данных с биржи.
Используем pandas — минимум кода, минимум шансов на ошибку в формулах.
"""
from __future__ import annotations
import pandas as pd


def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Average True Range по стандартной формуле (Wilder).
    df должен содержать колонки high, low, close.
    """
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1 / period, adjust=False).mean()


def compute_indicator_snapshot(df: pd.DataFrame, atr_period: int, ema_fast: int,
                                ema_slow: int, atr_regime_lookback: int) -> dict:
    """
    Возвращает срез индикаторов на последней свече: ATR, EMA fast/slow,
    наклон EMA fast, и отношение текущего ATR к его среднему (для детекта
    аномального всплеска волатильности).
    """
    df = df.copy()
    df["atr"] = atr(df, atr_period)
    df["ema_fast"] = ema(df["close"], ema_fast)
    df["ema_slow"] = ema(df["close"], ema_slow)

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    ema_fast_slope = last["ema_fast"] - prev["ema_fast"]
    trend_up = last["ema_fast"] > last["ema_slow"]

    atr_avg = df["atr"].tail(atr_regime_lookback).mean()
    atr_ratio = float(last["atr"] / atr_avg) if atr_avg and atr_avg > 0 else 1.0

    return {
        "close": float(last["close"]),
        "atr": float(last["atr"]),
        "atr_ratio_to_avg": atr_ratio,
        "ema_fast": float(last["ema_fast"]),
        "ema_slow": float(last["ema_slow"]),
        "ema_fast_slope": float(ema_fast_slope),
        "trend_up": bool(trend_up),
    }
