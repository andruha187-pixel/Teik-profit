"""
Логика принятия решения о входе.

Идея: заходим по 0.87–0.95 только когда расхождение цены от страйка
статистически значимо (в единицах ATR), тренд подтверждает направление,
волатильность не находится в аномальном всплеске (иначе высок риск
резкого разворота за оставшееся время), и в стакане достаточно ликвидности
на нужной стороне. Каждый фактор даёт вклад в 0–100 safety score;
входим, только если score >= порога И цена в целевом диапазоне.
"""
from __future__ import annotations
from dataclasses import dataclass, field

from config import settings
from src import runtime_state
from src.polymarket_client import OrderBookSnapshot


@dataclass
class Decision:
    should_enter: bool
    direction: str | None          # "UP" / "DOWN" / None
    entry_price: float | None
    safety_score: float
    minutes_left: float
    distance_atr: float
    reasons: list[str] = field(default_factory=list)


def _score_time_window(minutes_left: float) -> float:
    """Идеальное окно входа — не слишком рано (мало данных о расхождении),
    не слишком поздно (риск не успеть исполниться / нет времени на анализ)."""
    if minutes_left < settings.MIN_MINUTES_LEFT or minutes_left > settings.MAX_MINUTES_LEFT:
        return 0.0
    mid = (settings.MIN_MINUTES_LEFT + settings.MAX_MINUTES_LEFT) / 2
    span = (settings.MAX_MINUTES_LEFT - settings.MIN_MINUTES_LEFT) / 2
    closeness = 1 - abs(minutes_left - mid) / span if span > 0 else 1.0
    return max(0.0, min(1.0, closeness)) * 100


def _score_distance(distance_atr: float) -> float:
    """Чем больше расхождение цены от страйка в ATR — тем увереннее направление,
    но с насыщением (после ~3 ATR дальнейший рост мало что добавляет)."""
    if distance_atr < settings.ATR_DISTANCE_MULT:
        return 0.0
    capped = min(distance_atr, 3.0)
    return ((capped - settings.ATR_DISTANCE_MULT) / (3.0 - settings.ATR_DISTANCE_MULT)) * 100


def _score_trend_alignment(direction: str, ema_fast_slope: float, trend_up: bool) -> float:
    direction_up = direction == "UP"
    slope_agrees = (ema_fast_slope > 0) == direction_up
    trend_agrees = trend_up == direction_up
    if slope_agrees and trend_agrees:
        return 100.0
    if slope_agrees or trend_agrees:
        return 50.0
    return 0.0


def _score_volatility_regime(atr_ratio_to_avg: float) -> float:
    """Аномальный всплеск ATR (относительно среднего) — сигнал повышенного
    риска разворота, режем score резко."""
    if atr_ratio_to_avg >= settings.ATR_SPIKE_MULT:
        return 0.0
    if atr_ratio_to_avg <= 1.0:
        return 100.0
    span = settings.ATR_SPIKE_MULT - 1.0
    return max(0.0, (settings.ATR_SPIKE_MULT - atr_ratio_to_avg) / span) * 100


def _score_liquidity(book: OrderBookSnapshot, needed_usdc: float) -> float:
    if book.ask_liquidity_usdc <= 0:
        return 0.0
    if book.ask_liquidity_usdc >= needed_usdc * 3:
        return 100.0
    return max(0.0, book.ask_liquidity_usdc / (needed_usdc * 3)) * 100


def evaluate(
    current_price: float,
    strike_price: float,
    minutes_left: float,
    indicators: dict,
    up_book: OrderBookSnapshot,
    down_book: OrderBookSnapshot,
) -> Decision:
    reasons = []

    direction = "UP" if current_price >= strike_price else "DOWN"
    book = up_book if direction == "UP" else down_book

    if book.best_ask is None:
        return Decision(False, direction, None, 0.0, minutes_left, 0.0, ["нет asks в стакане"])

    min_entry = runtime_state.get("min_entry_price")
    max_entry = runtime_state.get("max_entry_price")
    score_threshold = runtime_state.get("safety_score_threshold")
    trade_size = runtime_state.get("trade_size_usdc")

    if not (min_entry <= book.best_ask <= max_entry):
        reasons.append(f"цена {book.best_ask:.3f} вне диапазона [{min_entry}, {max_entry}]")

    atr = indicators["atr"] or 1e-9
    distance_atr = abs(current_price - strike_price) / atr

    time_score = _score_time_window(minutes_left)
    distance_score = _score_distance(distance_atr)
    trend_score = _score_trend_alignment(direction, indicators["ema_fast_slope"], indicators["trend_up"])
    vol_score = _score_volatility_regime(indicators["atr_ratio_to_avg"])
    liq_score = _score_liquidity(book, trade_size)

    weights = {"time": 0.20, "distance": 0.30, "trend": 0.20, "volatility": 0.20, "liquidity": 0.10}
    safety_score = (
        time_score * weights["time"]
        + distance_score * weights["distance"]
        + trend_score * weights["trend"]
        + vol_score * weights["volatility"]
        + liq_score * weights["liquidity"]
    )

    if time_score == 0:
        reasons.append(f"вне временного окна входа ({minutes_left:.1f} мин осталось)")
    if distance_score == 0:
        reasons.append(f"расхождение {distance_atr:.2f} ATR ниже порога {settings.ATR_DISTANCE_MULT}")
    if vol_score < 30:
        reasons.append(f"аномальная волатильность (ATR/avg={indicators['atr_ratio_to_avg']:.2f})")
    if trend_score < 50:
        reasons.append("тренд EMA не подтверждает направление")
    if liq_score < 50:
        reasons.append("недостаточно ликвидности в стакане")

    price_in_range = min_entry <= book.best_ask <= max_entry
    should_enter = price_in_range and safety_score >= score_threshold

    return Decision(
        should_enter=should_enter,
        direction=direction,
        entry_price=book.best_ask,
        safety_score=round(safety_score, 1),
        minutes_left=round(minutes_left, 2),
        distance_atr=round(distance_atr, 2),
        reasons=reasons,
    )
