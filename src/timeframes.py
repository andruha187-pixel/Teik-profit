"""
Профили таймфреймов. Каждый актив торгуется независимо на каждом
таймфрейме из этого списка — итого len(ASSETS) x len(TIMEFRAMES)
параллельных потоков (см. main.py).

Почему у часового таймфрейма другие параметры, а не те же самые числа,
растянутые на час:

- **Индикаторы на 5-минутных свечах, а не 1-минутных.** 15-минутный рынок
  и ATR(14)/EMA(9,21) на 1m свечах соразмерны (14-21 минута истории на
  окно в 15 минут). Для часового рынка 1m-индикаторы были бы слишком
  шумными относительно масштаба окна — используем 5m свечи, тогда ATR(14)
  и EMA(9/21) агрегируют ~45-105 минут истории, что уже сопоставимо с
  часовым горизонтом.
- **Окно входа шире и позже относительно длины рынка**: 10-45 из 60 минут
  (то есть между 17-й и 50-й минутой) вместо 2-9 из 15. Логика та же
  пропорция "не слишком рано / не слишком поздно", просто пересчитанная
  под более длинное окно с поправкой на то, что часовому рынку требуется
  больше времени, чтобы цена успела статистически значимо разойтись от
  страйка, и одновременно есть больше времени на потенциальный разворот
  ближе к экспирации, так что верхняя граница (сколько минут ДО конца всё
  ещё можно входить) не растягивается пропорционально — оставляем более
  консервативный запас на развороты в последние 10 минут.
- **discovery_mode="hourly_et_named"**: у часовых рынков Polymarket
  СОВСЕМ ДРУГОЙ формат слага, не unix-таймстемп, а человекочитаемый и
  привязанный к Eastern Time: `bitcoin-up-or-down-september-13-2026-8pm-et`.
  Плюс часть активов называется полным именем, а не тикером (bitcoin,
  ethereum, solana — но xrp, bnb, hype как тикер). Обнаружено эмпирически
  через сайт Polymarket, не из документации — см. src/market_discovery.py.
- **Опрос реже** (30с вместо 5с) — часовой рынок меняется гораздо
  медленнее, незачем дёргать API так же часто, как для 15-минутного.

Пороги можно переопределить через .env (см. .env.example), но дефолты —
разумная отправная точка, а не проверенный оптимум ни для одного из
таймфреймов.
"""
from __future__ import annotations
from dataclasses import dataclass
import os


def _f(name: str, default: float) -> float:
    val = os.getenv(name)
    return float(val) if val else default


def _i(name: str, default: int) -> int:
    val = os.getenv(name)
    return int(val) if val else default


def _s(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class TimeframeProfile:
    label: str                     # "15m" / "1h" — используется в market_discovery и слагах
    interval_minutes: int
    kline_interval: str            # свечи Binance для индикаторов
    atr_period: int
    ema_fast: int
    ema_slow: int
    atr_lookback_for_regime: int
    min_minutes_left: float
    max_minutes_left: float
    atr_distance_mult: float
    atr_spike_mult: float
    discovery_mode: str            # "deterministic" | "hourly_et_named" | "series"
    poll_interval_seconds: int


_ALL_TIMEFRAMES: list[TimeframeProfile] = [
    TimeframeProfile(
        label="15m",
        interval_minutes=15,
        kline_interval=_s("TF_15M_KLINE_INTERVAL", "1m"),
        atr_period=_i("TF_15M_ATR_PERIOD", 14),
        ema_fast=_i("TF_15M_EMA_FAST", 9),
        ema_slow=_i("TF_15M_EMA_SLOW", 21),
        atr_lookback_for_regime=_i("TF_15M_ATR_LOOKBACK", 60),
        min_minutes_left=_f("TF_15M_MIN_MINUTES_LEFT", 2.0),
        max_minutes_left=_f("TF_15M_MAX_MINUTES_LEFT", 9.0),
        atr_distance_mult=_f("TF_15M_ATR_DISTANCE_MULT", 1.3),
        atr_spike_mult=_f("TF_15M_ATR_SPIKE_MULT", 2.2),
        discovery_mode="deterministic",
        poll_interval_seconds=_i("TF_15M_POLL_SECONDS", 5),
    ),
    TimeframeProfile(
        label="1h",
        interval_minutes=60,
        kline_interval=_s("TF_1H_KLINE_INTERVAL", "5m"),
        atr_period=_i("TF_1H_ATR_PERIOD", 14),
        ema_fast=_i("TF_1H_EMA_FAST", 9),
        ema_slow=_i("TF_1H_EMA_SLOW", 21),
        atr_lookback_for_regime=_i("TF_1H_ATR_LOOKBACK", 60),
        min_minutes_left=_f("TF_1H_MIN_MINUTES_LEFT", 10.0),
        max_minutes_left=_f("TF_1H_MAX_MINUTES_LEFT", 45.0),
        atr_distance_mult=_f("TF_1H_ATR_DISTANCE_MULT", 1.3),
        atr_spike_mult=_f("TF_1H_ATR_SPIKE_MULT", 2.2),
        discovery_mode="hourly_et_named",
        poll_interval_seconds=_i("TF_1H_POLL_SECONDS", 20),
    ),
]

# Какие таймфреймы реально торгуются — через запятую в .env. По умолчанию
# только 15m (часовую стратегию отключили по факту, но код для неё остаётся
# на месте — можно вернуть без единой правки, просто дописав "1h" сюда).
_enabled_labels = {s.strip() for s in os.getenv("ENABLED_TIMEFRAMES", "15m").split(",") if s.strip()}
TIMEFRAMES: list[TimeframeProfile] = [tf for tf in _ALL_TIMEFRAMES if tf.label in _enabled_labels]
