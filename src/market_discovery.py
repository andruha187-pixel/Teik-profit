"""
Поиск текущего активного Up/Down рынка на Polymarket — по активу и профилю
таймфрейма (см. src/timeframes.py).

Режимы обнаружения:
- "deterministic" (15m и короче): слаг вычисляется как
  {asset}-updown-{label}-<unix_ts_начала_окна>, окна выровнены по чистой
  UTC-сетке floor(now / interval) * interval. Быстро, без лишнего запроса.
- "hourly_et_named" (1h): у часовых рынков СОВСЕМ ДРУГОЙ формат слага —
  человекочитаемый, по Eastern Time, и с полным/особым именем актива, а не
  тикером: "bitcoin-up-or-down-september-13-2026-8pm-et" (не unix-таймстемп,
  как у 5m/15m/4h!). Обнаружено эмпирически через сайт Polymarket — окна
  выровнены по границам ET (сдвигаются между EST/EDT), поэтому вычисляем
  через zoneinfo, а не через простой floor() по UTC-эпохе.
- "series" — резервный путь (используется, если построенный по правилам
  выше слаг не найден): спрашиваем Gamma API по series_slug + closed=false.
"""
from __future__ import annotations
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx

from config import settings
from src import binance_feed
from src.timeframes import TimeframeProfile

_ET = ZoneInfo("America/New_York")

# У часовых рынков в слаге не всегда короткий тикер — часть активов Polymarket
# называет полным именем. Проверено по факту (fetch сайта): bitcoin, ethereum,
# solana — полным именем; xrp, bnb, hype — как тикер.
HOURLY_ASSET_NAMES = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "xrp",
    "bnb": "bnb",
    "hype": "hype",
}


@dataclass
class ActiveMarket:
    slug: str
    asset: str
    timeframe_label: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    start_time: int      # unix seconds
    end_time: int         # unix seconds
    strike_price: float   # цена базового актива на момент открытия окна


def _window_bounds(interval_minutes: int, now: float | None = None) -> tuple[int, int]:
    interval_sec = interval_minutes * 60
    now = now if now is not None else time.time()
    start = int(now // interval_sec) * interval_sec
    return start, start + interval_sec


def _expected_slug(asset: str, label: str, start_ts: int) -> str:
    return f"{asset}-updown-{label}-{start_ts}"


def _hourly_et_window(now: float | None = None) -> tuple[int, int]:
    """Часовые рынки выровнены по началу часа В Eastern Time, а не UTC."""
    now = now if now is not None else time.time()
    dt_et = datetime.fromtimestamp(now, tz=timezone.utc).astimezone(_ET)
    start_et = dt_et.replace(minute=0, second=0, microsecond=0)
    start_ts = int(start_et.astimezone(timezone.utc).timestamp())
    return start_ts, start_ts + 3600


def _hourly_slug(asset: str, start_ts: int) -> str:
    """'bitcoin-up-or-down-september-13-2026-8pm-et' — по времени НАЧАЛА
    часа в Eastern Time, человекочитаемо, без unix-таймстемпа."""
    name = HOURLY_ASSET_NAMES.get(asset.lower(), asset.lower())
    dt_et = datetime.fromtimestamp(start_ts, tz=timezone.utc).astimezone(_ET)
    month = dt_et.strftime("%B").lower()
    day = dt_et.day
    year = dt_et.year
    hour12 = dt_et.strftime("%I").lstrip("0") or "12"
    ampm = dt_et.strftime("%p").lower()
    return f"{name}-up-or-down-{month}-{day}-{year}-{hour12}{ampm}-et"


async def _fetch_event_by_slug(slug: str) -> dict | None:
    url = f"{settings.GAMMA_HOST}/events"
    params = {"slug": slug}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    if not data:
        return None
    return data[0] if isinstance(data, list) else data


async def _fetch_events_by_series(series_slug: str) -> list[dict]:
    url = f"{settings.GAMMA_HOST}/events"
    params = {"series_slug": series_slug, "closed": "false", "limit": 20,
              "order": "endDate", "ascending": "true"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    return data if isinstance(data, list) else []


async def _fetch_active_from_series(asset: str, label: str, interval_minutes: int) -> dict | None:
    """Резервный путь (если детерминированный/ET-слаг не сработал): ищем
    событие, окно которого содержит 'сейчас', пробуя оба варианта написания
    series_slug — источники по Polymarket расходятся."""
    candidates = [f"{asset}-up-or-down-{label}", f"{asset}-updown-{label}"]

    events: list[dict] = []
    for series_slug in candidates:
        try:
            events = await _fetch_events_by_series(series_slug)
        except httpx.HTTPStatusError:
            events = []
        if events:
            break

    if not events:
        return None

    now = time.time()
    for ev in events:
        start = _parse_timestamp(ev.get("marketStartTime"))
        if start is None:
            continue
        end = start + interval_minutes * 60
        if start <= now < end:
            return ev
    return events[0]


def _parse_timestamp(value) -> int | None:
    """API отдаёт время то unix-секундами, то ISO-строкой ('...Z') — а иногда
    в найденном поле вообще лежит дата создания записи, а не начала окна."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (ValueError, TypeError):
        return None


def _extract_market_fields(event: dict) -> tuple[str, list[str], int | None, int | None]:
    """Достаём condition_id, [up_token, down_token], start/end из объекта Event -> Market."""
    market = event["markets"][0] if "markets" in event and event["markets"] else event
    condition_id = market.get("conditionId") or market.get("condition_id")

    token_ids_raw = market.get("clobTokenIds")
    if isinstance(token_ids_raw, str):
        token_ids = json.loads(token_ids_raw)
    else:
        token_ids = token_ids_raw or []

    outcomes_raw = market.get("outcomes")
    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or ["Up", "Down"])

    up_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "up"), 0)
    down_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)
    up_token, down_token = token_ids[up_idx], token_ids[down_idx]

    start_time = _parse_timestamp(event.get("marketStartTime"))
    end_time = _parse_timestamp(event.get("endDate") or market.get("endDate"))

    return condition_id, [up_token, down_token], start_time, end_time


async def get_active_market(asset: str, timeframe: TimeframeProfile) -> ActiveMarket:
    asset = asset.lower()
    label = timeframe.label

    event = None
    matched_by_slug = False
    start_ts = end_ts = None

    if timeframe.discovery_mode == "deterministic":
        start_ts, end_ts = _window_bounds(timeframe.interval_minutes)
        slug = _expected_slug(asset, label, start_ts)
        event = await _fetch_event_by_slug(slug)
        matched_by_slug = event is not None

    elif timeframe.discovery_mode == "hourly_et_named":
        # Пробуем текущий час по ET, а на всякий случай (граничные эффекты
        # округления) — соседние часы, прежде чем падать в резервный путь.
        now = time.time()
        for offset_hours in (0, -1, 1):
            start_ts, end_ts = _hourly_et_window(now + offset_hours * 3600)
            slug = _hourly_slug(asset, start_ts)
            event = await _fetch_event_by_slug(slug)
            if event is not None and start_ts <= now < end_ts:
                matched_by_slug = True
                break
            event = None

    if event is None:
        event = await _fetch_active_from_series(asset, label, timeframe.interval_minutes)

    if event is None:
        raise RuntimeError(
            f"Не удалось найти активный рынок для {asset}/{label} ни по слагу, ни через серию Gamma API"
        )

    condition_id, (up_token, down_token), api_start, api_end = _extract_market_fields(event)

    if matched_by_slug:
        # Слаг уже кодирует точное начало окна (мы сами его вычислили и по
        # нему нашли ровно этот рынок) — надёжнее, чем поля из API.
        start_time, end_time = start_ts, end_ts
    else:
        start_time = api_start
        end_time = api_end or (start_time + timeframe.interval_minutes * 60 if start_time else None)
        if start_time is None:
            raise RuntimeError(f"Не удалось определить время начала окна для {asset}/{label}: {event.get('slug')}")

    symbol = binance_feed.symbol_for(asset)
    strike_price = await binance_feed.get_price_at(symbol, start_time)

    fallback_slug = _expected_slug(asset, label, start_time)
    return ActiveMarket(
        slug=event.get("slug", fallback_slug),
        asset=asset,
        timeframe_label=label,
        condition_id=condition_id,
        up_token_id=up_token,
        down_token_id=down_token,
        start_time=start_time,
        end_time=end_time,
        strike_price=strike_price,
    )


async def get_resolution(slug: str) -> str | None:
    """
    Возвращает "UP" / "DOWN", если рынок уже зарезолвился, иначе None.
    Читаем outcomePrices у market: финальная цена 1.0 у победившего исхода.
    Не зависит от актива/таймфрейма — работает по любому слагу.
    """
    event = await _fetch_event_by_slug(slug)
    if event is None:
        return None
    market = event["markets"][0] if event.get("markets") else event
    if not market.get("closed"):
        return None

    outcomes_raw = market.get("outcomes")
    outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else (outcomes_raw or ["Up", "Down"])
    prices_raw = market.get("outcomePrices")
    prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
    if not prices:
        return None

    winner_idx = max(range(len(prices)), key=lambda i: float(prices[i]))
    return outcomes[winner_idx].upper()
