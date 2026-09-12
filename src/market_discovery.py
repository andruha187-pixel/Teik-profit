"""
Поиск текущего активного 15-минутного Up/Down рынка на Polymarket.

Слаг рынка детерминирован: btc-updown-15m-<unix_ts_начала_окна>, где
unix_ts — это floor(now / (interval*60)) * (interval*60) в UTC.
Дополнительно сверяемся с Gamma API (events?series_slug=...), чтобы
получить condition_id, токены исходов и точное время окончания —
детерминированный слаг может разойтись с реальным листингом на кромке
минуты, поэтому Gamma остаётся источником правды.
"""
from __future__ import annotations
import json
import time
from dataclasses import dataclass

import httpx

from config import settings
from src import binance_feed


@dataclass
class ActiveMarket:
    slug: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    start_time: int      # unix seconds
    end_time: int         # unix seconds
    strike_price: float   # цена BTC на момент открытия окна


def _window_bounds(now: float | None = None) -> tuple[int, int]:
    interval_sec = settings.MARKET_INTERVAL_MIN * 60
    now = now if now is not None else time.time()
    start = int(now // interval_sec) * interval_sec
    return start, start + interval_sec


def _expected_slug(start_ts: int) -> str:
    return f"{settings.ASSET}-updown-{settings.MARKET_INTERVAL_MIN}m-{start_ts}"


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


async def _fetch_active_from_series() -> dict | None:
    """Фолбэк: берём серию и находим событие, окно которого содержит 'сейчас'."""
    series_slug = f"{settings.ASSET}-updown-{settings.MARKET_INTERVAL_MIN}m"
    url = f"{settings.GAMMA_HOST}/events"
    params = {"series_slug": series_slug, "closed": "false", "limit": 20, "order": "endDate", "ascending": "true"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        events = resp.json()

    now = time.time()
    for ev in events:
        start = _parse_timestamp(ev.get("marketStartTime"))
        if start is None:
            continue
        end = start + settings.MARKET_INTERVAL_MIN * 60
        if start <= now < end:
            return ev
    return events[0] if events else None


def _parse_timestamp(value) -> int | None:
    """API отдаёт время то unix-секундами, то ISO-строкой ('...Z') — а иногда
    в найденном поле вообще лежит дата создания записи, а не начала окна.
    Разбираем оба формата, но с этим полем всё равно не доверяем слепо —
    см. комментарий в get_active_market."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        from datetime import datetime
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

    # Сопоставляем токены с исходами по индексу, а не полагаемся на порядок "как повезёт"
    up_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "up"), 0)
    down_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)
    up_token, down_token = token_ids[up_idx], token_ids[down_idx]

    start_time = _parse_timestamp(event.get("marketStartTime"))
    end_time = _parse_timestamp(event.get("endDate") or market.get("endDate"))

    return condition_id, [up_token, down_token], start_time, end_time


async def get_active_market() -> ActiveMarket:
    start_ts, end_ts = _window_bounds()
    slug = _expected_slug(start_ts)

    event = await _fetch_event_by_slug(slug)
    matched_by_slug = event is not None
    if event is None:
        event = await _fetch_active_from_series()
    if event is None:
        raise RuntimeError("Не удалось найти активный рынок ни по слагу, ни через серию Gamma API")

    condition_id, (up_token, down_token), api_start, api_end = _extract_market_fields(event)

    if matched_by_slug:
        # Слаг уже кодирует точное начало окна (мы сами его вычислили и по
        # нему нашли ровно этот рынок) — это надёжнее, чем поля из API,
        # которые на практике то в ISO, то в unix, а иногда там вообще дата
        # создания записи, а не начала конкретного 15-минутного окна.
        start_time, end_time = start_ts, end_ts
    else:
        start_time = api_start or start_ts
        end_time = api_end or (start_time + settings.MARKET_INTERVAL_MIN * 60)

    strike_price = await binance_feed.get_price_at(start_time)

    return ActiveMarket(
        slug=event.get("slug", slug),
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
