"""
Живой стакан по WebSocket вместо REST-поллинга.

Раньше (v1) бот на каждом тике дёргал REST `GET /book` — это лишние
100-300ms сетевого раунд-трипа именно в момент принятия решения о входе,
и цена, на которую мы смотрим, могла на несколько сотен мс отставать от
реальной. Плюс Polymarket сам по себе обновляет свою книгу с задержкой
относительно бирж типа Binance — так что "прогретый" WS-стакан не убирает
этот системный лаг, но убирает НАШУ собственную задержку поверх него.

Идея "прогрева": подписываемся на токены рынка сразу, как только рынок
обнаружен (за много минут до возможного входа), и держим соединение
открытым. К моменту, когда strategy.evaluate() реально нужна цена аска,
она уже лежит в памяти, обновлённая пушем с сервера, а не тем, что мы
только что сходили и спросили.

Формат сообщений — по официальной документации Polymarket
(wss://ws-subscriptions-clob.polymarket.com/ws/market):
  {"type": "market", "assets_ids": [...], "custom_feature_enabled": true}
  -> event_type: "book" | "price_change" | "tick_size_change" | "last_trade_price"
"""
from __future__ import annotations
import asyncio
import json
import logging
import time

import websockets

from config import settings

log = logging.getLogger("book_stream")

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_BOOK_AGE_MS = 3000        # старше этого — считаем стакан протухшим, идём в REST
RECONNECT_BACKOFF_SEC = 2

_books: dict[str, dict] = {}
_subscribed: set[str] = set()
_send_queue: asyncio.Queue = asyncio.Queue()
_ws_ready = asyncio.Event()


def now_ms() -> int:
    return int(time.time() * 1000)


def _level_map(levels) -> dict[float, float]:
    out = {}
    for lvl in levels or []:
        try:
            price = float(lvl.get("price"))
            size = float(lvl.get("size"))
        except (TypeError, ValueError, AttributeError):
            continue
        if size > 0:
            out[price] = size
    return out


def _apply_snapshot(asset: str, msg: dict) -> None:
    tick = msg.get("tick_size") or msg.get("tickSize") or _books.get(asset, {}).get("tick_size") or 0.01
    _books[asset] = {
        "bids": _level_map(msg.get("bids")),
        "asks": _level_map(msg.get("asks")),
        "received_ms": now_ms(),
        "tick_size": float(tick),
    }


def _apply_delta(msg: dict) -> None:
    for ch in msg.get("price_changes", []):
        asset = str(ch.get("asset_id") or "")
        if not asset:
            continue
        book = _books.setdefault(asset, {"bids": {}, "asks": {}, "received_ms": now_ms(), "tick_size": 0.01})
        try:
            price = float(ch.get("price"))
            size = float(ch.get("size"))
        except (TypeError, ValueError):
            continue
        side = str(ch.get("side", "")).upper()
        target = book["bids"] if side == "BUY" else book["asks"]
        if size <= 0:
            target.pop(price, None)
        else:
            target[price] = size
        book["received_ms"] = now_ms()


def subscribe(asset_ids: list[str]) -> None:
    """Идемпотентно добавляем токены в подписку. Реально уходит в сокет,
    когда соединение поднято — если сокета ещё нет, он подхватит при коннекте."""
    new = [a for a in asset_ids if a and a not in _subscribed]
    if not new:
        return
    _subscribed.update(new)
    try:
        _send_queue.put_nowait({"type": "market", "assets_ids": list(_subscribed), "custom_feature_enabled": True})
    except asyncio.QueueFull:
        pass


def get_book(asset: str) -> dict | None:
    return _books.get(asset)


def best_ask(asset: str) -> float | None:
    b = _books.get(asset)
    if not b or not b.get("asks"):
        return None
    return min(b["asks"])


def best_bid(asset: str) -> float | None:
    b = _books.get(asset)
    if not b or not b.get("bids"):
        return None
    return max(b["bids"])


def ask_liquidity_usdc(asset: str, depth_levels: int = 5) -> float:
    b = _books.get(asset)
    if not b or not b.get("asks"):
        return 0.0
    top = sorted(b["asks"].items())[:depth_levels]
    return sum(price * size for price, size in top)


def tick_size(asset: str) -> float:
    b = _books.get(asset)
    return float(b["tick_size"]) if b and b.get("tick_size") else 0.01


def is_fresh(asset: str, max_age_ms: int = MAX_BOOK_AGE_MS) -> bool:
    b = _books.get(asset)
    if not b or not b.get("received_ms"):
        return False
    return (now_ms() - b["received_ms"]) <= max_age_ms


async def _sender(ws) -> None:
    while True:
        payload = await _send_queue.get()
        await ws.send(json.dumps(payload))


async def run_forever() -> None:
    """Фоновая задача: держит WS-соединение живым, переподключается при обрыве.
    Запускать один раз при старте бота (main.py). Keepalive — protocol-level
    WS ping/pong (ping_interval/ping_timeout), а не наши собственные текстовые
    сообщения: сервер Polymarket разбирает КАЖДОЕ входящее сообщение в этом
    канале как JSON-запрос на подписку, и любая нестандартная строка (в т.ч.
    наш прежний текстовый "PING") валится с 1008 policy violation."""
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                log.info("Book stream connected | subscribed=%d", len(_subscribed))
                _ws_ready.set()
                if _subscribed:
                    await ws.send(json.dumps({
                        "type": "market", "assets_ids": list(_subscribed), "custom_feature_enabled": True,
                    }))
                sender_task = asyncio.create_task(_sender(ws))
                try:
                    async for raw in ws:
                        if raw == "PONG":
                            continue
                        msg = json.loads(raw)
                        event_type = msg.get("event_type")
                        if event_type == "book":
                            asset = str(msg.get("asset_id") or "")
                            if asset:
                                _apply_snapshot(asset, msg)
                        elif event_type == "price_change":
                            _apply_delta(msg)
                        elif event_type == "tick_size_change":
                            asset = str(msg.get("asset_id") or "")
                            new_tick = msg.get("new_tick_size")
                            if asset in _books and new_tick:
                                _books[asset]["tick_size"] = float(new_tick)
                finally:
                    sender_task.cancel()
        except Exception as exc:  # noqa: BLE001
            log.warning("Book stream disconnected (%s), reconnecting in %ss", exc, RECONNECT_BACKOFF_SEC)
            _ws_ready.clear()
            await asyncio.sleep(RECONNECT_BACKOFF_SEC)


async def wait_ready(timeout: float = 5.0) -> bool:
    try:
        await asyncio.wait_for(_ws_ready.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False
