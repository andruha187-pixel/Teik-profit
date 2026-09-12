"""
Обёртка над polymarket-client — официальным "унифицированным" Python SDK
Polymarket (import как `polymarket`).

История пакетов для этого проекта (все — реальные ошибки, с которыми
столкнулись на практике, не гипотетические):
  py-clob-client (v1)     -> архивирован, шлёт заведомо отклоняемые ордера
  py-clob-client-v2       -> сам Polymarket в официальном migration guide
                              велит с него уходить; плюс открытые баги с
                              "maker address not allowed" для части кошельков
  polymarket-client       -> актуальный официальный SDK, САМ определяет тип
                              кошелька (EOA/прокси/deposit wallet) вместо
                              того, чтобы просить вручную угадывать
                              signature_type — это как раз обходит класс
                              багов из py-clob-client-v2. Асинхронный.

Инициализация клиента ленивая — если PRIVATE_KEY не задан (например, ты
сначала хочешь погонять бота в DRY_RUN на паблик-данных), модуль всё
равно позволяет читать orderbook без авторизации через AsyncPublicClient.

ВАЖНО: SDK находится в статусе beta (это подтверждено самой документацией
Polymarket) — если после обновления вылезет что-то новое в духе смены
формата ответа, это ожидаемо для этой стадии проекта, не признак ошибки
в самом боте. Перед LIVE обязательно прогони scripts/test_live_order.py.
"""
from __future__ import annotations
import math
import time
from dataclasses import dataclass

from config import settings
from src import book_stream

_client = None
_last_prewarm_ms = 0
PREWARM_INTERVAL_MS = 20_000


@dataclass
class OrderBookSnapshot:
    best_bid: float | None
    best_ask: float | None
    ask_liquidity_usdc: float  # сумма price*size по верхним уровням asks
    tick_size: float = 0.01
    source: str = "rest"       # "ws" (живой стакан) или "rest" (фолбэк)


async def _get_client():
    global _client
    if _client is not None:
        return _client

    if not settings.POLY_PRIVATE_KEY:
        # Read-only режим — публичные эндпоинты без авторизации
        from polymarket import AsyncPublicClient
        _client = AsyncPublicClient()
        return _client

    from polymarket import AsyncSecureClient

    kwargs = dict(private_key=settings.POLY_PRIVATE_KEY)
    if settings.POLY_FUNDER_ADDRESS:
        # В этом SDK параметр называется `wallet`, не `funder` (как в
        # py-clob-client-v2) — это адрес аккаунта, которым торгуешь, если
        # он отличается от адреса, выведенного из приватного ключа.
        kwargs["wallet"] = settings.POLY_FUNDER_ADDRESS

    # Сознательно НЕ передаём signature_type — SDK определяет тип кошелька
    # сам (виден в client.wallet_type после создания).
    _client = await AsyncSecureClient.create(**kwargs)
    return _client


def _field(obj, key):
    """Достаём поле независимо от того, dict это или объект с атрибутами —
    разные версии/эндпоинты SDK отдают то так, то так."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


# Публичный алиас — executor.py читает поля ответа ордера тем же способом,
# не дублируя логику dict-vs-объект у себя.
response_field = _field


def _to_level(lvl) -> tuple[float, float] | None:
    price = _field(lvl, "price")
    size = _field(lvl, "size")
    try:
        return float(price), float(size)
    except (TypeError, ValueError):
        return None


async def get_orderbook(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """REST-фолбэк (публичный эндпоинт). Используется, только если живой
    WS-стакан ещё не прогрелся или устарел — см. get_orderbook_cached."""
    client = await _get_client()
    book = await client.get_order_book(token_id=token_id)

    raw_asks = _field(book, "asks") or []
    raw_bids = _field(book, "bids") or []

    asks = sorted((lv for lv in (_to_level(l) for l in raw_asks) if lv), key=lambda x: x[0])
    bids = sorted((lv for lv in (_to_level(l) for l in raw_bids) if lv), key=lambda x: x[0], reverse=True)

    best_ask = asks[0][0] if asks else None
    best_bid = bids[0][0] if bids else None

    ask_liquidity = sum(price * size for price, size in asks[:depth_levels])
    tick_raw = _field(book, "tick_size") or _field(book, "tickSize")
    tick = float(tick_raw) if tick_raw else 0.01

    return OrderBookSnapshot(best_bid=best_bid, best_ask=best_ask, ask_liquidity_usdc=ask_liquidity,
                              tick_size=tick, source="rest")


async def get_orderbook_cached(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """
    Стакан "из прогрева": сначала смотрим в живой WS-кэш (book_stream) — там
    цена обновляется пушем с сервера без дополнительного сетевого раунд-трипа
    в момент принятия решения. Если кэш пуст или протух — падаем в REST.
    """
    if book_stream.is_fresh(token_id):
        best_ask = book_stream.best_ask(token_id)
        best_bid = book_stream.best_bid(token_id)
        liquidity = book_stream.ask_liquidity_usdc(token_id, depth_levels)
        tick = book_stream.tick_size(token_id)
        if best_ask is not None:
            return OrderBookSnapshot(best_bid=best_bid, best_ask=best_ask,
                                      ask_liquidity_usdc=liquidity, tick_size=tick, source="ws")
    return await get_orderbook(token_id, depth_levels)


def round_price_for_buy(price: float, tick_size: float) -> float:
    """
    Выравниваем цену BUY по шагу тика ВНИЗ (никогда не платим больше, чем
    планировали). Используется как верхний предел (worst-price) для
    market-ордера — защита от слиппеджа.
    """
    if tick_size <= 0:
        return round(price, 2)
    steps = math.floor(price / tick_size + 1e-9)
    return round(steps * tick_size, 6)


async def prewarm_transport() -> bool:
    """
    Прогрев авторизованного HTTP-транспорта: безобидный read-only запрос
    баланса, который выполняет тот же путь (соединение, TLS, аутентификация),
    что и реальный ордер, но не имеет торгового эффекта. Вызывается фоновой
    задачей (fire-and-forget), только когда DRY_RUN=false и ключ настроен.
    """
    global _last_prewarm_ms
    now = int(time.time() * 1000)
    if now - _last_prewarm_ms < PREWARM_INTERVAL_MS:
        return False
    if not settings.POLY_PRIVATE_KEY:
        return False
    try:
        client = await _get_client()
        await client.get_balance_allowance(asset_type="COLLATERAL")
        _last_prewarm_ms = now
        return True
    except Exception:
        _last_prewarm_ms = now
        return False


async def place_buy_order(token_id: str, price_cap: float, amount_usdc: float, tick_size: float = 0.01) -> dict:
    """
    Market-ордер BUY с исполнением FOK (Fill-Or-Kill) — либо полностью
    исполняется прямо сейчас, либо целиком отменяется. amount_usdc — это
    ДОЛЛАРОВАЯ сумма к трате (для BUY market-ордеров в этом SDK amount — это
    USD-номинал, а не количество акций — конвертация не нужна). max_price
    задаёт худшую допустимую цену исполнения (защита от слиппеджа) —
    именно так называется параметр в этом SDK, не "price".
    """
    client = await _get_client()
    return await client.place_market_order(
        token_id=token_id,
        side="BUY",
        amount=str(round(amount_usdc, 2)),
        max_price=str(price_cap),
        order_type="FOK",
    )
