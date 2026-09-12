"""
Обёртка над py-clob-client-v2 (Polymarket/py-clob-client-v2).

ВАЖНО: Polymarket в 2026 мигрировал CLOB на V2 и архивировал старый пакет
py-clob-client — он теперь отдаёт "invalid order version, please use the
latest clob-client" на ЛЮБОЙ ордер. Актуальный пакет — py-clob-client-v2,
с другим API (import прямо из py_clob_client_v2, create_and_post_order
одним вызовом вместо create_order+post_order, Side.BUY вместо BUY).

Инициализация клиента ленивая — если PRIVATE_KEY не задан (например, ты
сначала хочешь погонять бота в DRY_RUN на паблик-данных), модуль всё
равно позволяет читать orderbook без авторизации (Level 0 API).

ВАЖНО #2: на момент миграции на V2 у Polymarket есть открытые баги в
официальном SDK, из-за которых ордера отклоняются с "maker address not
allowed, please use the deposit wallet flow" для части типов кошельков
(EOA, Magic.link-прокси, V1-мигрированные Safe) — это подтверждённые
issues в их репозитории, не что-то, что чинится на нашей стороне. Если
ты логинился в Polymarket через email/Magic-ссылку — есть риск упереться
именно в это. См. README, раздел про CLOB V2.
"""
from __future__ import annotations
import math
from dataclasses import dataclass

from config import settings
from src import book_stream

_client = None
_last_prewarm_ms = 0
PREWARM_INTERVAL_MS = 20_000


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    best_bid: float | None
    best_ask: float | None
    ask_liquidity_usdc: float  # сумма price*size по верхним уровням asks
    tick_size: float = 0.01
    source: str = "rest"       # "ws" (живой стакан) или "rest" (фолбэк)


def _get_client():
    global _client
    if _client is not None:
        return _client

    from py_clob_client_v2 import ClobClient

    if not settings.POLY_PRIVATE_KEY:
        # Read-only режим — только публичные эндпоинты (orderbook, markets)
        _client = ClobClient(settings.POLY_HOST, chain_id=settings.POLY_CHAIN_ID)
        return _client

    # L1 (подпись кошельком) — получаем/выводим API-ключ отдельным клиентом
    l1_client = ClobClient(settings.POLY_HOST, chain_id=settings.POLY_CHAIN_ID, key=settings.POLY_PRIVATE_KEY)
    creds = l1_client.create_or_derive_api_key()

    kwargs = dict(
        key=settings.POLY_PRIVATE_KEY,
        chain_id=settings.POLY_CHAIN_ID,
        signature_type=settings.POLY_SIGNATURE_TYPE,
        creds=creds,
    )
    if settings.POLY_FUNDER_ADDRESS:
        kwargs["funder"] = settings.POLY_FUNDER_ADDRESS

    # L1+L2 полностью авторизованный клиент — им и торгуем
    _client = ClobClient(settings.POLY_HOST, **kwargs)
    return _client


def _field(obj, key):
    """Достаём поле независимо от того, dict это или объект с атрибутами —
    py-clob-client-v2 в разных местах отдаёт то так, то так."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _to_level(lvl) -> tuple[float, float] | None:
    price = _field(lvl, "price")
    size = _field(lvl, "size")
    try:
        return float(price), float(size)
    except (TypeError, ValueError):
        return None


def get_orderbook(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """REST-фолбэк (Level 0 API, без авторизации). Используется, только если
    живой WS-стакан ещё не прогрелся или устарел — см. get_orderbook_cached."""
    client = _get_client()
    book = client.get_order_book(token_id)

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


def get_orderbook_cached(token_id: str, depth_levels: int = 5) -> OrderBookSnapshot:
    """
    Стакан "из прогрева": сначала смотрим в живой WS-кэш (book_stream) — там
    цена обновляется пушем с сервера без дополнительного сетевого раунд-трипа
    в момент принятия решения. Если кэш пуст или протух (WS ещё не успел
    прогреться / соединение недавно оборвалось) — падаем в REST, чтобы бот
    никогда не принимал решение на основе отсутствующих данных.
    """
    if book_stream.is_fresh(token_id):
        best_ask = book_stream.best_ask(token_id)
        best_bid = book_stream.best_bid(token_id)
        liquidity = book_stream.ask_liquidity_usdc(token_id, depth_levels)
        tick = book_stream.tick_size(token_id)
        if best_ask is not None:
            return OrderBookSnapshot(best_bid=best_bid, best_ask=best_ask,
                                      ask_liquidity_usdc=liquidity, tick_size=tick, source="ws")
    return get_orderbook(token_id, depth_levels)


def round_price_for_buy(price: float, tick_size: float) -> float:
    """
    Выравниваем цену BUY по шагу тика ВНИЗ (никогда не платим больше, чем
    планировали). Пример: reference=0.635, tick=0.01 -> сырой предел 0.645,
    но 0.645 не кратно тику -> округляем до 0.64. Без этого CLOB просто
    отклонит ордер с неправильным шагом цены, и мы потеряем время на ретрай
    именно в тот момент, когда счёт идёт на миллисекунды.
    """
    if tick_size <= 0:
        return round(price, 2)
    steps = math.floor(price / tick_size + 1e-9)
    return round(steps * tick_size, 6)


def prewarm_transport() -> bool:
    """
    Прогрев авторизованного HTTP-транспорта: безобидный read-only запрос
    баланса, который выполняет тот же путь (соединение, TLS, аутентификация
    L2), что и реальный ордер, но не имеет торгового эффекта. Не даёт
    заплатить cold-connection задержку именно в момент реального входа.
    Вызывать периодически (раз в 15-30с) фоновой задачей, только когда
    DRY_RUN=false и ключ настроен.
    """
    global _last_prewarm_ms
    import time as _time
    now = int(_time.time() * 1000)
    if now - _last_prewarm_ms < PREWARM_INTERVAL_MS:
        return False
    if not settings.POLY_PRIVATE_KEY:
        return False
    try:
        client = _get_client()
        client.get_balance_allowance(asset_type="COLLATERAL")
        _last_prewarm_ms = now
        return True
    except Exception:
        _last_prewarm_ms = now
        return False


def place_buy_order(token_id: str, price: float, size_shares: float, tick_size: float = 0.01) -> dict:
    """
    Лимитный BUY с исполнением FOK (Fill-Or-Kill) — либо забираем нужный
    объём по цене не хуже указанной прямо сейчас, либо ордер отменяется.
    Это осознанный выбор для входа в рынок с истекающим временем: не хотим
    зависший GTC-ордер, который исполнится в неподходящий момент.
    create_and_post_order — это API py-clob-client-v2: строит, подписывает
    и отправляет ордер одним вызовом (в v1 это были два отдельных метода).
    """
    from py_clob_client_v2 import OrderArgs, OrderType, Side, PartialCreateOrderOptions

    client = _get_client()
    order_args = OrderArgs(token_id=token_id, price=price, size=size_shares, side=Side.BUY)
    return client.create_and_post_order(
        order_args=order_args,
        options=PartialCreateOrderOptions(tick_size=str(tick_size)),
        order_type=OrderType.FOK,
    )


def get_market_resolution(condition_id: str) -> dict | None:
    """Не используется в текущем потоке (резолюцию берём через Gamma API в
    market_discovery.get_resolution), оставлено как утилита на будущее."""
    client = _get_client()
    try:
        market = client.get_market(condition_id)
    except Exception:
        return None
    return market
