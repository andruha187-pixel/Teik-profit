"""
Точка входа. Запускает две асинхронные задачи параллельно:
1) торговый цикл (опрос рынка каждые POLL_INTERVAL_SECONDS)
2) телеграм-бот (слушает команды /status, /pause, /resume, /pnl)
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import binance_feed, market_discovery, indicators, strategy
from src import polymarket_client, storage, telegram_notify, executor, book_stream, runtime_state

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("polymarket-bot")


async def trading_loop():
    storage.init_db()
    runtime_state.init_from_db()
    dry_run = runtime_state.get("dry_run")
    await telegram_notify.notify(
        f"🤖 Бот запущен. Режим: {'DRY RUN (без реальных сделок)' if dry_run else 'LIVE — реальные сделки!'}\n"
        f"Открой /menu для управления (старт/стоп, размер позиции, стоп-лосс, настройки)."
    )

    while True:
        try:
            await _tick()
        except Exception as exc:  # noqa: BLE001 — цикл не должен падать целиком из-за одной ошибки
            log.exception("Ошибка в торговом цикле: %s", exc)
            await telegram_notify.notify(f"⚠️ Ошибка в цикле: {exc}")

        await asyncio.sleep(settings.POLL_INTERVAL_SECONDS)


async def _tick():
    market = await market_discovery.get_active_market()

    # Подписываемся на стакан этого рынка сразу при обнаружении — идемпотентно,
    # повторные вызовы на каждом тике ничего не стоят. К моменту входа (обычно
    # это 2-9 минуты из 15) книга уже несколько минут стримится живьём, а не
    # запрашивается REST-ом в момент решения — см. src/book_stream.py.
    if settings.USE_LIVE_BOOK_STREAM:
        book_stream.subscribe([market.up_token_id, market.down_token_id])

    # Периодический прогрев авторизованного HTTP-транспорта (не блокирует луп).
    if not runtime_state.get("dry_run"):
        asyncio.get_event_loop().run_in_executor(None, polymarket_client.prewarm_transport)

    klines = await binance_feed.get_klines(limit=max(100, settings.ATR_LOOKBACK_FOR_REGIME + settings.ATR_PERIOD + 5))
    ind = indicators.compute_indicator_snapshot(
        klines, settings.ATR_PERIOD, settings.EMA_FAST, settings.EMA_SLOW, settings.ATR_LOOKBACK_FOR_REGIME
    )
    current_price = ind["close"]

    minutes_left = max(0.0, (market.end_time - time.time()) / 60)

    get_book = polymarket_client.get_orderbook_cached if settings.USE_LIVE_BOOK_STREAM else polymarket_client.get_orderbook
    up_book = get_book(market.up_token_id)
    down_book = get_book(market.down_token_id)

    decision = strategy.evaluate(
        current_price=current_price,
        strike_price=market.strike_price,
        minutes_left=minutes_left,
        indicators=ind,
        up_book=up_book,
        down_book=down_book,
    )

    storage.log_signal(market.slug, current_price, market.strike_price, decision)

    telegram_notify.set_state_ref({
        "market_slug": market.slug,
        "direction": decision.direction,
        "current_price": round(current_price, 2),
        "strike_price": round(market.strike_price, 2),
        "minutes_left": round(minutes_left, 2),
        "safety_score": decision.safety_score,
    })

    active_book = up_book if decision.direction == "UP" else down_book
    log.info(
        "%s | price=%.2f strike=%.2f dir=%s left=%.1fm score=%.1f enter=%s book=%s reasons=%s",
        market.slug, current_price, market.strike_price, decision.direction,
        minutes_left, decision.safety_score, decision.should_enter, active_book.source, decision.reasons,
    )

    await executor.maybe_enter(market, decision)
    await executor.settle_resolved_trades()


async def main():
    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()

        book_stream_task = None
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream_task = asyncio.create_task(book_stream.run_forever())

        try:
            await trading_loop()
        finally:
            if book_stream_task:
                book_stream_task.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
