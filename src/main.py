"""
Точка входа. Архитектура: один независимый асинхронный поток на каждую
пару (актив, таймфрейм) — например, "btc/15m", "sol/1h" и т.д. Все потоки
делят общий book_stream (WS-стакан), общий Telegram-бот и общую БД.

Плюс две общие фоновые задачи, которые не привязаны к конкретному потоку:
- settlement_loop: резолюция сделок и разметка исходов сигналов по ВСЕМ
  активам/таймфреймам разом (если делать это в каждом из 12 потоков
  отдельно — 12-кратное дублирование запросов к Gamma API впустую).
- reporting.report_loop: периодический CSV-отчёт в Telegram (общий по
  всем активам — разбивка по колонкам asset/timeframe уже в самих данных).
"""
from __future__ import annotations
import asyncio
import logging
import time

from config import settings
from src import binance_feed, market_discovery, indicators, strategy
from src import polymarket_client, storage, telegram_notify, executor, book_stream, runtime_state, reporting
from src.timeframes import TIMEFRAMES, TimeframeProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("polymarket-bot")

# Слаг сейчас активного рынка по каждому потоку ("btc:15m" -> "btc-updown-15m-...")
# — общий словарь, читает settlement_loop, чтобы не спрашивать Gamma API про
# рынки, которые заведомо ещё не могли зарезолвиться.
_active_slugs: dict[str, str] = {}

# Последнее состояние каждого потока — для команды /token и общего статуса.
_instance_state: dict[str, dict] = {}


async def _instance_tick(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"
    market = await market_discovery.get_active_market(asset, timeframe)
    _active_slugs[key] = market.slug

    if settings.USE_LIVE_BOOK_STREAM:
        book_stream.subscribe([market.up_token_id, market.down_token_id])

    if not runtime_state.get("dry_run"):
        asyncio.create_task(polymarket_client.prewarm_transport())

    symbol = binance_feed.symbol_for(asset)
    klines = await binance_feed.get_klines(
        symbol,
        limit=max(100, timeframe.atr_lookback_for_regime + timeframe.atr_period + 5),
        interval=timeframe.kline_interval,
    )
    ind = indicators.compute_indicator_snapshot(
        klines, timeframe.atr_period, timeframe.ema_fast, timeframe.ema_slow, timeframe.atr_lookback_for_regime,
    )
    current_price = ind["close"]

    minutes_left = max(0.0, (market.end_time - time.time()) / 60)

    get_book = polymarket_client.get_orderbook_cached if settings.USE_LIVE_BOOK_STREAM else polymarket_client.get_orderbook
    up_book = await get_book(market.up_token_id)
    down_book = await get_book(market.down_token_id)

    decision = strategy.evaluate(
        current_price=current_price,
        strike_price=market.strike_price,
        minutes_left=minutes_left,
        indicators=ind,
        up_book=up_book,
        down_book=down_book,
        min_minutes_left=timeframe.min_minutes_left,
        max_minutes_left=timeframe.max_minutes_left,
        atr_distance_mult=timeframe.atr_distance_mult,
        atr_spike_mult=timeframe.atr_spike_mult,
    )

    storage.log_signal(market.slug, current_price, market.strike_price, decision,
                        indicators=ind, up_book=up_book, down_book=down_book)

    _instance_state[key] = {
        "market_slug": market.slug,
        "asset": asset,
        "timeframe": timeframe.label,
        "direction": decision.direction,
        "current_price": round(current_price, 2),
        "strike_price": round(market.strike_price, 2),
        "minutes_left": round(minutes_left, 2),
        "safety_score": decision.safety_score,
        "up_token_id": market.up_token_id,
        "down_token_id": market.down_token_id,
    }
    telegram_notify.set_state_ref(_instance_state)

    active_book = up_book if decision.direction == "UP" else down_book
    log.info(
        "%s | price=%.4f strike=%.4f dir=%s left=%.1fm score=%.1f enter=%s book=%s reasons=%s",
        market.slug, current_price, market.strike_price, decision.direction,
        minutes_left, decision.safety_score, decision.should_enter, active_book.source, decision.reasons,
    )

    await executor.maybe_enter(market, decision)


async def _instance_loop(asset: str, timeframe: TimeframeProfile) -> None:
    key = f"{asset}:{timeframe.label}"
    consecutive_failures = 0
    notified_dead = False
    while True:
        try:
            await _instance_tick(asset, timeframe)
            consecutive_failures = 0
            notified_dead = False
        except Exception as exc:  # noqa: BLE001 — один сломанный поток не должен ронять остальные
            consecutive_failures += 1
            log.exception("Ошибка в потоке %s (%d подряд): %s", key, consecutive_failures, exc)
            if consecutive_failures == 10 and not notified_dead:
                # После 10 неудач подряд это, скорее всего, не временный сбой сети, а
                # актив/таймфрейм, для которого рынка просто не существует (например,
                # у XRP на момент написания нет часового Up/Down рынка) — не спамим
                # логи и Telegram вечно, а один раз сообщаем и переходим на редкий опрос.
                notified_dead = True
                await telegram_notify.notify(
                    f"⚠️ Поток {key} не может найти рынок уже {consecutive_failures} попыток подряд "
                    f"({exc}). Похоже, этого рынка не существует для данного актива/таймфрейма. "
                    f"Перехожу на редкий опрос (раз в 10 минут), остальные потоки не затронуты."
                )

        sleep_for = timeframe.poll_interval_seconds if consecutive_failures < 10 else 600
        await asyncio.sleep(sleep_for)


async def settlement_loop() -> None:
    """Общая (не привязанная к конкретному активу) фоновая задача: резолюция
    сделок, стоп-лосс открытых позиций и разметка исходов сигналов по всем
    потокам разом."""
    while True:
        try:
            await executor.settle_resolved_trades()
            await executor.check_position_stop_losses()
            await executor.label_resolved_markets(exclude_slugs=set(_active_slugs.values()))
        except Exception as exc:  # noqa: BLE001
            log.exception("Ошибка в settlement_loop: %s", exc)
        await asyncio.sleep(10)


async def main():
    # Инициализация БД и настроек — до старта любых фоновых задач.
    storage.init_db()
    runtime_state.init_from_db()

    # Защита от "тихого" LIVE без ключа: если в БД с прошлого раза сохранён
    # LIVE-режим, а сейчас POLY_PRIVATE_KEY не задан (новый хостинг, забыли
    # перенести переменную и т.п.) — принудительно откатываемся в DRY RUN,
    # а не пытаемся торговать клиентом без прав на ордера.
    forced_back_to_dry_run = False
    if not runtime_state.get("dry_run") and not settings.POLY_PRIVATE_KEY:
        runtime_state.set("dry_run", True)
        forced_back_to_dry_run = True

    app = telegram_notify.build_app()
    async with app:
        await app.start()
        await app.updater.start_polling()

        await telegram_notify.clear_legacy_keyboard()
        dry_run = runtime_state.get("dry_run")
        assets_line = ", ".join(a.upper() for a in settings.ASSETS)
        timeframes_line = ", ".join(tf.label for tf in TIMEFRAMES)
        forced_note = (
            "\n⚠️ Был сохранён LIVE-режим с прошлого раза, но POLY_PRIVATE_KEY сейчас не задан — "
            "принудительно откатил в DRY RUN, чтобы не пытаться торговать без ключа."
            if forced_back_to_dry_run else ""
        )
        await telegram_notify.notify(
            f"🤖 Бот запущен. Режим: {'DRY RUN (без реальных сделок)' if dry_run else 'LIVE — реальные сделки!'}\n"
            f"Активы: {assets_line}\nТаймфреймы: {timeframes_line}\n"
            f"Открой /menu для управления (старт/стоп, размер позиции, стоп-лосс, настройки)."
            f"{forced_note}"
        )

        book_stream_task = None
        if settings.USE_LIVE_BOOK_STREAM:
            book_stream_task = asyncio.create_task(book_stream.run_forever())

        report_task = asyncio.create_task(reporting.report_loop())
        settlement_task = asyncio.create_task(settlement_loop())

        instance_tasks = [
            asyncio.create_task(_instance_loop(asset, timeframe))
            for asset in settings.ASSETS
            for timeframe in TIMEFRAMES
        ]

        try:
            await asyncio.gather(*instance_tasks)
        finally:
            if book_stream_task:
                book_stream_task.cancel()
            report_task.cancel()
            settlement_task.cancel()
            await app.updater.stop()
            await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
