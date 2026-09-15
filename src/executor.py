"""
Исполнение решений стратегии + учёт открытых позиций и их резолюции.
Управление капиталом: не больше MAX_OPEN_POSITIONS одновременно, стоп по
дневному лимиту убытков (стоп-лосс, настраивается кнопкой в Telegram) —
при достижении лимита новые входы блокируются до следующего дня.
"""
from __future__ import annotations
import time

from config import settings
from src import storage, polymarket_client, telegram_notify, book_stream, runtime_state
from src.market_discovery import ActiveMarket, get_resolution
from src.strategy import Decision


def _daily_loss_exceeded() -> bool:
    today_start = int(time.time() // 86400) * 86400
    summary = storage.get_pnl_summary(today_start)
    limit = runtime_state.get("daily_loss_limit_usdc")
    return summary["pnl_usdc"] <= -abs(limit)


def _scale_trade_size(base_size: float, score: float, threshold: float) -> float:
    """
    Размер ставки, зависящий от уверенности сигнала. На самом пороге
    (score == threshold) используем только MIN_FRACTION от базового размера,
    на score >= SIZE_SCALING_MAX_SCORE — полный размер. Между ними —
    линейная интерполяция. Идея: пограничный сигнал (score едва прошёл
    порог) — это по определению самый рискованный случай, не стоит на нём
    ставить так же, как на уверенном.
    """
    min_fraction = settings.SIZE_SCALING_MIN_FRACTION
    max_score = settings.SIZE_SCALING_MAX_SCORE
    span = max(max_score - threshold, 1e-6)
    progress = min(1.0, max(0.0, (score - threshold) / span))
    fraction = min_fraction + (1 - min_fraction) * progress
    return round(base_size * fraction, 2)


async def maybe_enter(market: ActiveMarket, decision: Decision) -> None:
    if not decision.should_enter:
        return
    if runtime_state.get("paused"):
        return
    if storage.get_open_trade_for_market(market.slug):
        return  # уже есть позиция в этом рынке
    if _daily_loss_exceeded():
        await telegram_notify.notify(
            f"🛑 Стоп-лосс достигнут (лимит {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC/день) — "
            f"вход заблокирован до конца дня. Поменять лимит можно в 🛑 Стоп-лосс в меню."
        )
        return
    if storage.count_open_trades() >= settings.MAX_OPEN_POSITIONS:
        # Общий потолок по ВСЕМ активам/таймфреймам разом — при нескольких
        # параллельных потоках несколько сигналов могут совпасть по времени.
        return

    base_size = runtime_state.get("trade_size_usdc")
    score_threshold = runtime_state.get("safety_score_threshold")
    if runtime_state.get("size_scaling_enabled"):
        trade_size = _scale_trade_size(base_size, decision.safety_score, score_threshold)
    else:
        trade_size = base_size
    dry_run = runtime_state.get("dry_run")

    token_id = market.up_token_id if decision.direction == "UP" else market.down_token_id

    # Проверяем реальную глубину стакана ПЕРЕД отправкой — FOK отменяет
    # ордер целиком, если не может закрыть всю сумму по цене не хуже
    # потолка. Если объём меньше запрошенного — торгуем тем, что реально
    # есть (с запасом на случай, что часть заберут раньше нас), а не
    # отправляем заведомо обречённый на отмену ордер на полную сумму.
    available_liquidity = book_stream.ask_liquidity_usdc(token_id, depth_levels=10)
    if available_liquidity < settings.MIN_VIABLE_TRADE_USDC:
        return  # почти пустой стакан — не о чем говорить, тихо пропускаем тик
    if available_liquidity < trade_size:
        trade_size = round(available_liquidity * 0.9, 2)

    # Между тем, как strategy.evaluate() прочитала ask, и моментом реальной
    # отправки ордера проходит какое-то время (сеть + подпись). Даём себе
    # небольшой запас на слиппедж, но не платим больше жёсткого потолка, и
    # обязательно выравниваем по тику — иначе CLOB отклонит ордер с неверным
    # шагом цены прямо в критичный момент.
    tick = book_stream.tick_size(token_id)
    raw_cap = min(decision.entry_price + settings.LIVE_ENTRY_MAX_SLIPPAGE, settings.MAX_ENTRY_EXECUTION_PRICE)
    execution_price = polymarket_client.round_price_for_buy(raw_cap, tick)

    status = "DRY_RUN"
    order_id = "dry-run"
    if not dry_run:
        if not settings.POLY_PRIVATE_KEY:
            # Не должно случиться благодаря проверкам в telegram_notify/main.py,
            # но лучше явная ошибка тут, чем AttributeError глубоко внутри SDK.
            await telegram_notify.notify(
                "❌ LIVE включён, но POLY_PRIVATE_KEY не задан — вход пропущен. "
                "Проверь переменные окружения и передеплой."
            )
            return
        try:
            # amount_usdc — это ДОЛЛАРОВАЯ сумма для BUY market-ордера, не
            # количество акций: конвертация не нужна, SDK делает это сам.
            resp = await polymarket_client.place_buy_order(token_id, execution_price, trade_size, tick)
        except Exception as exc:  # noqa: BLE001
            if "fully filled" in str(exc).lower() or "fok" in str(exc).lower():
                # Гонка: между нашей проверкой глубины и отправкой ордера кто-то
                # успел забрать ликвидность. Пробуем ещё раз меньшим объёмом —
                # один раз, не зацикливаемся.
                retry_size = round(trade_size * 0.5, 2)
                if retry_size < settings.MIN_VIABLE_TRADE_USDC:
                    await telegram_notify.notify(
                        f"⚠️ Сигнал по {market.slug} пропущен: не хватило ликвидности в стакане "
                        f"даже для уменьшенного объёма (FOK отменил ордер)."
                    )
                    return
                try:
                    resp = await polymarket_client.place_buy_order(token_id, execution_price, retry_size, tick)
                    trade_size = retry_size
                except Exception as exc2:  # noqa: BLE001
                    await telegram_notify.notify(
                        f"⚠️ Сигнал по {market.slug} пропущен: не хватило ликвидности даже после "
                        f"снижения размера до {retry_size:.2f} USDC ({exc2})."
                    )
                    return
            else:
                await telegram_notify.notify(f"❌ Ошибка при выставлении ордера: {exc}")
                return
        order_id = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID") or str(resp)
        status = polymarket_client.response_field(resp, "status") or "SUBMITTED"

    storage.log_trade(
        market_slug=market.slug,
        condition_id=market.condition_id,
        direction=decision.direction,
        entry_price=execution_price,
        size_usdc=trade_size,
        order_id=order_id,
        status=status,
        dry_run=dry_run,
        token_id=token_id,
    )

    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else '✅ '}Вход {decision.direction} по {market.slug}\n"
        f"Ask на сигнале: {decision.entry_price:.3f} | Потолок исполнения: {execution_price:.3f} "
        f"(тик {tick:g}) | Размер: {trade_size:.2f} из {base_size:.0f} USDC (score {decision.safety_score}/{score_threshold:.0f})\n"
        f"Расхождение: {decision.distance_atr} ATR | До конца рынка: {decision.minutes_left:.1f} мин"
    )


async def label_resolved_markets(exclude_slugs: set[str] | None = None) -> None:
    """
    Подписывает исходом ВСЕ ещё не подписанные сигналы прошлых рынков —
    основа для отчёта/анализа: без метки "что реально произошло" по
    каждому тику нельзя понять, какой сигнал был бы правильным, даже если
    бот в тот рынок не входил. exclude_slugs — слаги сейчас активных
    рынков по ВСЕМ активам/таймфреймам (их исход ещё не может быть
    известен, спрашивать API бессмысленно).
    """
    for slug in storage.get_markets_needing_outcome(exclude_slugs=exclude_slugs, limit=10):
        outcome = await get_resolution(slug)
        if outcome:
            storage.label_signals_outcome(slug, outcome)


async def settle_resolved_trades() -> None:
    for trade_id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id in storage.get_unsettled_trades():
        outcome = await get_resolution(market_slug)
        if outcome is None:
            continue

        shares = size_usdc / entry_price
        won = outcome == direction
        pnl = (shares * 1.0 - size_usdc) if won else -size_usdc
        storage.settle_trade(trade_id, outcome, pnl)

        emoji = "🟢" if won else "🔴"
        await telegram_notify.notify(
            f"{emoji} Рынок {market_slug} зарезолвился: {outcome}. "
            f"Наша ставка: {direction}. PnL: {pnl:+.2f} USDC"
            + (" (dry run)" if dry_run else "")
        )


async def check_position_stop_losses() -> None:
    """
    Стоп-лосс ОТДЕЛЬНОЙ позиции в процентах (не дневной!) — включается и
    настраивается кнопкой в Telegram. Пока рынок ещё не зарезолвился, но
    цена ушла против нас настолько, что текущая стоимость позиции упала на
    position_stop_loss_pct% и больше от суммы входа — закрываем продажей
    прямо сейчас, не дожидаясь исхода, чтобы не рисковать потерять 100%.

    Оценка stops по best bid из живого стакана (book_stream) — тот же
    источник, что и для входа, без лишнего REST-запроса, если стакан свежий.
    """
    if not runtime_state.get("position_stop_loss_enabled"):
        return
    stop_pct = runtime_state.get("position_stop_loss_pct")

    for trade_id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run, token_id in storage.get_unsettled_trades():
        if not token_id:
            continue  # старые сделки до появления этого поля — пропускаем, не падаем

        book = await polymarket_client.get_orderbook_cached(token_id)
        if book.best_bid is None:
            continue  # нет ставок на продажу прямо сейчас — не с чем сравнивать

        shares = size_usdc / entry_price
        current_value = shares * book.best_bid
        loss_pct = (current_value - size_usdc) / size_usdc * 100  # отрицательное число при убытке

        if loss_pct > -abs(stop_pct):
            continue  # просадка ещё не достигла порога

        pnl_estimate = current_value - size_usdc

        if dry_run:
            storage.settle_trade(trade_id, "STOPPED", pnl_estimate)
            await telegram_notify.notify(
                f"🧪 [DRY RUN] 📉 Стоп-лосс позиции сработал бы: {market_slug} ({direction})\n"
                f"Просадка {loss_pct:.1f}% (порог {stop_pct:.0f}%) | Оценка PnL: {pnl_estimate:+.2f} USDC"
            )
            continue

        if not settings.POLY_PRIVATE_KEY:
            await telegram_notify.notify(
                f"❌ Стоп-лосс сработал для {market_slug}, но POLY_PRIVATE_KEY не задан — "
                "продать не могу. Проверь переменные окружения."
            )
            continue

        try:
            resp = await polymarket_client.place_sell_order(token_id, shares, min_price=book.best_bid * 0.98)
        except Exception as exc:  # noqa: BLE001
            await telegram_notify.notify(f"❌ Не удалось закрыть позицию по стоп-лоссу ({market_slug}): {exc}")
            continue

        status = polymarket_client.response_field(resp, "status") or "SOLD"
        storage.settle_trade(trade_id, "STOPPED", pnl_estimate)
        await telegram_notify.notify(
            f"📉 Стоп-лосс сработал: {market_slug} ({direction}) продано по ~{book.best_bid:.3f}\n"
            f"Просадка {loss_pct:.1f}% (порог {stop_pct:.0f}%) | Оценка PnL: {pnl_estimate:+.2f} USDC | статус: {status}"
        )
