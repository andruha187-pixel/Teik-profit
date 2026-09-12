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

    base_size = runtime_state.get("trade_size_usdc")
    score_threshold = runtime_state.get("safety_score_threshold")
    trade_size = _scale_trade_size(base_size, decision.safety_score, score_threshold)
    dry_run = runtime_state.get("dry_run")

    token_id = market.up_token_id if decision.direction == "UP" else market.down_token_id
    size_shares = round(trade_size / decision.entry_price, 2)

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
        try:
            resp = polymarket_client.place_buy_order(token_id, execution_price, size_shares)
            order_id = resp.get("orderID") or resp.get("order_id") or str(resp)
            status = resp.get("status", "SUBMITTED")
        except Exception as exc:  # noqa: BLE001 — любая ошибка биржи не должна ронять бота
            await telegram_notify.notify(f"❌ Ошибка при выставлении ордера: {exc}")
            return

    storage.log_trade(
        market_slug=market.slug,
        condition_id=market.condition_id,
        direction=decision.direction,
        entry_price=execution_price,
        size_usdc=trade_size,
        order_id=order_id,
        status=status,
        dry_run=dry_run,
    )

    await telegram_notify.notify(
        f"{'🧪 [DRY RUN] ' if dry_run else '✅ '}Вход {decision.direction} по {market.slug}\n"
        f"Ask на сигнале: {decision.entry_price:.3f} | Потолок исполнения: {execution_price:.3f} "
        f"(тик {tick:g}) | Размер: {trade_size:.2f} из {base_size:.0f} USDC (score {decision.safety_score}/{score_threshold:.0f})\n"
        f"Расхождение: {decision.distance_atr} ATR | До конца рынка: {decision.minutes_left:.1f} мин"
    )


async def settle_resolved_trades() -> None:
    for trade_id, market_slug, condition_id, direction, entry_price, size_usdc, dry_run in storage.get_unsettled_trades():
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
