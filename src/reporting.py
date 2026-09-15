"""
Периодический отчёт для анализа стратегии — раз в REPORT_INTERVAL_HOURS
формирует CSV-файлы из накопленных данных и шлёт их в Telegram файлом.

Два файла:
  signals_*.csv — КАЖДЫЙ тик за период, вошёл бот или нет, со всеми сырыми
                  индикаторами, компонентами score и (когда рынок уже
                  зарезолвился) фактическим исходом. Это основной датасет
                  для поиска реального edge — без меток исхода на
                  НЕ-торгованных сигналах анализ был бы смещён только на
                  те случаи, где бот и так решил войти.
  trades_*.csv  — только реально исполненные (или dry-run) сделки с PnL.

Момент последнего отчёта хранится в bot_settings (переживает рестарт) —
чтобы при перезапуске не задваивать период и не терять данные между ним.
"""
from __future__ import annotations
import asyncio
import csv
import os
import time

from config import settings
from src import storage, telegram_notify

_LAST_REPORT_KEY = "last_report_ts"


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)


def _get_last_report_ts() -> int:
    saved = storage.get_all_settings().get(_LAST_REPORT_KEY)
    try:
        return int(saved)
    except (TypeError, ValueError):
        # Первый запуск — берём период отчёта назад от текущего момента,
        # а не всю историю с нуля.
        return int(time.time() - settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


async def build_and_send_report() -> None:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())

    signals = storage.get_signals_since(since_ts)
    trades = storage.get_trades_since(since_ts)

    if not signals and not trades:
        _set_last_report_ts(now_ts)
        return

    from_label = time.strftime("%Y%m%d-%H%M", time.gmtime(since_ts))
    to_label = time.strftime("%Y%m%d-%H%M", time.gmtime(now_ts))
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")

    signals_path = f"{base}_signals.csv"
    trades_path = f"{base}_trades.csv"

    _write_csv(signals_path, storage.SIGNALS_COLUMNS, signals)
    _write_csv(trades_path, storage.TRADES_COLUMNS, trades)

    entered = sum(1 for row in signals if row[storage.SIGNALS_COLUMNS.index("should_enter")])
    labeled = sum(1 for row in signals if row[storage.SIGNALS_COLUMNS.index("outcome")])
    closed_trades = [row for row in trades if row[storage.TRADES_COLUMNS.index("outcome")]]
    wins = sum(1 for row in closed_trades if row[storage.TRADES_COLUMNS.index("pnl_usdc")] and
               row[storage.TRADES_COLUMNS.index("pnl_usdc")] > 0)
    pnl_sum = sum(row[storage.TRADES_COLUMNS.index("pnl_usdc")] or 0 for row in closed_trades)

    by_asset = storage.get_pnl_by_asset(since_ts)
    asset_lines = "\n".join(
        f"  {a.upper()}: {b['trades']} сделок, PnL {b['pnl_usdc']:+.2f}"
        for a, b in sorted(by_asset.items())
    ) or "  (сделок за период не было)"

    caption = (
        f"📄 Отчёт {from_label} → {to_label} (UTC)\n"
        f"Тиков сигналов: {len(signals)} (с известным исходом: {labeled}) | вошли: {entered}\n"
        f"Сделок закрыто: {len(closed_trades)} | побед: {wins} | PnL: {pnl_sum:+.2f} USDC\n\n"
        f"По токенам за период:\n{asset_lines}"
    )

    await telegram_notify.send_document(signals_path, caption)
    await telegram_notify.send_document(trades_path, None)

    _set_last_report_ts(now_ts)


async def report_loop() -> None:
    """Фоновая задача: спит между отчётами, переживает произвольные
    интервалы рестарта за счёт хранения last_report_ts в БД."""
    while True:
        try:
            await build_and_send_report()
        except Exception:
            pass  # не роняем бота из-за проблем с отчётом; попробуем в следующий раз
        await asyncio.sleep(max(60, settings.REPORT_INTERVAL_HOURS * 3600))
