"""
Телеграм-бот: кнопочное меню управления + пуш-уведомления о сделках.

Меню:
  ▶️/⏸ Старт-стоп | 💰 Размер позиции | 🛑 Стоп-лосс | 📊 Статистика
  ⚙️ Настройки (safety score) | 🧪/🔴 режим DRY RUN / LIVE (с подтверждением)

Все изменения пишутся в runtime_state (который сам сохраняет их в SQLite),
так что настройки переживают рестарт процесса.
"""
from __future__ import annotations
import time

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import settings
from src import storage, runtime_state

_app: Application | None = None
_state_ref: dict = {}  # заполняется из main.py: последний сигнал/статус для меню

SIZE_PRESETS = [5, 10, 20, 50, 100]
STOPLOSS_PRESETS = [20, 50, 100, 200]
SCORE_PRESETS = [65, 75, 85, 90]


def set_state_ref(state: dict) -> None:
    global _state_ref
    _state_ref = state


# ---------------------------------------------------------------- меню ----

def _main_menu_text() -> str:
    s = _state_ref
    paused = runtime_state.get("paused")
    dry_run = runtime_state.get("dry_run")
    lines = [
        "🤖 *Polymarket BTC 15m Bot*",
        "",
        f"Статус: {'⏸ на паузе' if paused else '▶️ активен'} | Режим: {'🧪 DRY RUN' if dry_run else '🔴 LIVE'}",
        f"Размер позиции: {runtime_state.get('trade_size_usdc'):.0f} USDC",
        f"Стоп-лосс: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC/день",
        f"Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}",
    ]
    if s:
        lines += [
            "",
            f"Текущий рынок: {s.get('market_slug', '—')}",
            f"Направление: {s.get('direction', '—')} | score сейчас: {s.get('safety_score', '—')}",
        ]
    return "\n".join(lines)


def _main_menu_markup() -> InlineKeyboardMarkup:
    paused = runtime_state.get("paused")
    rows = [
        [InlineKeyboardButton("▶️ Старт" if paused else "⏸ Стоп", callback_data="pause_toggle")],
        [
            InlineKeyboardButton("💰 Размер позиции", callback_data="menu:size"),
            InlineKeyboardButton("🛑 Стоп-лосс", callback_data="menu:sl"),
        ],
        [
            InlineKeyboardButton("📊 Статистика", callback_data="stats"),
            InlineKeyboardButton("⚙️ Настройки", callback_data="menu:settings"),
        ],
        [InlineKeyboardButton(
            "🔴 Включить LIVE" if runtime_state.get("dry_run") else "🧪 Переключить в DRY RUN",
            callback_data="mode_toggle",
        )],
    ]
    return InlineKeyboardMarkup(rows)


def _size_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("trade_size_usdc")
    row = []
    rows = []
    for val in SIZE_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"size_set:{val}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _stoploss_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("daily_loss_limit_usdc")
    row = []
    rows = []
    for val in STOPLOSS_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"sl_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−10", callback_data="sl_delta:-10"),
        InlineKeyboardButton("+10", callback_data="sl_delta:+10"),
    ])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _settings_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("safety_score_threshold")
    row = []
    rows = []
    for val in SCORE_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"score_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−5", callback_data="score_delta:-5"),
        InlineKeyboardButton("+5", callback_data="score_delta:+5"),
    ])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _confirm_live_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Да, включить LIVE", callback_data="mode_confirm_live")],
        [InlineKeyboardButton("❌ Отмена", callback_data="menu:main")],
    ])


# ------------------------------------------------------------- команды ----

async def _cmd_start_or_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(_stats_text())


def _stats_text() -> str:
    today_start = int(time.time() // 86400) * 86400
    today = storage.get_pnl_summary(today_start)
    total = storage.get_pnl_summary(0)
    return (
        f"📊 Статистика\n\n"
        f"Сегодня: {today['trades']} сделок, PnL {today['pnl_usdc']:+.2f} USDC, побед {today['wins']}\n"
        f"Всего: {total['trades']} сделок, PnL {total['pnl_usdc']:+.2f} USDC, побед {total['wins']}"
    )


# ------------------------------------------------------------- кнопки -----

async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()

    if data == "menu:main":
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data == "menu:size":
        await query.edit_message_text("💰 Выбери размер позиции (USDC на сделку):", reply_markup=_size_menu_markup())

    elif data == "menu:sl":
        await query.edit_message_text(
            f"🛑 Дневной стоп-лосс: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC.\n"
            "При достижении убытка на эту сумму за день бот перестаёт открывать новые позиции до полуночи.",
            reply_markup=_stoploss_menu_markup(),
        )

    elif data == "menu:settings":
        await query.edit_message_text(
            f"⚙️ Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}\n"
            "Чем выше — тем реже и осторожнее входы.\n\n"
            "Размер ставки уже масштабируется от порога: на пограничном score — "
            f"{settings.SIZE_SCALING_MIN_FRACTION*100:.0f}% от размера позиции, "
            f"на score {settings.SIZE_SCALING_MAX_SCORE:.0f}+ — полный размер.",
            reply_markup=_settings_menu_markup(),
        )

    elif data == "stats":
        await query.edit_message_text(_stats_text(), reply_markup=_main_menu_markup())

    elif data == "pause_toggle":
        runtime_state.set("paused", not runtime_state.get("paused"))
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data.startswith("size_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("trade_size_usdc", val)
        await query.edit_message_text(f"✅ Размер позиции: {val:.0f} USDC", reply_markup=_size_menu_markup())

    elif data.startswith("sl_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("daily_loss_limit_usdc", val)
        await query.edit_message_text(f"✅ Стоп-лосс: {val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data.startswith("sl_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = max(5.0, runtime_state.get("daily_loss_limit_usdc") + delta)
        runtime_state.set("daily_loss_limit_usdc", new_val)
        await query.edit_message_text(f"✅ Стоп-лосс: {new_val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data.startswith("score_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(100.0, max(0.0, runtime_state.get("safety_score_threshold") + delta))
        runtime_state.set("safety_score_threshold", new_val)
        await query.edit_message_text(
            f"⚙️ Safety score порог: {new_val:.0f}", reply_markup=_settings_menu_markup(),
        )

    elif data.startswith("score_set:"):
        new_val = float(data.split(":", 1)[1])
        runtime_state.set("safety_score_threshold", new_val)
        await query.edit_message_text(
            f"⚙️ Safety score порог: {new_val:.0f}", reply_markup=_settings_menu_markup(),
        )

    elif data == "mode_toggle":
        if runtime_state.get("dry_run"):
            # DRY RUN -> LIVE — это реальные деньги, спрашиваем подтверждение
            await query.edit_message_text(
                "⚠️ Включить LIVE-режим? Бот начнёт выставлять реальные ордера на Polymarket.",
                reply_markup=_confirm_live_markup(),
            )
        else:
            runtime_state.set("dry_run", True)
            await query.edit_message_text(
                "🧪 Переключено в DRY RUN — реальные сделки остановлены.",
                reply_markup=_main_menu_markup(),
            )

    elif data == "mode_confirm_live":
        runtime_state.set("dry_run", False)
        await query.edit_message_text(
            "🔴 LIVE включён. Бот будет выставлять реальные ордера на реальные деньги.",
            reply_markup=_main_menu_markup(),
        )


def build_app() -> Application:
    global _app
    _app = Application.builder().token(settings.TELEGRAM_BOT_TOKEN).build()
    _app.add_handler(CommandHandler("start", _cmd_start_or_menu))
    _app.add_handler(CommandHandler("menu", _cmd_start_or_menu))
    _app.add_handler(CommandHandler("status", _cmd_status))
    _app.add_handler(CommandHandler("pnl", _cmd_pnl))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    return _app


async def notify(text: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text)
