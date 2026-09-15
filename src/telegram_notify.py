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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

from config import settings
from src import storage, runtime_state

_app: Application | None = None
_state_ref: dict = {}  # заполняется из main.py: последний сигнал/статус для меню
_pending_input: str | None = None  # "size" | "stoploss" | None — ждём ли текстовый ввод числа

SIZE_PRESETS = [5, 10, 20, 50, 100]
STOPLOSS_PRESETS = [20, 50, 100, 200]
POSITION_SL_PRESETS = [20, 30, 50, 70]
SCORE_PRESETS = [65, 75, 85, 90]


def set_state_ref(state: dict) -> None:
    global _state_ref
    _state_ref = state


# ---------------------------------------------------------------- меню ----

def _main_menu_text() -> str:
    s = _state_ref  # dict: "asset:timeframe" -> instance state
    paused = runtime_state.get("paused")
    dry_run = runtime_state.get("dry_run")
    pos_sl_on = runtime_state.get("position_stop_loss_enabled")
    lines = [
        "🤖 *Polymarket Multi-Asset Bot*",
        "",
        f"Статус: {'⏸ на паузе' if paused else '▶️ активен'} | Режим: {'🧪 DRY RUN' if dry_run else '🔴 LIVE'}",
        f"Размер позиции: {runtime_state.get('trade_size_usdc'):.0f} USDC",
        f"Стоп-лосс/день: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC",
        f"Стоп-лосс позиции: {'вкл ' + str(round(runtime_state.get('position_stop_loss_pct'))) + '%' if pos_sl_on else 'выкл'}",
        f"Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}",
    ]
    if s:
        lines.append("")
        lines.append(f"Потоков активно: {len(s)}")
        # Сортируем по активу, потом по таймфрейму — стабильный порядок в UI
        for key in sorted(s.keys()):
            inst = s[key]
            lines.append(
                f"  {inst['asset'].upper()} {inst['timeframe']}: {inst.get('direction','—')} "
                f"score {inst.get('safety_score','—')}"
            )
    return "\n".join(lines)


def _main_menu_markup() -> InlineKeyboardMarkup:
    paused = runtime_state.get("paused")
    rows = [
        [InlineKeyboardButton("▶️ Старт" if paused else "⏸ Стоп", callback_data="pause_toggle")],
        [
            InlineKeyboardButton("💰 Размер позиции", callback_data="menu:size"),
            InlineKeyboardButton("🛑 Стоп-лосс/день", callback_data="menu:sl"),
        ],
        [
            InlineKeyboardButton("📉 Стоп-лосс позиции", callback_data="menu:possl"),
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
    rows.append([InlineKeyboardButton("✏️ Свой размер", callback_data="size_custom")])
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
    rows.append([InlineKeyboardButton("✏️ Свой лимит", callback_data="sl_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _position_sl_menu_markup() -> InlineKeyboardMarkup:
    current = runtime_state.get("position_stop_loss_pct")
    enabled = runtime_state.get("position_stop_loss_enabled")
    row = []
    rows = []
    for val in POSITION_SL_PRESETS:
        mark = "✅ " if abs(val - current) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}%", callback_data=f"possl_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−5", callback_data="possl_delta:-5"),
        InlineKeyboardButton("+5", callback_data="possl_delta:+5"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой процент", callback_data="possl_custom")])
    rows.append([InlineKeyboardButton(
        "🔴 Выключить" if enabled else "🟢 Включить", callback_data="possl_toggle",
    )])
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
    rows.append([
        InlineKeyboardButton("−1", callback_data="score_delta:-1"),
        InlineKeyboardButton("+1", callback_data="score_delta:+1"),
    ])
    scaling_on = runtime_state.get("size_scaling_enabled")
    rows.append([InlineKeyboardButton(
        "📉 Масштабировать размер по score" if not scaling_on else "💯 Входить полным размером",
        callback_data="scaling_toggle",
    )])
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
    await update.message.reply_text(_stats_text(), parse_mode="Markdown")


async def _cmd_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = _state_ref
    if not s:
        await update.message.reply_text("Пока нет данных ни по одному потоку — подожди первого тика бота.")
        return
    parts = []
    for key in sorted(s.keys()):
        inst = s[key]
        parts.append(
            f"*{inst['asset'].upper()} {inst['timeframe']}* — `{inst.get('market_slug', '—')}`\n"
            f"UP: `{inst.get('up_token_id', '—')}`\n"
            f"DOWN: `{inst.get('down_token_id', '—')}`"
        )
    await update.message.reply_text(
        "\n\n".join(parts) + "\n\nДолгий тап на строку с ID — скопировать.",
        parse_mode="Markdown",
    )


def _stats_text() -> str:
    today_start = int(time.time() // 86400) * 86400

    today = storage.get_pnl_summary(today_start)
    total = storage.get_pnl_summary(0)

    lines = [
        "📊 *Статистика*",
        "",
        f"Сегодня: {today['trades']} сделок, PnL {today['pnl_usdc']:+.2f} USDC, побед {today['wins']}",
        f"Всего: {total['trades']} сделок, PnL {total['pnl_usdc']:+.2f} USDC, побед {total['wins']}",
    ]

    by_asset_total = storage.get_pnl_by_asset(0)
    if by_asset_total:
        lines.append("")
        lines.append("*По токенам (всего):*")
        for asset in sorted(by_asset_total.keys()):
            b = by_asset_total[asset]
            lines.append(f"  {asset.upper()}: {b['trades']} сделок, PnL {b['pnl_usdc']:+.2f}, побед {b['wins']}")

    by_tf_total = storage.get_pnl_by_timeframe(0)
    if by_tf_total:
        lines.append("")
        lines.append("*По таймфреймам (всего):*")
        for label in sorted(by_tf_total.keys()):
            b = by_tf_total[label]
            lines.append(f"  {label}: {b['trades']} сделок, PnL {b['pnl_usdc']:+.2f}, побед {b['wins']}")

    return "\n".join(lines)


async def _on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловит обычный текст, когда мы ждём число после '✏️ Свой размер'/'✏️ Свой лимит'.
    Вне этого режима ничего не делает — не мешает обычной переписке."""
    global _pending_input
    if _pending_input is None:
        return

    raw = (update.message.text or "").strip().replace(",", ".")
    try:
        value = float(raw)
        if value <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Не похоже на положительное число, попробуй ещё раз (например: 15.5)")
        return

    if _pending_input == "size":
        runtime_state.set("trade_size_usdc", value)
        _pending_input = None
        await update.message.reply_text(f"✅ Размер позиции: {value:.2f} USDC", reply_markup=_size_menu_markup())
    elif _pending_input == "stoploss":
        runtime_state.set("daily_loss_limit_usdc", value)
        _pending_input = None
        await update.message.reply_text(f"✅ Стоп-лосс/день: {value:.2f} USDC", reply_markup=_stoploss_menu_markup())
    elif _pending_input == "position_sl":
        value = min(99.0, value)  # 100%+ бессмысленно — это уже полная потеря
        runtime_state.set("position_stop_loss_pct", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Стоп-лосс позиции: {value:.1f}%", reply_markup=_position_sl_menu_markup(),
        )


# ------------------------------------------------------------- кнопки -----

async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _pending_input
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

    elif data == "menu:possl":
        enabled = runtime_state.get("position_stop_loss_enabled")
        await query.edit_message_text(
            f"📉 Стоп-лосс ОТДЕЛЬНОЙ позиции: {'🟢 включён' if enabled else '🔴 выключен'}, "
            f"порог {runtime_state.get('position_stop_loss_pct'):.0f}%.\n\n"
            "Если стоимость открытой позиции (по текущей цене в стакане) падает на этот "
            "процент от суммы входа ещё ДО резолюции рынка — бот продаёт её досрочно, "
            "не дожидаясь исхода. Это отдельно от дневного лимита в USDC.",
            reply_markup=_position_sl_menu_markup(),
        )

    elif data == "menu:settings":
        scaling_on = runtime_state.get("size_scaling_enabled")
        scaling_line = (
            f"Размер ставки масштабируется от порога: на пограничном score — "
            f"{settings.SIZE_SCALING_MIN_FRACTION*100:.0f}% от размера позиции, "
            f"на score {settings.SIZE_SCALING_MAX_SCORE:.0f}+ — полный размер."
            if scaling_on else
            "Масштабирование выключено — любая прошедшая порог сделка идёт полным размером."
        )
        await query.edit_message_text(
            f"⚙️ Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}\n"
            "Чем выше — тем реже и осторожнее входы.\n\n" + scaling_line,
            reply_markup=_settings_menu_markup(),
        )

    elif data == "stats":
        await query.edit_message_text(_stats_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data == "pause_toggle":
        runtime_state.set("paused", not runtime_state.get("paused"))
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data.startswith("size_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("trade_size_usdc", val)
        await query.edit_message_text(f"✅ Размер позиции: {val:.0f} USDC", reply_markup=_size_menu_markup())

    elif data == "size_custom":
        _pending_input = "size"
        await query.edit_message_text(
            "✏️ Напиши число (USDC) следующим сообщением, например: 15.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:size")]]),
        )

    elif data.startswith("sl_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("daily_loss_limit_usdc", val)
        await query.edit_message_text(f"✅ Стоп-лосс: {val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data.startswith("sl_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = max(5.0, runtime_state.get("daily_loss_limit_usdc") + delta)
        runtime_state.set("daily_loss_limit_usdc", new_val)
        await query.edit_message_text(f"✅ Стоп-лосс: {new_val:.0f} USDC/день", reply_markup=_stoploss_menu_markup())

    elif data == "sl_custom":
        _pending_input = "stoploss"
        await query.edit_message_text(
            "✏️ Напиши число (USDC/день) следующим сообщением, например: 75",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:sl")]]),
        )

    elif data.startswith("possl_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("position_stop_loss_pct", val)
        await query.edit_message_text(f"✅ Стоп-лосс позиции: {val:.0f}%", reply_markup=_position_sl_menu_markup())

    elif data.startswith("possl_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(99.0, max(1.0, runtime_state.get("position_stop_loss_pct") + delta))
        runtime_state.set("position_stop_loss_pct", new_val)
        await query.edit_message_text(
            f"✅ Стоп-лосс позиции: {new_val:.0f}%", reply_markup=_position_sl_menu_markup(),
        )

    elif data == "possl_custom":
        _pending_input = "position_sl"
        await query.edit_message_text(
            "✏️ Напиши процент просадки следующим сообщением, например: 40",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:possl")]]),
        )

    elif data == "possl_toggle":
        new_val = not runtime_state.get("position_stop_loss_enabled")
        runtime_state.set("position_stop_loss_enabled", new_val)
        msg = (f"🟢 Стоп-лосс позиции включён, порог {runtime_state.get('position_stop_loss_pct'):.0f}%."
               if new_val else "🔴 Стоп-лосс позиции выключен — позиции держим до резолюции рынка в любом случае.")
        await query.edit_message_text(msg, reply_markup=_position_sl_menu_markup())

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

    elif data == "scaling_toggle":
        new_val = not runtime_state.get("size_scaling_enabled")
        runtime_state.set("size_scaling_enabled", new_val)
        msg = ("📉 Масштабирование включено — размер сделки зависит от score."
               if new_val else
               "💯 Масштабирование выключено — любая прошедшая порог сделка идёт полным размером.")
        await query.edit_message_text(msg, reply_markup=_settings_menu_markup())

    elif data == "mode_toggle":
        if runtime_state.get("dry_run"):
            if not settings.POLY_PRIVATE_KEY:
                await query.edit_message_text(
                    "❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан в переменных окружения.\n"
                    "Добавь ключ и передеплой бота, потом попробуй снова.",
                    reply_markup=_main_menu_markup(),
                )
                return
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
        if not settings.POLY_PRIVATE_KEY:
            # Двойная защита: ключ мог пропасть между нажатием "Старт" и подтверждением
            # (например, кто-то параллельно поменял env и не передеплоил).
            await query.edit_message_text(
                "❌ Нельзя включить LIVE: POLY_PRIVATE_KEY не задан. Остаёмся в DRY RUN.",
                reply_markup=_main_menu_markup(),
            )
            return
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
    _app.add_handler(CommandHandler("token", _cmd_token))
    _app.add_handler(CallbackQueryHandler(_on_callback))
    _app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _on_text))
    return _app


async def clear_legacy_keyboard() -> None:
    """
    Если этот бот-токен раньше использовался другой программой (например,
    copy-trading ботом со своей постоянной клавиатурой START/STOP/AMOUNT/...),
    Telegram продолжает показывать её внизу чата, пока что-то явно не пришлёт
    ReplyKeyboardRemove — наши собственные кнопки инлайновые и её не трогают.
    Вызывается один раз при старте.
    """
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    try:
        await _app.bot.send_message(
            chat_id=settings.TELEGRAM_CHAT_ID,
            text="🧹 Убираю старую клавиатуру, если она осталась от другого бота...",
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception:
        pass


async def notify(text: str) -> None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    await _app.bot.send_message(chat_id=settings.TELEGRAM_CHAT_ID, text=text)


async def send_document(path: str, caption: str | None) -> None:
    """Отправляет файл (например, CSV-отчёт) в чат. caption может быть None,
    если это второй файл в паре и подпись уже была у первого."""
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if _app is None:
        return
    with open(path, "rb") as f:
        await _app.bot.send_document(chat_id=settings.TELEGRAM_CHAT_ID, document=f, caption=caption)
