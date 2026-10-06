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
from src import storage, runtime_state, loss_stats

_app: Application | None = None
_state_ref: dict = {}  # заполняется из main.py: последний сигнал/статус для меню
_pending_input: str | None = None  # "size" | "stoploss" | None — ждём ли текстовый ввод числа

SIZE_PRESETS = [5, 10, 20, 50, 100]
STOPLOSS_PRESETS = [20, 50, 100, 200]
POSITION_SL_PRESETS = [20, 30, 50, 70]
SCORE_PRESETS = [75, 85, 88, 92]
# Мин. расстояние от страйка, % от цены (0 = выкл). Для BTC ~84k: 0.05% ≈ $42, 0.07% ≈ $59, 0.10% ≈ $84.
DISTANCE_PRESETS = [0.0, 0.05, 0.07, 0.10]


def set_state_ref(state: dict) -> None:
    global _state_ref
    _state_ref = state


# ---------------------------------------------------------------- меню ----

def _size_summary() -> str:
    if runtime_state.get("sizing_mode") == "percent":
        size_now = runtime_state.compute_trade_size()
        return f"{runtime_state.get('bankroll_pct'):.0f}% банка (сейчас {size_now:.2f} USDC)"
    return f"{runtime_state.get('trade_size_usdc'):.2f} USDC (фикс.)"


def _distance_summary() -> str:
    pct = runtime_state.get("min_distance_pct") or 0.0
    if pct <= 0:
        return "выкл"
    return f"{pct:.2f}% (≈${pct / 100 * 84000:.0f} для BTC)"


def _settings_text() -> str:
    scaling_on = runtime_state.get("size_scaling_enabled")
    scaling_line = (
        f"Размер ставки масштабируется от порога: на пограничном score — "
        f"{settings.SIZE_SCALING_MIN_FRACTION*100:.0f}% от размера позиции, "
        f"на score {settings.SIZE_SCALING_MAX_SCORE:.0f}+ — полный размер."
        if scaling_on else
        "Масштабирование выключено — любая прошедшая порог сделка идёт полным размером."
    )
    return (
        f"⚙️ Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}\n"
        "Чем выше — тем реже и осторожнее входы.\n\n"
        f"📏 Мин. расстояние от страйка: {_distance_summary()}\n"
        "Не входить, если цена ближе к страйку, чем этот % от цены.\n\n"
        + scaling_line
    )


_MODE_ICON = {"live": "🔴", "dry": "🧪", "off": "⏸"}


def _assets_modes_line() -> str:
    """'BTC 🔴, ETH 🧪, SOL 🧪' — включённые монеты с режимом: 🔴 реальные
    сделки, 🧪 виртуальные (DRY)."""
    return ", ".join(
        f"{a.upper()} {_MODE_ICON[runtime_state.asset_mode(a)]}"
        for a in settings.ASSETS if runtime_state.is_asset_enabled(a)
    )


def _main_menu_text() -> str:
    s = _state_ref  # dict: "asset:timeframe" -> instance state
    paused = runtime_state.get("paused")
    dry_run = runtime_state.get("dry_run")
    pos_sl_on = runtime_state.get("position_stop_loss_enabled")
    enabled_assets = runtime_state.get_enabled_assets()
    lines = [
        "🤖 *Polymarket Multi-Asset Bot*",
        "",
        f"Статус: {'⏸ на паузе' if paused else '▶️ активен'} | Режим: {'🧪 DRY RUN' if dry_run else '🔴 LIVE'}",
        f"Активы: {_assets_modes_line() or '(нет включённых)'}",
        f"Размер позиции: {_size_summary()}",
        f"Стоп-лосс/день: {runtime_state.get('daily_loss_limit_usdc'):.0f} USDC",
        f"Стоп-лосс позиции: {'вкл ' + str(round(runtime_state.get('position_stop_loss_pct'))) + '%' if pos_sl_on else 'выкл'}",
        f"Safety score порог: {runtime_state.get('safety_score_threshold'):.0f}",
        f"Диапазон входа: {runtime_state.get('min_entry_price'):.2f}–{runtime_state.get('max_entry_price'):.2f}",
        f"Мин. расстояние от страйка: {_distance_summary()}",
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
        [InlineKeyboardButton("🪙 Активы", callback_data="menu:assets")],
        [
            InlineKeyboardButton("💰 Размер позиции", callback_data="menu:size"),
            InlineKeyboardButton("🛑 Стоп-лосс/день", callback_data="menu:sl"),
        ],
        [
            InlineKeyboardButton("📉 Стоп-лосс позиции", callback_data="menu:possl"),
            InlineKeyboardButton("📈 Диапазон входа", callback_data="menu:range"),
        ],
        [InlineKeyboardButton("🐋 Слежка за кошельком", callback_data="menu:wallet")],
        [InlineKeyboardButton("🔒 Хедж-бот", callback_data="menu:hedge")],
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


def _assets_menu_markup() -> InlineKeyboardMarkup:
    """Строка на монету: [✅/⏸ МОНЕТА] — включить/выключить поток,
    [🔴 LIVE / 🧪 DRY] — реальные или виртуальные сделки по этой монете."""
    enabled = runtime_state.get_enabled_assets()
    rows = []
    for asset in settings.ASSETS:
        on = asset in enabled
        live = runtime_state.is_asset_live(asset)
        rows.append([
            InlineKeyboardButton(f"{'✅' if on else '⏸'} {asset.upper()}", callback_data=f"asset_toggle:{asset}"),
            InlineKeyboardButton("🔴 LIVE" if live else "🧪 DRY", callback_data=f"asset_mode:{asset}"),
        ])
    notify_on = runtime_state.get("notify_dry_assets")
    rows.append([InlineKeyboardButton(
        "🔔 Уведомления DRY-монет: вкл" if notify_on else "🔕 Уведомления DRY-монет: выкл",
        callback_data="dry_notify_toggle",
    )])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _assets_menu_text() -> str:
    enabled = runtime_state.get_enabled_assets()
    general = "🧪 DRY RUN — все сделки виртуальные" if runtime_state.get("dry_run") else "🔴 LIVE"
    return (
        f"🪙 Монеты: включено {len(enabled)} из {len(settings.ASSETS)}. Общий режим бота: {general}.\n\n"
        "Левая кнопка — включить/выключить монету. Правая — режим монеты:\n"
        "🔴 LIVE — реальные сделки (когда сам бот в LIVE),\n"
        "🧪 DRY — виртуальные сделки: стратегия проверяется на этой монете без денег.\n\n"
        "Новые монеты всегда начинают с DRY. Каждый тик по каждой включённой монете "
        "попадает в 4-часовой отчёт — по нему видно, на какой монете стратегия работает."
    )


BANKROLL_PCT_PRESETS = [3, 5, 7, 10]
COPYTRADE_SIZE_PRESETS = [2, 5, 10, 20]


HEDGE_STAKE_PRESETS = [2, 5, 10, 20]
HEDGE_TRIGGER_PRESETS = [0.85, 0.88, 0.90, 0.93]


def _hedge_menu_markup() -> InlineKeyboardMarkup:
    enabled = runtime_state.get("hedge_bot_enabled")
    entry = runtime_state.get("hedge_entry_price")
    trigger = runtime_state.get("hedge_trigger_price")
    stake = runtime_state.get("hedge_stake_usdc")

    rows = [
        [InlineKeyboardButton(
            "🔴 Выключить хедж-бота" if enabled else "🟢 Включить хедж-бота",
            callback_data="hedge_toggle",
        )],
        [InlineKeyboardButton("— Порог хеджа (сейчас {:.2f}) —".format(trigger), callback_data="noop")],
    ]
    row = []
    for val in HEDGE_TRIGGER_PRESETS:
        mark = "✅ " if abs(val - trigger) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"hedgetrigger_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("— Размер ставки —", callback_data="noop")])
    row = []
    for val in HEDGE_STAKE_PRESETS:
        mark = "✅ " if abs(val - stake) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"hedgestake_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Свой размер ставки", callback_data="hedgestake_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _wallet_menu_markup() -> InlineKeyboardMarkup:
    notify_on = runtime_state.get("wallet_notify_enabled")
    copy_on = runtime_state.get("wallet_copytrade_enabled")
    size = runtime_state.get("copytrade_size_usdc")

    rows = [
        [InlineKeyboardButton(
            "🔔 Уведомления: выкл" if not notify_on else "🔕 Уведомления: вкл",
            callback_data="wallet_notify_toggle",
        )],
        [InlineKeyboardButton(
            "🟢 Включить копитрейдинг" if not copy_on else "🔴 Выключить копитрейдинг",
            callback_data="wallet_copytrade_toggle",
        )],
        [InlineKeyboardButton("— Размер копи-сделки —", callback_data="noop")],
    ]
    row = []
    for val in COPYTRADE_SIZE_PRESETS:
        mark = "✅ " if abs(val - size) < 0.01 else ""
        row.append(InlineKeyboardButton(f"{mark}{val}", callback_data=f"copysize_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✏️ Свой размер копи-сделки", callback_data="copysize_custom")])
    rows.append([InlineKeyboardButton("◀️ Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def _size_menu_markup() -> InlineKeyboardMarkup:
    mode = runtime_state.get("sizing_mode")
    rows = [[InlineKeyboardButton(
        "🔀 Режим: % от банка" if mode == "fixed" else "🔀 Режим: фикс. сумма",
        callback_data="sizing_mode_toggle",
    )]]

    if mode == "percent":
        row = []
        for val in BANKROLL_PCT_PRESETS:
            mark = "✅ " if abs(val - runtime_state.get("bankroll_pct")) < 0.01 else ""
            row.append(InlineKeyboardButton(f"{mark}{val}%", callback_data=f"bankrollpct_set:{val}"))
            if len(row) == 2:
                rows.append(row)
                row = []
        if row:
            rows.append(row)
        rows.append([
            InlineKeyboardButton("−1%", callback_data="bankrollpct_delta:-1"),
            InlineKeyboardButton("+1%", callback_data="bankrollpct_delta:+1"),
        ])
        rows.append([InlineKeyboardButton("✏️ Свой стартовый банк", callback_data="startbank_custom")])
    else:
        current = runtime_state.get("trade_size_usdc")
        row = []
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


MIN_ENTRY_PRESETS = [0.80, 0.85, 0.87, 0.90]
MAX_ENTRY_PRESETS = [0.93, 0.95, 0.97]


def _range_menu_markup() -> InlineKeyboardMarkup:
    cur_min = runtime_state.get("min_entry_price")
    cur_max = runtime_state.get("max_entry_price")
    rows = [[InlineKeyboardButton("— Минимум —", callback_data="noop")]]
    row = []
    for val in MIN_ENTRY_PRESETS:
        mark = "✅ " if abs(val - cur_min) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"minentry_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−0.01", callback_data="minentry_delta:-0.01"),
        InlineKeyboardButton("+0.01", callback_data="minentry_delta:+0.01"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой минимум", callback_data="minentry_custom")])

    rows.append([InlineKeyboardButton("— Максимум —", callback_data="noop")])
    row = []
    for val in MAX_ENTRY_PRESETS:
        mark = "✅ " if abs(val - cur_max) < 0.001 else ""
        row.append(InlineKeyboardButton(f"{mark}{val:.2f}", callback_data=f"maxentry_set:{val}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton("−0.01", callback_data="maxentry_delta:-0.01"),
        InlineKeyboardButton("+0.01", callback_data="maxentry_delta:+0.01"),
    ])
    rows.append([InlineKeyboardButton("✏️ Свой максимум", callback_data="maxentry_custom")])

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
    rows.append([InlineKeyboardButton("— 📏 Мин. расстояние от страйка —", callback_data="noop")])
    cur_dist = runtime_state.get("min_distance_pct") or 0.0
    drow = []
    for val in DISTANCE_PRESETS:
        mark = "✅ " if abs(val - cur_dist) < 1e-6 else ""
        label = "выкл" if val == 0 else f"{val:.2f}%"
        drow.append(InlineKeyboardButton(f"{mark}{label}", callback_data=f"dist_set:{val}"))
    rows.append(drow)
    rows.append([InlineKeyboardButton("✏️ Своё расстояние, %", callback_data="dist_custom")])
    rows.append([InlineKeyboardButton("⭐ Рекомендованные настройки", callback_data="preset_apply")])
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

def _owner_chat_id() -> int | None:
    try:
        return int(str(settings.TELEGRAM_CHAT_ID).strip())
    except (TypeError, ValueError):
        return None


def _is_owner(update: Update) -> bool:
    """Управлять ботом может только чат из TELEGRAM_CHAT_ID (тот же, куда
    приходят уведомления). Раньше проверки не было: любой, кто найдёт имя
    бота, мог открыть /menu и нажать кнопки — вплоть до LIVE и размера
    ставки. Если TELEGRAM_CHAT_ID не задан, ограничение не включаем, чтобы
    не запереть владельца."""
    owner = _owner_chat_id()
    if owner is None:
        return True
    chat = getattr(update, "effective_chat", None)
    return chat is not None and getattr(chat, "id", None) == owner


async def _cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/export — ВСЕ сделки из базы одним CSV (в 4-часовых отчётах только
    сделки своего окна, и по ним легко недосчитаться проигрышей)."""
    if not _is_owner(update):
        return
    import csv
    import os
    rows = storage.get_trades_since(0)
    os.makedirs(settings.REPORTS_DIR, exist_ok=True)
    path = os.path.join(settings.REPORTS_DIR, time.strftime("all_trades_%Y%m%d-%H%M.csv", time.gmtime()))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(storage.TRADES_COLUMNS)
        w.writerows(rows)
    live = storage.get_outcome_stats(live=True)
    caption = f"📥 Все сделки: {len(rows)}\n" + loss_stats.short_line("LIVE", live)
    await send_document(path, caption)


async def _cmd_start_or_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(
        _main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown",
    )


async def _cmd_pnl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
    await update.message.reply_text(_stats_text(), parse_mode="Markdown")


async def _cmd_token(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_owner(update):
        return
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

    t_live = storage.get_pnl_summary(today_start, live_only=True)
    a_live = storage.get_pnl_summary(0, live_only=True)
    t_dry = storage.get_pnl_summary(today_start, dry_only=True)
    a_dry = storage.get_pnl_summary(0, dry_only=True)

    lines = [
        "📊 *Статистика*",
        "",
        f"LIVE сегодня: {loss_stats.plural_trades(t_live['trades'])}, PnL {t_live['pnl_usdc']:+.2f} USDC, побед {t_live['wins']}",
        f"LIVE всего: {loss_stats.plural_trades(a_live['trades'])}, PnL {a_live['pnl_usdc']:+.2f} USDC, побед {a_live['wins']}",
    ]
    if a_dry["trades"]:
        # Виртуальный PnL — отдельно, чтобы не путать с реальными деньгами
        lines.append(f"DRY сегодня: {loss_stats.plural_trades(t_dry['trades'])}, PnL {t_dry['pnl_usdc']:+.2f} (виртуально), побед {t_dry['wins']}")
        lines.append(f"DRY всего: {loss_stats.plural_trades(a_dry['trades'])}, PnL {a_dry['pnl_usdc']:+.2f} (виртуально), побед {a_dry['wins']}")

    # Нормальные ли проигрыши: факт против того, что заложено в цены входа
    # (см. src/loss_stats.py). Отдельно LIVE и DRY RUN — их нельзя смешивать.
    try:
        live_st = storage.get_outcome_stats(live=True)
        dry_st = storage.get_outcome_stats(live=False)
        lines.append("")
        lines.append("*Проигрыши: факт против цены входа*")
        lines.extend(loss_stats.summary_lines("LIVE", live_st))
        if dry_st["trades"]:
            lines.extend(loss_stats.summary_lines("DRY RUN", dry_st))
        lines.append("Все сделки одним файлом: /export")
    except Exception:  # noqa: BLE001 — статистика не должна ломать меню
        pass

    # По каждой монете отдельно для LIVE и DRY: так видно, на какой монете
    # стратегия даёт меньше проигрышей, чем заложено в цены, а на какой нет.
    try:
        by = storage.get_pnl_by_asset_mode(0)
        if by:
            lines.append("")
            lines.append("*По монетам:*")
            for asset, dry in sorted(by.keys(), key=lambda k: (k[1], k[0])):
                st = storage.get_outcome_stats(live=not dry, asset=asset)
                label = f"{asset.upper()} {'DRY' if dry else 'LIVE'}"
                if st["trades"]:
                    lines.append(f"  {loss_stats.short_line(label, st)}, PnL {st['pnl_usdc']:+.2f}")
    except Exception:  # noqa: BLE001
        pass

    return "\n".join(lines)


async def _on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловит обычный текст, когда мы ждём число после '✏️ Свой размер'/'✏️ Свой лимит'.
    Вне этого режима ничего не делает — не мешает обычной переписке."""
    global _pending_input
    if _pending_input is None or not _is_owner(update):
        return

    raw = (update.message.text or "").strip().replace(",", ".")
    try:
        value = float(raw)
        if value < 0 or (value == 0 and _pending_input != "min_distance"):
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
    elif _pending_input == "min_entry":
        value = round(min(value, runtime_state.get("max_entry_price") - 0.01), 2)
        runtime_state.set("min_entry_price", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Минимум диапазона входа: {value:.2f}", reply_markup=_range_menu_markup(),
        )
    elif _pending_input == "max_entry":
        value = round(max(value, runtime_state.get("min_entry_price") + 0.01), 2)
        value = min(value, 0.99)
        runtime_state.set("max_entry_price", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Максимум диапазона входа: {value:.2f}", reply_markup=_range_menu_markup(),
        )
    elif _pending_input == "starting_bankroll":
        runtime_state.set("starting_bankroll_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Стартовый банк: {value:.2f} USDC", reply_markup=_size_menu_markup(),
        )
    elif _pending_input == "copytrade_size":
        runtime_state.set("copytrade_size_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Размер копи-сделки: {value:.2f} USDC", reply_markup=_wallet_menu_markup(),
        )
    elif _pending_input == "min_distance":
        value = min(value, 5.0)
        runtime_state.set("min_distance_pct", value)
        _pending_input = None
        await update.message.reply_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())
    elif _pending_input == "hedge_stake":
        runtime_state.set("hedge_stake_usdc", value)
        _pending_input = None
        await update.message.reply_text(
            f"✅ Размер ставки хеджа: {value:.2f} USDC", reply_markup=_hedge_menu_markup(),
        )


# ------------------------------------------------------------- кнопки -----

async def _on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _pending_input
    query = update.callback_query
    if not _is_owner(update):
        await query.answer()
        return
    data = query.data
    await query.answer()

    if data == "menu:main":
        await query.edit_message_text(_main_menu_text(), reply_markup=_main_menu_markup(), parse_mode="Markdown")

    elif data == "menu:size":
        mode = runtime_state.get("sizing_mode")
        if mode == "percent":
            bank = runtime_state.current_bankroll()
            size_now = runtime_state.compute_trade_size()
            text = (
                f"💰 Режим: % от банка\n"
                f"Стартовый банк: {runtime_state.get('starting_bankroll_usdc'):.2f} USDC\n"
                f"Текущий банк (старт + реализованный PnL): {bank:.2f} USDC\n"
                f"Доля на сделку: {runtime_state.get('bankroll_pct'):.0f}% → сейчас это {size_now:.2f} USDC\n\n"
                "Размер сам растёт на прибыли и сжимается на просадке."
            )
        else:
            text = f"💰 Режим: фиксированная сумма — {runtime_state.get('trade_size_usdc'):.2f} USDC на сделку."
        await query.edit_message_text(text, reply_markup=_size_menu_markup())

    elif data == "sizing_mode_toggle":
        new_mode = "percent" if runtime_state.get("sizing_mode") == "fixed" else "fixed"
        runtime_state.set("sizing_mode", new_mode)
        await query.edit_message_text(
            f"✅ Режим размера ставки: {'% от банка' if new_mode=='percent' else 'фиксированная сумма'}",
            reply_markup=_size_menu_markup(),
        )

    elif data.startswith("bankrollpct_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("bankroll_pct", val)
        await query.edit_message_text(f"✅ Доля от банка: {val:.0f}%", reply_markup=_size_menu_markup())

    elif data.startswith("bankrollpct_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = min(50.0, max(1.0, runtime_state.get("bankroll_pct") + delta))
        runtime_state.set("bankroll_pct", new_val)
        await query.edit_message_text(f"✅ Доля от банка: {new_val:.0f}%", reply_markup=_size_menu_markup())

    elif data == "startbank_custom":
        _pending_input = "starting_bankroll"
        await query.edit_message_text(
            "✏️ Напиши стартовый банк в USDC следующим сообщением, например: 60",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:size")]]),
        )

    elif data == "menu:hedge":
        enabled = runtime_state.get("hedge_bot_enabled")
        await query.edit_message_text(
            f"🔒 Хедж-бот: {'🟢 включён' if enabled else '🔴 выключен'}\n"
            f"Вход при цене {runtime_state.get('hedge_entry_price'):.2f}, хедж противоположной стороны при "
            f"{runtime_state.get('hedge_trigger_price'):.2f}\n"
            f"Размер ставки: {runtime_state.get('hedge_stake_usdc'):.2f} USDC\n\n"
            "Если цена не доходит до порога хеджа — остаётся односторонняя позиция "
            "(по нашим данным такие случаи почти всегда проигрывают). "
            "PnL в отчётах — без учёта комиссии тейкера.",
            reply_markup=_hedge_menu_markup(),
        )

    elif data == "hedge_toggle":
        new_val = not runtime_state.get("hedge_bot_enabled")
        runtime_state.set("hedge_bot_enabled", new_val)
        await query.edit_message_text(
            f"{'🟢 Хедж-бот включён' if new_val else '🔴 Хедж-бот выключен'}",
            reply_markup=_hedge_menu_markup(),
        )

    elif data.startswith("hedgetrigger_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("hedge_trigger_price", val)
        await query.edit_message_text(f"✅ Порог хеджа: {val:.2f}", reply_markup=_hedge_menu_markup())

    elif data.startswith("hedgestake_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("hedge_stake_usdc", val)
        await query.edit_message_text(f"✅ Размер ставки хеджа: {val:.2f} USDC", reply_markup=_hedge_menu_markup())

    elif data == "hedgestake_custom":
        _pending_input = "hedge_stake"
        await query.edit_message_text(
            "✏️ Напиши размер ставки хеджа в USDC следующим сообщением, например: 7.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:hedge")]]),
        )

    elif data == "menu:wallet":
        addr = settings.WALLET_TRACK_ADDRESS
        notify_on = runtime_state.get("wallet_notify_enabled")
        copy_on = runtime_state.get("wallet_copytrade_enabled")
        await query.edit_message_text(
            f"🐋 Слежу за кошельком:\n`{addr}`\n\n"
            f"Уведомления: {'🔔 включены' if notify_on else '🔕 выключены'}\n"
            f"Копитрейдинг: {'🟢 включён' if copy_on else '🔴 выключен'} "
            f"(размер: {runtime_state.get('copytrade_size_usdc'):.2f} USDC на сделку)\n\n"
            "Копитрейдинг использует те же лимиты риска, что и основная стратегия "
            "(дневной стоп-лосс, общий потолок открытых позиций).",
            reply_markup=_wallet_menu_markup(),
            parse_mode="Markdown",
        )

    elif data == "wallet_notify_toggle":
        new_val = not runtime_state.get("wallet_notify_enabled")
        runtime_state.set("wallet_notify_enabled", new_val)
        await query.edit_message_text(
            f"{'🔔 Уведомления включены' if new_val else '🔕 Уведомления выключены'}",
            reply_markup=_wallet_menu_markup(),
        )

    elif data == "wallet_copytrade_toggle":
        new_val = not runtime_state.get("wallet_copytrade_enabled")
        runtime_state.set("wallet_copytrade_enabled", new_val)
        msg = (
            "🟢 Копитрейдинг включён — бот будет пытаться повторять его входы реальными "
            "(или dry-run) сделками." if new_val else
            "🔴 Копитрейдинг выключен — только уведомления, без автоматических сделок."
        )
        await query.edit_message_text(msg, reply_markup=_wallet_menu_markup())

    elif data.startswith("copysize_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("copytrade_size_usdc", val)
        await query.edit_message_text(f"✅ Размер копи-сделки: {val:.2f} USDC", reply_markup=_wallet_menu_markup())

    elif data == "copysize_custom":
        _pending_input = "copytrade_size"
        await query.edit_message_text(
            "✏️ Напиши размер копи-сделки в USDC следующим сообщением, например: 7.5",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:wallet")]]),
        )

    elif data == "menu:assets":
        await query.edit_message_text(_assets_menu_text(), reply_markup=_assets_menu_markup())

    elif data.startswith("asset_toggle:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        now_enabled = runtime_state.toggle_asset(asset)
        mode = "реальные сделки" if not runtime_state.trade_is_dry(asset) else "виртуальные сделки (DRY)"
        await query.edit_message_text(
            f"{'✅' if now_enabled else '⏸'} {asset.upper()} теперь "
            f"{'включён — ' + mode if now_enabled else 'выключен'}.\n\n" + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

    elif data.startswith("asset_mode:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        if runtime_state.is_asset_live(asset):
            # LIVE -> DRY — безопасное направление, без подтверждения
            runtime_state.set_asset_live(asset, False)
            await query.edit_message_text(
                f"🧪 {asset.upper()} переведён в DRY: дальше по нему только виртуальные сделки.\n\n"
                + _assets_menu_text(),
                reply_markup=_assets_menu_markup(),
            )
        else:
            await query.edit_message_text(
                f"⚠️ Включить реальные сделки по {asset.upper()}?\n"
                "Когда бот в LIVE, по этой монете пойдут настоящие ордера. Сначала стоит "
                "убедиться в 📊 Статистике, что в DRY по ней проигрышей заметно меньше, "
                "чем заложено в цены.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"✅ Да, LIVE для {asset.upper()}", callback_data=f"asset_live_confirm:{asset}")],
                    [InlineKeyboardButton("❌ Оставить DRY", callback_data="menu:assets")],
                ]),
            )

    elif data.startswith("asset_live_confirm:"):
        asset = data.split(":", 1)[1]
        if asset not in settings.ASSETS:
            return
        runtime_state.set_asset_live(asset, True)
        note = ("" if not runtime_state.get("dry_run")
                else "\nСам бот сейчас в DRY RUN — реальные сделки пойдут, когда включишь LIVE в главном меню.")
        await query.edit_message_text(
            f"🔴 {asset.upper()} помечен для реальных сделок.{note}\n\n" + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

    elif data == "dry_notify_toggle":
        new_val = not runtime_state.get("notify_dry_assets")
        runtime_state.set("notify_dry_assets", new_val)
        await query.edit_message_text(
            ("🔔 Уведомления о виртуальных сделках DRY-монет включены.\n\n" if new_val
             else "🔕 Уведомления о виртуальных сделках DRY-монет выключены — они видны в 📊 Статистике и отчётах.\n\n")
            + _assets_menu_text(),
            reply_markup=_assets_menu_markup(),
        )

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
            "не дожидаясь исхода. Это отдельно от дневного лимита в USDC.\n\n"
            "Что показала история (18.09–06.10, ~2700 рынков BTC 5m и все 213 реальных сделок): "
            "прибыль стоп не увеличивает. Проигрышная позиция падает с 0.9 до 0.4 за секунды, "
            "поэтому продаёт он обычно уже около −50…−80%, а примерно 4 из 10 позиций, просевших "
            "на 50%, потом всё равно выигрывают — их стоп продаёт в минус. На реальных сделках "
            "25.09–06.10 стоп 50% дал бы +$66 вместо +$114. Уменьшить боль от проигрышей "
            "честнее размером ставки (💰 Размер позиции).",
            reply_markup=_position_sl_menu_markup(),
        )

    elif data == "menu:range":
        await query.edit_message_text(
            f"📈 Диапазон входа: {runtime_state.get('min_entry_price'):.2f} — "
            f"{runtime_state.get('max_entry_price'):.2f}\n\n"
            "Бот входит, только если ask на нужной стороне попадает в этот диапазон "
            "(и score выше порога). Шире диапазон — больше сигналов, но ниже средняя "
            "цена входа (более рискованные, менее 'подтверждённые' рынком ситуации).",
            reply_markup=_range_menu_markup(),
        )

    elif data == "noop":
        pass

    elif data.startswith("minentry_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("min_entry_price", val)
        await query.edit_message_text(
            f"✅ Минимум диапазона входа: {val:.2f}", reply_markup=_range_menu_markup(),
        )

    elif data.startswith("minentry_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = round(min(runtime_state.get("max_entry_price") - 0.01, max(0.5, runtime_state.get("min_entry_price") + delta)), 2)
        runtime_state.set("min_entry_price", new_val)
        await query.edit_message_text(
            f"✅ Минимум диапазона входа: {new_val:.2f}", reply_markup=_range_menu_markup(),
        )

    elif data == "minentry_custom":
        _pending_input = "min_entry"
        await query.edit_message_text(
            "✏️ Напиши минимальную цену входа следующим сообщением, например: 0.82",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:range")]]),
        )

    elif data.startswith("maxentry_set:"):
        val = float(data.split(":", 1)[1])
        runtime_state.set("max_entry_price", val)
        await query.edit_message_text(
            f"✅ Максимум диапазона входа: {val:.2f}", reply_markup=_range_menu_markup(),
        )

    elif data.startswith("maxentry_delta:"):
        delta = float(data.split(":", 1)[1])
        new_val = round(max(runtime_state.get("min_entry_price") + 0.01, min(0.99, runtime_state.get("max_entry_price") + delta)), 2)
        runtime_state.set("max_entry_price", new_val)
        await query.edit_message_text(
            f"✅ Максимум диапазона входа: {new_val:.2f}", reply_markup=_range_menu_markup(),
        )

    elif data == "maxentry_custom":
        _pending_input = "max_entry"
        await query.edit_message_text(
            "✏️ Напиши максимальную цену входа следующим сообщением, например: 0.96",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:range")]]),
        )

    elif data == "menu:settings":
        await query.edit_message_text(_settings_text(), reply_markup=_settings_menu_markup())

    elif data.startswith("dist_set:"):
        runtime_state.set("min_distance_pct", float(data.split(":", 1)[1]))
        await query.edit_message_text("✅ " + _settings_text(), reply_markup=_settings_menu_markup())

    elif data == "dist_custom":
        _pending_input = "min_distance"
        await query.edit_message_text(
            "✏️ Напиши минимальное расстояние от страйка в % от цены, например: 0.07\n"
            "(для BTC ~84k: 0.05 ≈ $42, 0.07 ≈ $59, 0.10 ≈ $84). 0 — выключить фильтр.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="menu:settings")]]),
        )

    elif data == "preset_apply":
        runtime_state.apply_recommended()
        await query.edit_message_text(
            "⭐ Применены рекомендованные настройки: порог 88, диапазон "
            f"{runtime_state.get('min_entry_price'):.2f}–{runtime_state.get('max_entry_price'):.2f}.\n\n"
            + _settings_text(),
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
    _app.add_handler(CommandHandler("export", _cmd_export))
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
