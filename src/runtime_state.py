"""
Настройки, которые можно менять на лету из Telegram (без передеплоя):
размер позиции, дневной стоп-лосс, порог safety score, диапазон входа,
пауза, режим DRY_RUN/LIVE.

Живут в памяти для быстрого доступа из strategy/executor на каждом тике,
но каждое изменение сразу пишется в SQLite (`bot_settings`) — переживает
рестарт процесса (важно на Render: контейнер может перезапуститься сам
по себе, не только по твоей команде).
"""
from __future__ import annotations

from config import settings
from src import storage

_DEFAULTS = {
    "paused": False,
    "dry_run": settings.DRY_RUN,
    "trade_size_usdc": settings.TRADE_SIZE_USDC,
    "daily_loss_limit_usdc": settings.DAILY_LOSS_LIMIT_USDC,
    "safety_score_threshold": settings.SAFETY_SCORE_THRESHOLD,
    "min_entry_price": settings.MIN_ENTRY_PRICE,
    "max_entry_price": settings.MAX_ENTRY_PRICE,
    # Мин. расстояние цены от страйка, % от цены (0 = выкл) — см. config.py.
    "min_distance_pct": settings.MIN_DISTANCE_PCT,
    # Версия применённого набора рекомендованных настроек (см. RECOMMENDED ниже).
    "preset_version": 0,
    # По умолчанию выключено: каждая прошедшая порог сделка идёт полным
    # TRADE_SIZE_USDC, без урезания по пограничности score.
    "size_scaling_enabled": False,
    # Стоп-лосс ОТДЕЛЬНОЙ позиции в процентах (не дневной!): если текущая
    # стоимость позиции (по best bid в стакане) упала настолько от суммы
    # входа — закрываем досрочно продажей, не дожидаясь резолюции рынка.
    "position_stop_loss_enabled": False,
    "position_stop_loss_pct": 50.0,
    # Какие активы сейчас реально торгуются — можно включать/выключать
    # по одному через Telegram, не трогая остальные и не передеплоя.
    # Хранится как строка через запятую (см. get/set_enabled_assets ниже).
    "enabled_assets": ",".join(settings.ASSETS),
    # Режим размера ставки: "fixed" (константа в USDC, trade_size_usdc) или
    # "percent" (доля от ТЕКУЩЕГО банка — starting_bankroll_usdc + вся
    # реализованная прибыль/убыток с начала). Percent-режим сам сжимается
    # при просадке и растёт при выигрышах — в отличие от fixed, который на
    # похудевшем банке становится относительно только агрессивнее.
    "sizing_mode": "fixed",
    "bankroll_pct": 5.0,
    "starting_bankroll_usdc": 60.0,
    # Отслеживание чужого кошелька: уведомления всегда можно включить
    # отдельно от реального копирования сделок (copytrade) — по умолчанию
    # только уведомляем, ничего не покупаем автоматически.
    "wallet_notify_enabled": True,
    "wallet_copytrade_enabled": False,
    "copytrade_size_usdc": 5.0,
    # Хедж-бот: вход по ENTRY_PRICE, докупка противоположной стороны при
    # достижении HEDGE_PRICE — см. src/hedge_bot.py. Пороги пришли из
    # анализа реальных momentum-отчётов (2026-09-20): хедж на 0.90 дал
    # положительный PnL на бэктесте, хедж на 0.70-0.85 — отрицательный,
    # несмотря на то, что сам хедж всегда безубыточен по построению —
    # разница в том, сколько сессий вообще НЕ доходит до точки хеджа и
    # остаётся неприкрытой позицией (см. обсуждение в чате).
    "hedge_bot_enabled": False,  # хедж живёт отдельным ботом (polymarket-hedge-bot)
    "hedge_entry_price": 0.70,
    "hedge_trigger_price": 0.90,
    "hedge_stake_usdc": 5.0,
}

# Типы приведения при чтении из SQLite (там всё хранится как TEXT)
_CASTERS = {
    "paused": lambda v: str(v).lower() == "true",
    "dry_run": lambda v: str(v).lower() == "true",
    "trade_size_usdc": float,
    "daily_loss_limit_usdc": float,
    "safety_score_threshold": float,
    "min_entry_price": float,
    "max_entry_price": float,
    "min_distance_pct": float,
    "preset_version": int,
    "size_scaling_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_pct": float,
    "enabled_assets": str,
    "sizing_mode": str,
    "bankroll_pct": float,
    "starting_bankroll_usdc": float,
    "wallet_notify_enabled": lambda v: str(v).lower() == "true",
    "wallet_copytrade_enabled": lambda v: str(v).lower() == "true",
    "copytrade_size_usdc": float,
    "hedge_bot_enabled": lambda v: str(v).lower() == "true",
    "hedge_entry_price": float,
    "hedge_trigger_price": float,
    "hedge_stake_usdc": float,
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    saved = storage.get_all_settings()
    for key, raw in saved.items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass
    _apply_preset_if_new()


# Рекомендованные настройки по итогам анализа отчётов 25-28.09 (5m и 15m BTC).
# Применяются ОДИН РАЗ при первом старте новой версии поверх того, что было
# сохранено в базе из Telegram (иначе старые значения из bot_settings, например
# порог 92, так и остались бы). Дальше можно спокойно менять из Telegram —
# повторно не перезапишутся, пока не поднимем PRESET_VERSION. Кнопка
# "⭐ Рекомендованные" в ⚙️ Настройках применяет их вручную ещё раз.
PRESET_VERSION = 1


def recommended() -> dict:
    return {
        "safety_score_threshold": 88.0,
        "min_entry_price": 0.90,
        "max_entry_price": 0.95,
        "min_distance_pct": settings.MIN_DISTANCE_PCT,
        # Хедж вынесен в отдельный бот; здесь он тратил бы тот же кошелёк
        # и в LIVE покупал бы по $5 на каждом рынке, где цена прошла 0.70.
        "hedge_bot_enabled": False,
    }


def apply_recommended() -> dict:
    rec = recommended()
    for key, value in rec.items():
        set(key, value)
    return rec


def _apply_preset_if_new() -> None:
    if int(_state.get("preset_version") or 0) >= PRESET_VERSION:
        return
    apply_recommended()
    set("preset_version", PRESET_VERSION)


def get(key: str):
    return _state[key]


def set(key: str, value) -> None:
    _state[key] = value
    storage.set_setting(key, value)


def snapshot() -> dict:
    return dict(_state)


# --- Включение/выключение отдельных активов ---

def get_enabled_assets() -> set[str]:
    raw = _state.get("enabled_assets", "") or ""
    return {a for a in raw.split(",") if a}


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def set_enabled_assets(assets: set[str]) -> None:
    set("enabled_assets", ",".join(sorted(assets)))


def toggle_asset(asset: str) -> bool:
    """Переключает состояние актива и возвращает новое (True = включён)."""
    asset = asset.lower()
    enabled = get_enabled_assets()
    if asset in enabled:
        enabled.discard(asset)
    else:
        enabled.add(asset)
    set_enabled_assets(enabled)
    return asset in enabled


# --- Размер ставки: fixed или % от текущего банка ---

def current_bankroll() -> float:
    """starting_bankroll_usdc + реализованная прибыль/убыток с начала.
    Считаем ТОЛЬКО реальные (не dry-run) сделки — иначе виртуальный PnL из
    периодов тестового прогона исказил бы размер реальных ставок (баг,
    найденный на реальных отчётах: банк считался завышенным на сумму
    прошлого dry-run PnL). Если сейчас DRY_RUN, наоборот, честнее было бы
    видеть, как рос бы виртуальный банк — но раз sizing реальных денег и
    dry-run использует один и тот же расчёт, отдаём предпочтение
    безопасности реальных ставок."""
    pnl = storage.get_pnl_summary(0, live_only=True)["pnl_usdc"]
    return get("starting_bankroll_usdc") + pnl


def compute_trade_size() -> float:
    """Базовый размер ставки ДО масштабирования по score (см.
    executor._scale_trade_size) — либо константа, либо доля от банка."""
    if get("sizing_mode") == "percent":
        bankroll = max(0.0, current_bankroll())
        return round(bankroll * get("bankroll_pct") / 100, 2)
    return get("trade_size_usdc")
