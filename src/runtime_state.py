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
    "size_scaling_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_enabled": lambda v: str(v).lower() == "true",
    "position_stop_loss_pct": float,
    "enabled_assets": str,
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
