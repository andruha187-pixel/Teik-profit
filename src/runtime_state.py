"""
Настройки, которые меняются на лету из Telegram: общий режим DRY RUN / LIVE,
монеты (вкл/выкл, 🧪 тест / 🔴 реально) и правило входа ⏱ (окно, цена,
расстояние до страйка, ATR, ставка, дневной стоп).

Живут в памяти, каждое изменение сразу пишется в SQLite (`bot_settings`) и
переживает рестарт — если база на постоянном томе (/app/data).
"""
from __future__ import annotations

from config import settings
from src import storage


def _coins_csv(raw: str) -> str:
    return ",".join(sorted({a.strip().lower() for a in (raw or "").split(",") if a.strip()}))


def _bool(v) -> bool:
    return str(v).lower() == "true"


# Пределы, чтобы кнопки и ввод вручную не сделали правило бессмысленным
WINDOW_LIMITS = (5, 295)            # окно входа — секунд до конца 5-минутного окна
PRICE_LIMITS = (0.50, 0.99)
STAKE_LIMITS = (1.0, 1000.0)


def _sec(v) -> int:
    return int(min(WINDOW_LIMITS[1], max(WINDOW_LIMITS[0], int(float(v)))))


def _price(v) -> float:
    return round(min(PRICE_LIMITS[1], max(PRICE_LIMITS[0], float(v))), 2)


_DEFAULTS = {
    "dry_run": settings.DRY_RUN,
    "late_coins_on": _coins_csv(settings.LATE_COINS),
    "late_coins_live": _coins_csv(settings.LATE_LIVE_COINS),
    "late_enabled": settings.LATE_ENABLED,
    "late_from_sec": _sec(max(settings.LATE_FROM_SEC, settings.LATE_TO_SEC)),
    "late_to_sec": _sec(min(settings.LATE_FROM_SEC, settings.LATE_TO_SEC)),
    "late_min_price": _price(min(settings.LATE_MIN_PRICE, settings.LATE_MAX_PRICE)),
    "late_max_price": _price(max(settings.LATE_MIN_PRICE, settings.LATE_MAX_PRICE)),
    "late_min_dist": max(0.0, settings.LATE_MIN_DIST_PCT),
    "late_min_atr": max(0.0, settings.LATE_MIN_ATR),
    "late_stake": min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], settings.LATE_STAKE_USDC)),
    "late_stop": max(0.0, settings.LATE_STOP_USDC),
    # с какого момента действует нынешнее правило (итоги «по нынешнему правилу» — с него)
    "late_rule_since": 0,
}

_CASTERS = {
    "dry_run": _bool,
    "late_coins_on": str,
    "late_coins_live": str,
    "late_enabled": _bool,
    "late_from_sec": _sec,
    "late_to_sec": _sec,
    "late_min_price": _price,
    "late_max_price": _price,
    "late_min_dist": lambda v: max(0.0, float(v)),
    "late_min_atr": lambda v: max(0.0, float(v)),
    "late_stake": lambda v: min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], float(v))),
    "late_stop": lambda v: max(0.0, float(v)),
    "late_rule_since": lambda v: int(float(v)),
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db()."""
    for key, raw in storage.get_all_settings().items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass


def get(key: str):
    return _state.get(key)


def set(key: str, value) -> None:
    _state[key] = value
    storage.set_setting(key, value)


# --- Монеты: включена ли и реальные ли ставки ---

def _csv_set(key: str) -> set[str]:
    return {a for a in str(_state.get(key) or "").split(",") if a}


def get_enabled_assets() -> set[str]:
    return _csv_set("late_coins_on")


def is_asset_enabled(asset: str) -> bool:
    return asset.lower() in get_enabled_assets()


def toggle_asset(asset: str) -> bool:
    """Включить/выключить монету; возвращает новое состояние (True — включена)."""
    asset = asset.lower()
    on = get_enabled_assets()
    on.symmetric_difference_update({asset})
    set("late_coins_on", ",".join(sorted(on)))
    return asset in on


def get_live_assets() -> set[str]:
    return _csv_set("late_coins_live")


def is_asset_live(asset: str) -> bool:
    return asset.lower() in get_live_assets()


def set_asset_live(asset: str, live: bool) -> None:
    coins = get_live_assets()
    if live:
        coins.add(asset.lower())
    else:
        coins.discard(asset.lower())
    set("late_coins_live", ",".join(sorted(coins)))
