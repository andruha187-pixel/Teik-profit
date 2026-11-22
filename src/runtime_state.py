"""
Настройки, которые меняются на лету из Telegram: общий режим DRY RUN / LIVE,
монеты (вкл/выкл, 🧪 тест / 🔴 реально) и правило входа ⏱ (окно, цена,
расстояние до страйка, ATR, ставка, дневной стоп).

Живут в памяти, каждое изменение сразу пишется в SQLite (`bot_settings`) и
переживает рестарт — если база на постоянном томе (/app/data).

У монеты может быть своя сумма входа (💵: $ или % банка) и свой банк (🏦): сумма,
с которой монета стартовала, и момент, с которого считаем её реальные ставки.
И сторона реальных ставок (🎯): обе, только UP или только DOWN — тестовые идут по обеим.

➕ Докупка на откате: вкл/выкл, 🧪 тест / 🔴 реально (независимо от основной
стратегии), уровни цены и сумма на уровень — фиксированная $ или % от банка.

У монеты может быть своё правило входа (📏: до страйка в % и в ATR) и своя
докупка (вкл/выкл, 🧪/🔴, уровни, сумма) — чего не задано, берётся общее.
"""
from __future__ import annotations
import json
import re
import time

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
PCT_LIMITS = (0.1, 100.0)           # ставка монеты — % от банка
BANK_LIMITS = (0.0, 1_000_000.0)    # банк вручную (0 — не задан)


def _stakes_json(v) -> str:
    """Ставки монет: {"btc": {"mode": "usd", "value": 20}, "eth": {"mode": "pct", "value": 5}} — проверенный JSON."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {}
    for asset, cfg in (raw or {}).items():
        mode, value = str(cfg.get("mode")), float(cfg.get("value"))
        if mode == "usd":
            value = min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], value))
        elif mode == "pct":
            value = min(PCT_LIMITS[1], max(PCT_LIMITS[0], value))
        else:
            continue
        out[str(asset).lower()] = {"mode": mode, "value": round(value, 2)}
    return json.dumps(out, sort_keys=True)


def _banks_json(v) -> str:
    """Свои банки монет: {"btc": {"start": 100, "since": 1793600000}} — проверенный JSON (сумма > 0)."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {}
    for asset, cfg in (raw or {}).items():
        try:
            start, since = float(cfg.get("start")), int(float(cfg.get("since") or 0))
        except (TypeError, ValueError, AttributeError):
            continue
        if not start > 0:
            continue
        out[str(asset).lower()] = {"start": round(min(BANK_LIMITS[1], start), 2), "since": max(0, since)}
    return json.dumps(out, sort_keys=True)


SIDES = ("UP", "DOWN")


def _sides_json(v) -> str:
    """Сторона реальных ставок монет: {"btc": "UP"} — только эта сторона; нет записи — обе."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {str(a).lower(): str(s).upper() for a, s in (raw or {}).items() if str(s).upper() in SIDES}
    return json.dumps(out, sort_keys=True)


ADDON_USD_LIMITS = (0.1, 1000.0)    # ➕ докупка — $ на уровень
ADDON_MAX_LEVELS = 20
ADDON_DEFAULT_LEVELS = "0.95,0.94,0.93,0.92,0.91,0.90"


def _levels_csv(v) -> str:
    """Уровни докупки → «0.95,0.94,…»: от большего к меньшему, без повторов, в пределах цены (0.50–0.99),
    не больше ADDON_MAX_LEVELS. Ни одного годного — ValueError (остаётся прежнее значение)."""
    # (set() тут нельзя: ниже в модуле set — запись настройки)
    raw = v if isinstance(v, (list, tuple, frozenset)) or type(v).__name__ == "set" \
        else re.split(r"[,;\s]+", str(v or ""))
    out: dict[float, None] = {}
    for x in raw:
        try:
            f = float(str(x).strip())
        except ValueError:
            continue
        f = round(f / 100 if f > 1 else f, 2)
        if PRICE_LIMITS[0] - 1e-9 <= f <= PRICE_LIMITS[1] + 1e-9:
            out[f] = None
    if not out:
        raise ValueError("нет ни одного уровня 0.50–0.99")
    return ",".join(f"{x:.2f}" for x in sorted(out, reverse=True)[:ADDON_MAX_LEVELS])


def _addon_usd(v) -> float:
    return round(min(ADDON_USD_LIMITS[1], max(ADDON_USD_LIMITS[0], float(v))), 2)


ADDON_MODES = ("usd", "pct")       # на уровень: фиксированная $ / % от банка


def _addon_mode(v) -> str:
    v = str(v).strip().lower()
    if v not in ADDON_MODES:
        raise ValueError(f"режим суммы докупки: {v}")
    return v


def _pct(v) -> float:
    return round(min(PCT_LIMITS[1], max(PCT_LIMITS[0], float(v))), 2)


DIST_LIMITS = (0.0, 5.0)           # до страйка, % (0 — без условия)
ATR_LIMITS = (0.0, 20.0)           # до страйка, ATR (0 — без условия)


RULE_KEYS = ("dist", "atr", "dist_UP", "atr_UP", "dist_DOWN", "atr_DOWN")


def _rules_json(v) -> str:
    """Своё правило монеты: {"btc": {"dist": 0.12, "dist_DOWN": 0.15}, "sol": {"atr": 2.5}} — до страйка в % и в
    ATR: для обеих сторон (dist, atr) и отдельно для UP / DOWN (dist_UP, atr_DOWN, …); поля нет — дальше по цепочке:
    сторона → монета → общее из ⚙️."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {}
    for asset, cfg in (raw or {}).items():
        if not isinstance(cfg, dict):
            continue
        c = {}
        for key, (lo, hi) in ((k, DIST_LIMITS if k.startswith("dist") else ATR_LIMITS) for k in RULE_KEYS):
            if cfg.get(key) is None:
                continue
            try:
                c[key] = round(min(hi, max(lo, float(cfg[key]))), 4)
            except (TypeError, ValueError):
                continue
        if c:
            out[str(asset).lower()] = c
    return json.dumps(out, sort_keys=True)


def _coin_addons_json(v) -> str:
    """Своя докупка монеты: {"btc": {"on": false}, "sol": {"levels": "0.95,0.93", "mode": "pct", "pct": 1.0}} —
    вкл/выкл (on), 🔴 реально (live), уровни, сумма на уровень ($ — usd или % банка — pct, mode — что из них);
    поля нет — как в общей докупке."""
    raw = json.loads(v) if isinstance(v, str) else dict(v or {})
    out = {}
    for asset, cfg in (raw or {}).items():
        if not isinstance(cfg, dict):
            continue
        c = {}
        for key in ("on", "live"):
            if cfg.get(key) is not None:
                c[key] = cfg[key] if isinstance(cfg[key], bool) else _bool(cfg[key])
        if cfg.get("levels") is not None:
            try:
                c["levels"] = _levels_csv(cfg["levels"])
            except ValueError:
                pass
        if str(cfg.get("mode")) in ADDON_MODES:
            c["mode"] = str(cfg["mode"])
        for key, fn in (("usd", _addon_usd), ("pct", _pct)):
            if cfg.get(key) is not None:
                try:
                    c[key] = fn(cfg[key])
                except (TypeError, ValueError):
                    pass
        if c:
            out[str(asset).lower()] = c
    return json.dumps(out, sort_keys=True)


# 🎛 Пресет монет — по архиву 21.10–13.11 (вход 3:30–3:20 по 0.96–0.97): что у монет своё по умолчанию.
# Ставится один раз на версию пресета (late_preset_v): в новой базе — сразу, в работающей — только тем монетам
# и полям, где своё ещё не задано. Потом всё меняется кнопками и больше не трогается.
PRESET_VERSION = 1
PRESET_COIN_RULES = {"btc": {"dist_DOWN": 0.15}, "sol": {"atr": 2.5}, "xrp": {"dist": 0.12}}
PRESET_COIN_SIDES = {"doge": "UP", "bnb": "UP"}
PRESET_NOTE = {"text": ""}          # что поставили при этом старте — для сообщения о запуске


def _default_levels() -> str:
    try:
        return _levels_csv(settings.LATE_ADDON_LEVELS)
    except ValueError:
        return ADDON_DEFAULT_LEVELS


def _sec(v) -> int:
    return int(min(WINDOW_LIMITS[1], max(WINDOW_LIMITS[0], int(float(v)))))


def _price(v) -> float:
    return round(min(PRICE_LIMITS[1], max(PRICE_LIMITS[0], float(v))), 2)


WINDOW15_LIMITS = (5, 895)          # окно входа 15m — секунд до конца 15-минутного окна


def _sec15(v) -> int:
    return int(min(WINDOW15_LIMITS[1], max(WINDOW15_LIMITS[0], int(float(v)))))


def _stake(v) -> float:
    return round(min(STAKE_LIMITS[1], max(STAKE_LIMITS[0], float(v))), 2)


def _default_levels15() -> str:
    try:
        return _levels_csv(settings.LATE15_ADDON_LEVELS)
    except ValueError:
        return ADDON_DEFAULT_LEVELS


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
    # своя ставка у монеты: фиксированная $ или % от банка (нет записи — общая ставка late_stake)
    "late_coin_stakes": "{}",
    # банк для ставок в % — если баланс кошелька не читается (0 — не задан)
    "late_bank_manual": max(0.0, settings.LATE_BANK_USDC),
    # свой банк монеты: сумма и с какого момента считаем её реальные ставки (нет записи — общий банк)
    "late_coin_banks": "{}",
    # сторона реальных ставок монеты: "UP" / "DOWN" (нет записи — обе); тестовые — всегда обе
    "late_coin_sides": _sides_json(PRESET_COIN_SIDES) if settings.LATE_COIN_PRESET else "{}",
    # ➕ докупка на откате: вкл/выкл, 🔴 реально (иначе только тест), уровни цены, $ на уровень
    "late_addon_on": settings.LATE_ADDON_ON,
    "late_addon_live": settings.LATE_ADDON_LIVE,
    "late_addon_levels": _default_levels(),
    "late_addon_usd": _addon_usd(settings.LATE_ADDON_USDC),
    # сумма на уровень: "usd" — late_addon_usd, "pct" — late_addon_pct % от банка (как у ставки монеты)
    "late_addon_mode": "pct" if settings.LATE_ADDON_PCT > 0 else "usd",
    "late_addon_pct": _pct(settings.LATE_ADDON_PCT if settings.LATE_ADDON_PCT > 0 else 1.0),
    # своё правило монеты (до страйка % и ATR; можно отдельно для UP / DOWN) и своя докупка монеты — поля нет —
    # общее; по умолчанию — пресет монет (LATE_COIN_PRESET)
    "late_coin_rules": _rules_json(PRESET_COIN_RULES) if settings.LATE_COIN_PRESET else "{}",
    "late_coin_addons": "{}",
    # какая версия пресета монет уже поставлена (0 — никакая)
    "late_preset_v": 0,
    # ⏱15 запись окон 15m и ставки по ним: вкл/выкл, 🔴 реальные (иначе только тест), окно входа, сумма,
    # ➕ докупка 15m (вкл/выкл, уровни, $ на уровень; реальная — вместе с реальными ставками 15m)
    "late15_on": settings.LATE15_ON,
    "late15_live": settings.LATE15_LIVE,
    "late15_from_sec": _sec15(max(settings.LATE15_FROM_SEC, settings.LATE15_TO_SEC)),
    "late15_to_sec": _sec15(min(settings.LATE15_FROM_SEC, settings.LATE15_TO_SEC)),
    "late15_stake": _stake(settings.LATE15_STAKE_USDC),
    "late15_addon_on": settings.LATE15_ADDON_ON,
    "late15_addon_levels": _default_levels15(),
    "late15_addon_usd": _addon_usd(settings.LATE15_ADDON_USDC),
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
    "late_coin_stakes": _stakes_json,
    "late_bank_manual": lambda v: min(BANK_LIMITS[1], max(BANK_LIMITS[0], float(v))),
    "late_coin_banks": _banks_json,
    "late_coin_sides": _sides_json,
    "late_addon_on": _bool,
    "late_addon_live": _bool,
    "late_addon_levels": _levels_csv,
    "late_addon_usd": _addon_usd,
    "late_addon_mode": _addon_mode,
    "late_addon_pct": _pct,
    "late_coin_rules": _rules_json,
    "late_coin_addons": _coin_addons_json,
    "late_preset_v": lambda v: int(float(v)),
    "late15_on": _bool,
    "late15_live": _bool,
    "late15_from_sec": _sec15,
    "late15_to_sec": _sec15,
    "late15_stake": _stake,
    "late15_addon_on": _bool,
    "late15_addon_levels": _levels_csv,
    "late15_addon_usd": _addon_usd,
}

_state: dict = dict(_DEFAULTS)


def init_from_db() -> None:
    """Вызывать один раз при старте, после storage.init_db(). Заодно — пресет монет, если он ещё не стоял."""
    for key, raw in storage.get_all_settings().items():
        if key in _CASTERS:
            try:
                _state[key] = _CASTERS[key](raw)
            except (TypeError, ValueError):
                pass
    PRESET_NOTE["text"] = apply_coin_preset()


def apply_coin_preset() -> str:
    """🎛 Пресет монет (один раз на версию): своё правило и сторона — только там, где у монеты своё ещё не задано.
    Возвращает, что поставили («BTC ⬇️ DOWN ≥0.15%; …»), или "" — ничего (уже стоял / выключен)."""
    if not settings.LATE_COIN_PRESET or int(_state.get("late_preset_v") or 0) >= PRESET_VERSION:
        return ""
    rules = coin_rules()
    for asset, cfg in PRESET_COIN_RULES.items():
        cur = dict(rules.get(asset) or {})
        rules[asset] = {**cfg, **cur}                      # своё монеты важнее пресета
    set("late_coin_rules", _rules_json(rules))
    sides = coin_sides()
    for asset, side in PRESET_COIN_SIDES.items():
        sides.setdefault(asset, side)
    set("late_coin_sides", _sides_json(sides))
    set("late_preset_v", PRESET_VERSION)
    # что из пресета теперь действует (там, где у монеты было своё другое, — не пишем)
    done = []
    rules = coin_rules()
    for asset, cfg in PRESET_COIN_RULES.items():
        for k, v in cfg.items():
            if abs(float((rules.get(asset) or {}).get(k, -1)) - v) < 1e-9:
                side = {"_UP": "⬆️ UP ", "_DOWN": "⬇️ DOWN "}.get(k[k.find("_"):] if "_" in k else "", "")
                done.append(f"{asset.upper()} {side}" + (f"≥{v:g}%" if k.startswith("dist") else f"≥{v:g} ATR"))
    sides = coin_sides()
    done += [f"{a.upper()} реальные только {s}" for a, s in PRESET_COIN_SIDES.items() if sides.get(a) == s]
    return "; ".join(done)


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


# --- Своя ставка монеты: ("usd", $) или ("pct", % банка); None — общая ставка ---

def coin_stakes() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_stakes") or "{}"))
    except ValueError:
        return {}


def coin_stake(asset: str) -> tuple[str, float] | None:
    cfg = coin_stakes().get(asset.lower())
    return (cfg["mode"], float(cfg["value"])) if cfg else None


def set_coin_stake(asset: str, mode: str | None, value: float | None = None) -> None:
    """mode "usd" / "pct" — своя ставка; None — снова общая."""
    stakes = coin_stakes()
    if mode is None:
        stakes.pop(asset.lower(), None)
    else:
        stakes[asset.lower()] = {"mode": mode, "value": float(value)}
    set("late_coin_stakes", _stakes_json(stakes))


# --- Свой банк монеты: (сумма, с какого момента считаем её ставки); None — общий банк ---

def coin_banks() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_banks") or "{}"))
    except ValueError:
        return {}


def coin_bank(asset: str) -> tuple[float, int] | None:
    cfg = coin_banks().get(asset.lower())
    return (float(cfg["start"]), int(cfg["since"])) if cfg else None


def set_coin_bank(asset: str, start: float | None, since: int | None = None) -> None:
    """start > 0 — свой банк монеты с этой суммы, считаем с since (по умолчанию — сейчас); None/0 — убрать."""
    banks = coin_banks()
    if not start or start <= 0:
        banks.pop(asset.lower(), None)
    else:
        banks[asset.lower()] = {"start": float(start), "since": int(time.time() if since is None else since)}
    set("late_coin_banks", _banks_json(banks))


# --- Сторона реальных ставок монеты: "UP" / "DOWN"; None — обе (тестовые — всегда обе) ---

def coin_sides() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_sides") or "{}"))
    except ValueError:
        return {}


def coin_side(asset: str) -> str | None:
    return coin_sides().get(asset.lower())


def set_coin_side(asset: str, side: str | None) -> None:
    """side "UP" / "DOWN" — реальные ставки по монете только на эту сторону; None — обе."""
    sides = coin_sides()
    if side is None or str(side).upper() not in SIDES:
        sides.pop(asset.lower(), None)
    else:
        sides[asset.lower()] = str(side).upper()
    set("late_coin_sides", _sides_json(sides))


# --- ➕ Докупка на откате: уровни (от большего к меньшему) ---

def addon_levels() -> list[float]:
    return [float(x) for x in str(_state.get("late_addon_levels") or "").split(",") if x]


def set_addon_levels(levels) -> None:
    """Список цен (или строка «0.95,0.94»); ValueError — ни одного годного уровня."""
    set("late_addon_levels", _levels_csv(levels))


# --- 📏 Своё правило монеты: до страйка в % (dist) и в ATR (atr); поля нет — общее ---

def coin_rules() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_rules") or "{}"))
    except ValueError:
        return {}


def coin_rule(asset: str) -> dict:
    return dict(coin_rules().get(asset.lower()) or {})


def set_coin_rule(asset: str, key: str, value: float | None) -> None:
    """key "dist" / "atr" (обе стороны) или "dist_UP" / "atr_DOWN" … (одна сторона); value None — снова по цепочке
    (сторона → монета → общее)."""
    rules = coin_rules()
    cfg = dict(rules.get(asset.lower()) or {})
    if value is None:
        cfg.pop(key, None)
    else:
        cfg[key] = float(value)
    if cfg:
        rules[asset.lower()] = cfg
    else:
        rules.pop(asset.lower(), None)
    set("late_coin_rules", _rules_json(rules))


# --- ➕ Своя докупка монеты: on / live / levels / mode / usd / pct; поля нет — как в общей ---

def coin_addons() -> dict:
    try:
        return json.loads(str(_state.get("late_coin_addons") or "{}"))
    except ValueError:
        return {}


def coin_addon(asset: str) -> dict:
    return dict(coin_addons().get(asset.lower()) or {})


def set_coin_addon(asset: str, **fields) -> None:
    """Поля своей докупки монеты: on=, live=, levels= (список или «0.95,0.94»), mode= ("usd"/"pct"), usd=, pct=.
    Значение None — поле снова общее. Уровни без единого годного — ValueError."""
    addons = coin_addons()
    cfg = dict(addons.get(asset.lower()) or {})
    for key, value in fields.items():
        if value is None:
            cfg.pop(key, None)
        elif key == "levels":
            cfg[key] = _levels_csv(value)
        else:
            cfg[key] = value
    if cfg:
        addons[asset.lower()] = cfg
    else:
        addons.pop(asset.lower(), None)
    set("late_coin_addons", _coin_addons_json(addons))


def clear_coin_addon(asset: str) -> None:
    addons = coin_addons()
    addons.pop(asset.lower(), None)
    set("late_coin_addons", _coin_addons_json(addons))
