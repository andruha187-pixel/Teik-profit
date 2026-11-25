"""
⏱ Поздний вход на 5-минутных рынках Up/Down Polymarket.

Правило. За ~3 минуты до конца окна (по умолчанию с 3:10 до 3:00 до конца)
покупаем сторону, которая уже далеко ушла от страйка:
  - цена стороны (ask) 0.92–0.95;
  - цена Chainlink ушла от страйка (цены Chainlink на старте окна) не меньше
    чем на 0.10% и не меньше чем на 2 ATR (Wilder ATR(7) по 1m-свечам Binance).
Держим до конца окна — стоп-лосс и досрочная продажа на данных 25.09–20.10
только ухудшали итог. Всё перечисленное меняется кнопками в Telegram.

Откуда правило. На ~15 тыс. 5m-рынков (25.09–20.10) вход с этими условиями в
окне 3:10–3:00 до конца — 232 ставки, 98.3% выигрышей, около +3% на ставку;
соседние окна (2:50–3:10, 3:00–3:20) — 96.7–97.7%. При цене ~0.94 окупаемость —
от ~95% выигрышей, поэтому сначала — DRY.

Ставки — по включённым монетам (🪙): тестовая всегда, реальная — если монета 🔴
и бот в 🔴 LIVE, есть ключ и не достигнут дневной стоп. Одна ставка на окно.
У монеты можно разрешить реальные ставки только на одну сторону (🎯 только UP /
только DOWN): тестовая всё равно ставится по обеим — видно, как сыграла бы
выключенная сторона (в 📊 и в отчёте — итоги теста по сторонам).
Сумма — общая (⚙️) или своя у монеты (🪙 💵): фиксированная $ или % от банка.
Банк — свой у монеты (🏦: заданная сумма + результат её реальных ставок с этого
момента; ставка не больше его свободных денег, кончились — реальные по монете
не идут, тест идёт), а если своего нет — общий: баланс кошелька (не читается —
банк, заданный вручную).

Реальный ордер (FAK, не дороже ask + 1¢) не исполнился или исполнился частично —
бот пробует снова каждые LIVE_RETRY_SEC, пока идёт окно входа и условия
выполняются (цена в диапазоне, та же сторона), и добирает до суммы ставки. В
Telegram — только итог («✅ ставка …»); окно без покупки пишется в ставки со
статусом SKIP_NOFILL и причиной (note). Об ошибках, которые повтором не лечатся
(баланс, ключ), — сообщение, но не чаще раза в час.

➕ Докупка на откате (вкл/выкл кнопкой): после основной ставки бот следит за
ask нашей стороны до ADDON_MIN_LEFT_SEC до конца; опустился до уровня (по
умолчанию 0.95, 0.94 … 0.90; уровни не ниже цены входа пропускаются) — покупка
не дороже уровня, каждый уровень — раз за окно, сумма на уровень — фиксированная $
или % от банка (как у ставки монеты: свой банк 🏦, а нет его — общий). Режим свой:
🧪 тест — по стакану, как тестовые ставки (идёт всегда, пока докупка включена);
🔴 реально — ещё и FAK-ордер, если по окну есть реальная основная ставка, монета
🔴 и бот в LIVE (не меньше 5 акций). В итоге реальной ставки в Telegram — и
«с тестовой докупкой было бы …» (или итог с реальной докупкой).

🔁 Запись: по каждому окну каждой монеты (и выключенной тоже) — страйк и путь
[секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR, bid UP, bid DOWN,
объём ask UP, объём ask DOWN, объём bid UP, объём bid DOWN] каждые REC_STEP_SEC
секунд с 4:30 до конца; исход — ранний (стакан / Chainlink) и официальный. По
этим файлам подбираются окно входа, пороги и монеты; бид и объёмы нужны, чтобы
честно проверить мейкерский вход (исполнилась бы лимитка на стороне-лидере).
"""
from __future__ import annotations
import asyncio
import json
import logging
import math
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from config import settings
from src import atr_feed, book_stream, chainlink_feed, market_discovery, runtime_state, storage

log = logging.getLogger("late_bot")

STEP = 300
LABEL = "5m"
PREFETCH_SEC = 60              # рынок следующего окна ищем за минуту до начала…
PREFETCH_RETRY_SEC = 10        # …и снова каждые 10 с, если его ещё нет
DISCOVERY_RETRY_SEC = 5        # рынок текущего окна не нашёлся — снова через 5 с
LATE_RETRY_SEC = 30            # …после первой минуты окна — через 30 с
BOOK_WINDOW_SEC = 12.0         # исход по стакану: читаем в последние 12 с…
BOOK_LAST_SEC = 3.0            # …и верим последнему чтению, если оно не раньше чем за 3 с до конца
BOOK_LO, BOOK_HI = 0.03, 0.97
BOOK_MAX_AGE_MS = 30_000       # стакан без обновлений дольше — не входим (поток мог зависнуть)
EARLY_WAIT_SEC = 30            # ждём точку Chainlink после конца окна не дольше
STRIKE_WAIT_SEC = 10           # точки Chainlink с начала окна нет столько — берём ближайшую до него
SETTLE_EVERY_SEC = 60
SETTLE_LAG_SEC = 90            # официальный исход спрашиваем не раньше чем через 1.5 мин после конца
SETTLE_MAX_AGE_SEC = 2 * 86400
FLUSH_EVERY_SEC = 30
TRIM_EVERY_SEC = 3600
UNSUPPORTED_AFTER = 3          # рынка нет 3 окна подряд (с запуска) — монету пропускаем…
UNSUPPORTED_SKIP_SEC = 6 * 3600  # …на 6 часов
REQUEST_PAUSE_SEC = 0.2

MIN_ORDER_SHARES = 5.0         # меньше 5 акций биржа ордер не берёт
PRICE_SLIP = 0.01              # потолок ордера — ask + 1¢ (но не выше верхней границы цены + 1¢)
LIVE_RETRY_SEC = 0.5           # реальный ордер не исполнился — снова через полсекунды, пока идёт окно входа
LIVE_MAX_TRIES = 30            # проверок и ордеров на окно — не больше (окно входа ~10 с)
LIVE_MAX_ERRORS = 3            # непонятных ошибок подряд — хватит
ERROR_NOTE_EVERY_SEC = 3600    # об одной и той же ошибке ордера — не чаще раза в час
BANK_REFRESH_SEC = 60          # баланс кошелька (для ставок в % банка) — раз в минуту
MIN_TRADE_USDC = 1.0

ADDON_MIN_LEFT_SEC = 10.0      # ➕ докупка — пока до конца окна не меньше 10 с
ADDON_LIVE_WAIT_SEC = 10.0     # реальная докупка не исполнилась (цену перебили) — ждём возврата к уровню до 10 с…
ADDON_LIVE_TRIES = 3           # …и не больше 3 ордеров на уровень
MIN_DRY_ADDON_USDC = 0.05      # тестовая докупка меньше — не в счёт (объём на уровне съели уровни выше)

# Кнопки ⚙️ Настроек (окно — секунд до конца: (от, до)); любое другое значение — ✏️ сообщением
WINDOW_PRESETS = ((210, 200), (220, 200), (190, 180))
PRICE_PRESETS = ((0.96, 0.97), (0.95, 0.97), (0.92, 0.95))
DIST_PRESETS = (0.05, 0.08, 0.10, 0.15)
ATR_PRESETS = (1.5, 2.0, 2.5, 0.0)
COIN_DIST_PRESETS = (0.12, 0.15, 0.20)        # 📏 своё у монеты (и для стороны)
COIN_ATR_PRESETS = (2.5, 3.0)
SIDE_ATR_PRESETS = (2.5, 3.0, 4.0)            # 📏 своё для стороны UP / DOWN
STAKE_PRESETS = (5.0, 10.0, 20.0, 50.0)
PCT_PRESETS = (1.0, 2.0, 5.0, 10.0)
STOP_PRESETS = (0.0, 30.0, 60.0, 150.0)
ADDON_USD_PRESETS = (0.5, 1.0, 2.0, 5.0)
ADDON_PCT_PRESETS = (0.5, 1.0, 2.0, 5.0)
ADDON_LEVEL_PRESETS = ((0.95, 0.94, 0.93, 0.92, 0.91, 0.90), (0.95, 0.93, 0.91), (0.93, 0.90))


@dataclass
class Win:
    asset: str
    start: int
    end: int
    slug: str
    up: str
    down: str
    own_tokens: bool = False
    strike: float | None = None
    path: list = field(default_factory=list)
    last_rec_t: float = 1e9            # секунд до конца у последней точки пути
    book_res: str | None = None
    book_res_t: float = 0.0
    early_set: bool = False
    last_flush: float = 0.0
    decided: bool = False              # ставка сделана или окно входа прошло
    checked: bool = False              # в базе проверили, нет ли уже ставки по окну (рестарт)
    why: str = ""                      # последняя причина «не входим» в окне входа
    addon: dict | None = None          # ➕ докупка окна: сторона, токен, цена входа, уровни, что ещё ждёт отката


_cur: dict[str, Win] = {}
_next: dict[str, Win] = {}
_prefetch_at: dict[tuple[str, int], float] = {}
_prefetching: set = set()
_opening: set = set()
_pending: list[Win] = []
_retry_at: dict[tuple[str, int], float] = {}
_supported: set = set()
_fails: dict[str, int] = defaultdict(int)
_skip_until: dict[str, float] = {}
_last_start: dict[str, int] = {}
_tasks: set = set()
_trade_note = {"stop_day": None}
_err_notes: dict[str, float] = {}      # текст ошибки ордера -> когда последний раз сообщали
_bank = {"wallet": None, "ts": 0.0, "error": ""}   # баланс кошелька USDC (для ставок в % банка)
_why = defaultdict(Counter)            # монета -> {причина: окон}
_addon_note = {"why": "", "ts": 0.0}   # почему последняя реальная докупка не пошла (для экрана ➕)
_stats = {"windows": 0, "striked": 0, "early": 0, "official": 0, "discovery_errors": 0, "last_error": ""}


def reset() -> None:
    for d in (_cur, _next, _prefetch_at, _retry_at, _fails, _skip_until, _last_start, _why):
        d.clear()
    _addon_note.update(why="", ts=0.0)
    _prefetching.clear()
    _opening.clear()
    _pending.clear()
    _supported.clear()
    _trade_note["stop_day"] = None
    _err_notes.clear()
    _bank.update(wallet=None, ts=0.0, error="")
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0


# ---------------------------------------------------------- настройки ----

def _rs(key: str):
    return runtime_state.get(key)


def enabled() -> bool:
    return bool(_rs("late_enabled"))


def window() -> tuple[int, int]:
    """(от, до) — секунд до конца окна: вход, пока от ≥ осталось ≥ до. Одно время
    («ровно 3:00») — это секунда перед ним: бот проверяет раз в секунду."""
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    hi, lo = max(a, b), min(a, b)
    return max(hi, lo + 1), lo


def price_range() -> tuple[float, float]:
    a, b = float(_rs("late_min_price")), float(_rs("late_max_price"))
    return min(a, b), max(a, b)


def _rule_value(key: str, asset: str | None, side: str | None) -> float:
    """Порог по цепочке: своё для стороны монеты (dist_DOWN) → своё у монеты (dist) → общее из ⚙️."""
    own = runtime_state.coin_rule(asset) if asset else {}
    for k in ((f"{key}_{side}",) if side else ()) + (key,):
        if own.get(k) is not None:
            return float(own[k])
    return float(_rs("late_min_dist" if key == "dist" else "late_min_atr"))


def min_dist(asset: str | None = None, side: str | None = None) -> float:
    """До страйка, %: своё для стороны монеты → своё у монеты (📏) → общее из ⚙️."""
    return _rule_value("dist", asset, side)


def min_atr(asset: str | None = None, side: str | None = None) -> float:
    """До страйка, ATR: своё для стороны монеты → своё у монеты (📏) → общее из ⚙️."""
    return _rule_value("atr", asset, side)


def coin_rule_own(asset: str) -> dict:
    """Что у монеты своё в правиле: {"dist": 0.12, "dist_DOWN": 0.15} / {}."""
    return runtime_state.coin_rule(asset)


def _thr(d: float, k: float) -> str:
    return (f"≥{d:g}%" if d > 0 else "без %") + " и " + (f"≥{k:g} ATR" if k > 0 else "без ATR")


def coin_rule_text(asset: str) -> str:
    """«до страйка ≥0.12% (своё) и ≥2 ATR (общее); ⬇️ DOWN: ≥0.15% и ≥2 ATR»."""
    own = coin_rule_own(asset)
    d, k = min_dist(asset), min_atr(asset)
    dist = (f"≥{d:g}%" if d > 0 else "без условия по %") + (" (своё)" if "dist" in own else " (общее)")
    atr = (f"≥{k:g} ATR" if k > 0 else "без ATR") + (" (своё)" if "atr" in own else " (общее)")
    s = f"до страйка {dist} и {atr}"
    for side, mark in (("UP", "⬆️"), ("DOWN", "⬇️")):
        if f"dist_{side}" in own or f"atr_{side}" in own:
            s += f"; {mark} {side}: {_thr(min_dist(asset, side), min_atr(asset, side))}"
    return s


def coin_rule_short(asset: str) -> str:
    """Своё у монеты коротко: «≥0.12%», «⬇️≥0.15%», «≥2.5 ATR · ⬆️≥0.2%» ("" — всё общее)."""
    own = coin_rule_own(asset)
    bits = []
    for suffix, mark in (("", ""), ("_UP", "⬆️"), ("_DOWN", "⬇️")):
        part = ([f"≥{own['dist' + suffix]:g}%"] if f"dist{suffix}" in own else []) + \
               ([f"≥{own['atr' + suffix]:g} ATR"] if f"atr{suffix}" in own else [])
        if part:
            bits.append(mark + " и ".join(part))
    return " · ".join(bits)


def coin_rules_text() -> str:
    """У каких монет своё правило: «BTC ⬇️≥0.15%; SOL ≥2.5 ATR» ("" — ни у каких)."""
    return "; ".join(f"{a.upper()} {coin_rule_short(a)}" for a in settings.ASSETS if coin_rule_own(a))


def stake() -> float:
    return float(_rs("late_stake"))


def stop() -> float:
    return float(_rs("late_stop"))


def rule_since() -> int:
    return int(_rs("late_rule_since") or 0)


def mark_rule_change(now: float | None = None) -> None:
    runtime_state.set("late_rule_since", int(time.time() if now is None else now))


def mmss(sec: float) -> str:
    sec = int(round(sec))
    return f"{sec // 60}:{sec % 60:02d}"


def plural(n: int, one: str, few: str, many: str) -> str:
    """1 ставка, 2 ставки, 5 ставок."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def bets_text(n: int) -> str:
    return f"{n} {plural(n, 'ставка', 'ставки', 'ставок')}"


def window_phrase() -> str:
    """«за 3:00–3:10 до конца» или «ровно за 3:00 до конца»."""
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    return f"ровно за {mmss(a)} до конца" if a == b else f"за {window_text()} до конца"


def window_text() -> str:
    a, b = int(_rs("late_from_sec")), int(_rs("late_to_sec"))
    if a == b:
        return f"ровно {mmss(a)}"
    hi, lo = window()
    return f"{mmss(lo)}–{mmss(hi)}"


def rule_tag(asset: str | None = None, side: str | None = None) -> str:
    """Правило коротко — пишется в каждую ставку (колонка rule); у монеты и стороны — с их порогами."""
    hi, lo = window()
    pl, ph = price_range()
    tag = f"{hi}-{lo}s|{pl:.2f}-{ph:.2f}|d{min_dist(asset, side):g}%|atr{min_atr(asset, side):g}"
    if hot_atr_x() > 0 or hot_move_pct() > 0:
        tag += f"|hot{hot_atr_x():g}/{hot_move_pct():g}"
    return tag


# --- 🌪 резкий рынок ---
HOT_ATR_BACK_SEC = 870         # «ATR 15 минут назад» — на момент за 14:30 до начала окна (как в разборе архива)
HOT_MOVE_BACK_SEC = 600        # «ход за 10 минут» — от цены Chainlink на начало окна два окна назад


def hot_atr_x() -> float:
    return float(_rs("late_hot_atr_x") or 0.0)


def hot_move_pct() -> float:
    return float(_rs("late_hot_move_pct") or 0.0)


def hot_text() -> str:
    """«не ставит, если ATR ×≥1.7 за 15 мин или за 10 мин уже ≥0.45% в сторону ставки» / «выключен»."""
    parts = []
    if hot_atr_x() > 0:
        parts.append(f"ATR вырос ×≥{hot_atr_x():g} за 15 мин")
    if hot_move_pct() > 0:
        parts.append(f"за 10 мин уже ≥{hot_move_pct():g}% в сторону ставки")
    return ("не ставит, если " + " или ".join(parts)) if parts else "выключен"


def hot_why(asset: str, start: int, price: float, side: str, atr: float | None) -> str | None:
    """Причина не ставить на резком рынке или None. Нет истории (бот только что запущен) — не мешаем."""
    x = hot_atr_x()
    if x > 0 and atr:
        a0 = atr_feed.get_at(asset, start - HOT_ATR_BACK_SEC)
        if a0 and atr / a0 >= x - 1e-9:
            return f"резкий рынок: ATR ×{atr / a0:.1f} за 15 мин"
    m = hot_move_pct()
    if m > 0:
        p0 = chainlink_feed.price_at(asset, start - HOT_MOVE_BACK_SEC)
        if p0:
            mv = (price - p0) / p0 * 100 * (1 if side == "UP" else -1)
            if mv >= m - 1e-12:
                return f"резкий рынок: за 10 мин уже {mv:.2f}% в сторону ставки"
    return None


def atr_text() -> str:
    return f"≥{min_atr():g} ATR" if min_atr() > 0 else "без ATR"


def rule_short() -> str:
    """Правило в одну строку: «за 3:00–3:10 до конца, цена 0.92–0.95, до страйка ≥0.1% и ≥2 ATR»."""
    pl, ph = price_range()
    atr = f" и {atr_text()}" if min_atr() > 0 else " (без ATR)"
    own = coin_rules_text()
    hot = f"; 🌪 {hot_text()}" if hot_atr_x() > 0 or hot_move_pct() > 0 else ""
    return (f"{window_phrase()}, цена {pl:.2f}–{ph:.2f}, до страйка ≥{min_dist():g}%{atr}"
            + (f" (своё у {own})" if own else "") + hot)


def rule_text() -> str:
    pl, ph = price_range()
    atr = f", не меньше {min_atr():g} ATR" if min_atr() > 0 else ", без условия по ATR"
    own = coin_rules_text()
    hot = f"; 🌪 резкий рынок — {hot_text()}" if hot_atr_x() > 0 or hot_move_pct() > 0 else ""
    return (f"{window_phrase()} окна сторона по цене {pl:.2f}–{ph:.2f}, "
            f"цена Chainlink ушла от страйка не меньше чем на {min_dist():g}%{atr}"
            + (f"; своё у монет: {own}" if own else "") + hot)


def trade_assets() -> list[str]:
    return [a for a in settings.ASSETS if runtime_state.is_asset_enabled(a)]


def coin_live(asset: str) -> bool:
    return runtime_state.is_asset_live(asset)


def live_coins() -> list[str]:
    return [a for a in trade_assets() if coin_live(a)]


def side_allowed(asset: str, side: str) -> bool:
    """Реальная ставка по этой стороне разрешена (🎯)? Тестовые идут по обеим всегда."""
    only = runtime_state.coin_side(asset)
    return only is None or only == side


def side_mark(asset: str) -> str:
    return {"UP": "⬆️", "DOWN": "⬇️"}.get(runtime_state.coin_side(asset) or "", "")


def side_label(asset: str) -> str:
    return {"UP": "только UP", "DOWN": "только DOWN"}.get(runtime_state.coin_side(asset) or "", "обе стороны")


def coin_name(asset: str) -> str:
    """«BTC» или «BTC⬆️» — если реальные только на одну сторону."""
    return asset.upper() + side_mark(asset)


# ---------------------------------------------------- ➕ докупка: настройки ----

def _coin_ad(asset: str | None) -> dict:
    """Своя докупка монеты (что задано у неё самой); {} — всё как в общей."""
    return runtime_state.coin_addon(asset) if asset else {}


def addon_custom(asset: str) -> bool:
    return bool(_coin_ad(asset))


def addon_on(asset: str | None = None) -> bool:
    """Докупка включена: у монеты — своя настройка, если задана, иначе общая."""
    v = _coin_ad(asset).get("on")
    return bool(_rs("late_addon_on")) if v is None else bool(v)


def addon_live(asset: str | None = None) -> bool:
    """🔴 реальная докупка (иначе только тест). Реальный ордер — ещё и при монете 🔴 и боте в LIVE."""
    v = _coin_ad(asset).get("live")
    return bool(_rs("late_addon_live")) if v is None else bool(v)


def addon_levels(asset: str | None = None) -> list[float]:
    v = _coin_ad(asset).get("levels")
    return [float(x) for x in str(v).split(",") if x] if v else runtime_state.addon_levels()


def addon_usd(asset: str | None = None) -> float:
    """Фиксированная сумма на уровень (и запасная для % банка, если банк неизвестен)."""
    v = _coin_ad(asset).get("usd")
    return float(_rs("late_addon_usd") if v is None else v)


def addon_amount_mode(asset: str | None = None) -> str:
    """Сумма на уровень: "usd" — фиксированная $, "pct" — % от банка."""
    v = _coin_ad(asset).get("mode") or _rs("late_addon_mode")
    return "pct" if v == "pct" else "usd"


def addon_pct(asset: str | None = None) -> float:
    v = _coin_ad(asset).get("pct")
    return float(_rs("late_addon_pct") if v is None else v)


def addon_usd_for(asset: str) -> float:
    """$ на уровень у монеты (по её докупке): фиксированная или % банка — своего банка монеты (🏦: заданная сумма
    + итог её реальных ставок), а нет его — общего (баланс кошелька, не читается — заданный вручную); банк
    неизвестен — фиксированная $. Так же, как % у ставки монеты."""
    if addon_amount_mode(asset) != "pct":
        return addon_usd(asset)
    own = coin_bank(asset)
    if own is not None:
        b = max(0.0, own["now"])
    else:
        b, _src = bank()
        if b is None:
            return addon_usd(asset)
    return round(min(runtime_state.ADDON_USD_LIMITS[1], b * addon_pct(asset) / 100.0), 2)


def addon_live_usd(level: float, asset: str | None = None) -> float:
    """Реальная докупка на уровне: сумма на уровень (у монеты — с учётом % банка), но не меньше 5 акций по цене
    уровня (меньше биржа не берёт)."""
    base = addon_usd() if asset is None else addon_usd_for(asset)
    return max(base, round(MIN_ORDER_SHARES * level + 0.01, 2))


def addon_amount_text(asset: str | None = None) -> str:
    """«$1» или «1% банка» — для экранов."""
    return f"{addon_pct(asset):g}% банка" if addon_amount_mode(asset) == "pct" else f"${addon_usd(asset):g}"


def addon_amount_note(asset: str) -> str:
    """Сколько сейчас на уровень у монеты: «$1», «~$1.00 (банк BTC $100.00)», «~$2.10 (общий банк $210.00)»
    или «$1 (банк неизвестен)»."""
    if addon_amount_mode(asset) != "pct":
        return f"${addon_usd(asset):g}"
    own = coin_bank(asset)
    if own is not None:
        return f"~${addon_usd_for(asset):.2f} (банк {asset.upper()} ${own['now']:.2f})"
    b, _src = bank()
    if b is None:
        return f"${addon_usd(asset):g} (банк неизвестен)"
    return f"~${addon_usd_for(asset):.2f} (общий банк ${b:.2f})"


def addon_amounts_text() -> str:
    """По включённым монетам: «BTC ~$1.00 (банк BTC $100.00), ETH ~$2.10 (общий банк $210.00)»."""
    coins = [a for a in trade_assets() if addon_on(a)]
    return ", ".join(f"{a.upper()} {addon_amount_note(a)}" for a in coins) if coins else "монет с докупкой нет"


def levels_text(levels: list[float] | None = None, asset: str | None = None) -> str:
    """«0.95–0.90 через 1¢» (подряд по центу, от 3 уровней) или «0.95, 0.93, 0.91»."""
    lv = sorted(addon_levels(asset) if levels is None else levels, reverse=True)
    if not lv:
        return "нет уровней"
    if len(lv) >= 3 and all(abs(a - b - 0.01) < 1e-6 for a, b in zip(lv, lv[1:])):
        return f"{lv[0]:.2f}–{lv[-1]:.2f} через 1¢"
    return ", ".join(f"{x:.2f}" for x in lv)


def addon_mode_text(asset: str | None = None) -> str:
    return "🔴 реально" if addon_live(asset) else "🧪 тест"


def addon_short(asset: str | None = None) -> str:
    """«✅ 🧪 тест · 0.95–0.90 через 1¢ · по $1 на уровень» или «⏸ выключена» (у монеты — с её настройкой)."""
    if not addon_on(asset):
        return "⏸ выключена"
    return (f"✅ {addon_mode_text(asset)} · {levels_text(asset=asset)} · по {addon_amount_text(asset)} "
            "на уровень")


def coin_addons_text() -> str:
    """У каких монет своя докупка: «BTC ⏸ выключена; SOL ✅ 🧪 тест · 0.95, 0.93 · по $2 на уровень»."""
    return "; ".join(f"{a.upper()} {addon_short(a)}" for a in settings.ASSETS if addon_custom(a))


def any_addon_on() -> bool:
    """Докупка включена хоть у одной монеты (общая или своя)."""
    return addon_on() or any(addon_on(a) for a in settings.ASSETS)


# ------------------------------------------------- сумма ставки и банк ----

async def refresh_bank(force: bool = False) -> float | None:
    """Баланс кошелька USDC — не чаще раза в BANK_REFRESH_SEC (force — сразу, например после ставки)."""
    if not settings.POLY_PRIVATE_KEY:
        return None
    now = time.time()
    if not force and now - _bank["ts"] < BANK_REFRESH_SEC:
        return _bank["wallet"]
    _bank["ts"] = now
    from src import polymarket_client
    try:
        val = await polymarket_client.get_usdc_balance()
    except Exception as exc:  # noqa: BLE001
        _bank["error"] = str(exc)[:150]
        return _bank["wallet"]
    if val is not None:
        _bank.update(wallet=val, error="")
    elif not _bank["error"]:
        _bank["error"] = "SDK не отдал баланс"
    return _bank["wallet"]


def bank() -> tuple[float | None, str]:
    """(банк для ставок в %, откуда он): баланс кошелька, иначе заданный вручную; (None, "") — неизвестен."""
    if _bank["wallet"] is not None:
        return float(_bank["wallet"]), "баланс кошелька"
    manual = float(_rs("late_bank_manual") or 0.0)
    if manual > 0:
        return manual, "задан вручную"
    return None, ""


def coin_bank(asset: str) -> dict | None:
    """Свой банк монеты (🏦): start — заданная сумма, since — с какого момента считаем, pnl — закрытый
    результат её реальных ставок с тех пор, now = start + pnl, open — в открытых реальных ставках (с
    комиссией), free = now − open. None — своего банка нет (ставки в % — от общего банка)."""
    cfg = runtime_state.coin_bank(asset)
    if cfg is None:
        return None
    start, since = cfg
    pnl, open_ = storage.coin_bank_flow(asset, since)
    now = start + pnl
    return {"start": start, "since": since, "pnl": pnl, "now": now, "open": open_, "free": now - open_}


def min_order_usdc() -> float:
    """Минимальный ордер биржи (5 акций) по потолку цены — ставка меньше не уйдёт."""
    return round(MIN_ORDER_SHARES * (price_range()[1] + PRICE_SLIP) + 0.01, 2)


def stake_rule(asset: str) -> tuple[str, float]:
    """Как считается ставка монеты: ("общая", $), ("usd", $) или ("pct", %)."""
    return runtime_state.coin_stake(asset) or ("общая", stake())


def stake_for(asset: str | None) -> float:
    """Сумма ставки монеты до правила «не меньше 5 акций»: общая, своя фиксированная или % банка.
    % — от своего банка монеты (🏦), а нет его — от общего (неизвестен — общая сумма).
    Со своим банком ставка не больше его свободных денег."""
    if asset is None:
        return stake()
    mode, value = stake_rule(asset)
    own = coin_bank(asset)
    if mode == "pct":
        if own is not None:
            amt = max(0.0, own["now"]) * value / 100.0
        else:
            b, _src = bank()
            if b is None:
                return stake()
            amt = b * value / 100.0
        amt = min(runtime_state.STAKE_LIMITS[1], amt)
    else:
        amt = value
    if own is not None:
        amt = min(amt, max(0.0, own["free"]))
    return round(amt, 2)


def stake_label(asset: str) -> str:
    """«$20», «5% банка (~$10.67)», «10% банка BTC (~$11.24)», «общая $5»."""
    mode, value = stake_rule(asset)
    own = coin_bank(asset)
    if mode == "pct":
        if own is not None:
            return f"{value:g}% банка {asset.upper()} (~${stake_for(asset):.2f})"
        b, _src = bank()
        return f"{value:g}% банка" + (f" (~${b * value / 100:.2f})" if b is not None else f" (банк неизвестен — общая ${stake():g})")
    s = f"${value:g}" if mode == "usd" else f"общая ${value:g}"
    if own is not None and own["free"] < value - 0.005:
        s += f" (в банке {asset.upper()} свободно ${max(0.0, own['free']):.2f})"
    return s


def stake_button(asset: str) -> str:
    mode, value = stake_rule(asset)
    s = {"usd": f"${value:g}", "pct": f"{value:g}%"}.get(mode, "общая")
    return s + (" 🏦" if runtime_state.coin_bank(asset) else "")


def live_target(asset: str, usdc: float) -> tuple[float, str]:
    """Сколько реально ставить: сумма ставки, но не больше баланса кошелька и свободных денег банка монеты.
    (сумма, что её урезало: "" — ничего, "wallet" — кошелёк, "coin" — банк монеты)."""
    target, why = usdc, ""
    wallet = _bank["wallet"]
    if wallet is not None and target > wallet:
        target, why = round(float(wallet) - 0.01, 2), "wallet"          # на балансе меньше — на сколько есть
    own = coin_bank(asset)
    if own is not None and target > own["free"]:
        target, why = math.floor(max(0.0, own["free"]) * 100) / 100, "coin"
    return target, why


def bet_usdc(cap: float, asset: str | None = None) -> float:
    """Сумма ставки: общая или своя у монеты, но не меньше 5 акций по потолку цены (иначе биржа отклонит)."""
    return max(stake_for(asset), round(MIN_ORDER_SHARES * cap + 0.01, 2))


def fee_for(usdc: float, price: float) -> float:
    return round(usdc * float(settings.FEE_RATE) * (1 - price), 4)


# --------------------------------------------------------------- окна ----

def slug_for(asset: str, start: int) -> str:
    return f"{asset}-updown-{LABEL}-{start}"


def book_winner(up: str, down: str) -> str | None:
    ua, da = book_stream.best_ask(up), book_stream.best_ask(down)
    if da is not None and da <= BOOK_LO and (ua is None or ua >= BOOK_HI):
        return "UP"
    if ua is not None and ua <= BOOK_LO and (da is None or da >= BOOK_HI):
        return "DOWN"
    return None


def _task_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.warning("late_bot: фоновая задача упала: %s", task.exception())


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_task_done)


async def drain() -> None:
    """Дождаться фоновых задач (реальные ордера, поиск рынков следующего окна)."""
    while _tasks:
        await asyncio.gather(*list(_tasks), return_exceptions=True)


def _prune(d: dict, now: float) -> None:
    if len(d) > 2000:
        for old in [x for x in d if x[1] < now - 86400]:
            d.pop(old, None)


async def _fetch_win(asset: str, start: int, now: float) -> Win | None:
    try:
        toks = await market_discovery.get_window_tokens(asset, LABEL, start)
    except Exception as exc:  # noqa: BLE001 — Gamma недоступна: попробуем позже
        _stats["discovery_errors"] += 1
        _stats["last_error"] = f"{asset}: {exc}"[:200]
        return None
    if not toks:
        return None
    up, down = toks
    own = bool(settings.USE_LIVE_BOOK_STREAM)
    if own and start > now:
        book_stream.subscribe([up, down], watch_after_ms=int(start * 1000))
    elif own:
        book_stream.subscribe([up, down])
    return Win(asset, start, start + STEP, slug_for(asset, start), up, down, own_tokens=own)


def _activate(w: Win) -> Win:
    storage.window_upsert(w.asset, w.start, slug=w.slug)
    _supported.add(w.asset)
    _fails[w.asset] = 0
    _stats["windows"] += 1
    return w


def _set_current(asset: str, w: Win, now: float) -> None:
    _cur[asset] = w
    if (runtime_state.is_asset_enabled(asset) and coin_live(asset) and enabled()
            and live_blocked(now) is None):
        from src import polymarket_client
        _spawn(polymarket_client.prewarm_market([w.up, w.down]))


def _maybe_open(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _opening or now < _retry_at.get(key, 0):
        return
    _opening.add(key)
    _spawn(_open(asset, start, now))


async def _open(asset: str, start: int, now: float) -> None:
    """Рынок текущего окна, если он не нашёлся заранее, — в фоне: запрос к Gamma
    (до 10 с) не должен держать тик остальных монет."""
    key = (asset, start)
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            _retry_at[key] = now + (LATE_RETRY_SEC if now - start > 60 else DISCOVERY_RETRY_SEC)
            _prune(_retry_at, now)
            return
        cur = _cur.get(asset)
        if _last_start.get(asset) != start or (cur is not None and cur.start == start):
            _release(w)                           # окно уже сменилось или рынок уже есть
            return
        _set_current(asset, _activate(w), now)
    finally:
        _opening.discard(key)


def _release(w: Win) -> None:
    if not w.own_tokens:
        return
    cur = _cur.get(w.asset)
    busy = {cur.up, cur.down} if cur is not None else set()
    toks = [t for t in (w.up, w.down) if t not in busy]
    if toks:
        book_stream.unsubscribe(toks)


def _maybe_prefetch(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _prefetching or now < _prefetch_at.get(key, 0):
        return
    _prefetching.add(key)
    _prefetch_at[key] = now + PREFETCH_RETRY_SEC
    _prune(_prefetch_at, now)
    _spawn(_prefetch(asset, start, now))


async def _prefetch(asset: str, start: int, now: float) -> None:
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            return
        old, cur = _next.get(asset), _cur.get(asset)
        if (old is not None and old.start == start) or (cur is not None and cur.start == start):
            return
        if old is not None:
            _release(old)
        _next[asset] = w
    finally:
        _prefetching.discard((asset, start))


def _num(v: float | None, digits: int = 8):
    return None if v is None else float(f"{v:.{digits}g}")


def _flush(w: Win) -> None:
    storage.window_upsert(w.asset, w.start, slug=w.slug, path=json.dumps(w.path, separators=(",", ":")))


def _sample(w: Win, now: float) -> None:
    """Точка пути каждые REC_STEP_SEC с. Поля [0..4] неизменны (совместимость со старыми
    записями и выгрузками): [секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR].
    Поля [5..10] добавлены для симуляции мейкерского входа — бид обеих сторон и объёмы:
    [bid UP, bid DOWN, объём(ask UP), объём(ask DOWN), объём(bid UP), объём(bid DOWN)]."""
    t_left = w.end - now
    if w.strike is None and now >= w.start:
        # страйк — цена Chainlink на начало окна: первая точка не раньше начала; если её
        # так и нет через STRIKE_WAIT_SEC — ближайшая в пределах 5 с (как в chainlink_feed)
        s = chainlink_feed.price_after(w.asset, w.start)
        if s is None and now >= w.start + STRIKE_WAIT_SEC:
            s = chainlink_feed.price_at(w.asset, w.start)
        if s is not None:
            w.strike = s
            _stats["striked"] += 1
            storage.window_upsert(w.asset, w.start, strike=_num(s))
    step = max(0.5, float(settings.REC_STEP_SEC))
    if settings.REC_TO_SEC <= t_left <= settings.REC_FROM_SEC and w.last_rec_t - t_left >= step - 0.5:
        bu, bus, ua, uas = book_stream.top_of_book(w.up)
        bd, bds, da, das = book_stream.top_of_book(w.down)
        price, age = chainlink_feed.latest(w.asset, now)
        if price is not None and age is not None and age > settings.CHAINLINK_MAX_AGE_SEC:
            price = None
        w.path.append([round(t_left, 1), ua, da, _num(price), _num(atr_feed.get(w.asset, now), 6),
                       bu, bd, _num(uas, 6), _num(das, 6), _num(bus, 6), _num(bds, 6)])
        w.last_rec_t = t_left
    if w.end - BOOK_WINDOW_SEC <= now < w.end:
        w.book_res, w.book_res_t = book_winner(w.up, w.down), now
    if now - w.last_flush >= FLUSH_EVERY_SEC:
        w.last_flush = now
        _flush(w)


def _close(w: Win) -> None:
    _flush(w)
    if w.own_tokens:
        book_stream.unsubscribe([w.up, w.down])
    _pending.append(w)


def resolve_pending(now: float | None = None) -> int:
    """Ранний исход закончившихся окон: стакан последних секунд, иначе Chainlink конец vs страйк."""
    now = time.time() if now is None else now
    done = 0
    for w in list(_pending):
        if now < w.end:
            continue
        book = w.book_res if w.book_res_t >= w.end - BOOK_LAST_SEC else None
        if book and not w.early_set:
            w.early_set = True
            storage.window_upsert(w.asset, w.start, early_outcome=book)
            done += 1
        cl_end = chainlink_feed.price_after(w.asset, w.end)
        if cl_end is None and now < w.end + EARLY_WAIT_SEC:
            continue
        _pending.remove(w)
        if not w.early_set and w.strike and cl_end:
            w.early_set = True
            storage.window_upsert(w.asset, w.start, early_outcome="UP" if cl_end >= w.strike else "DOWN",
                                  cl_end=_num(cl_end))
            done += 1
        elif cl_end:
            storage.window_upsert(w.asset, w.start, cl_end=_num(cl_end))
    _stats["early"] += done
    return done


async def settle(now: float | None = None) -> int:
    """Официальные исходы закончившихся окон (Gamma), по несколько за проход."""
    now = time.time() if now is None else now
    n = 0
    for asset, start, slug in storage.windows_unresolved(int(now - SETTLE_LAG_SEC), int(now - SETTLE_MAX_AGE_SEC)):
        try:
            res = await market_discovery.get_resolution(slug)
        except Exception:  # noqa: BLE001
            res = None
        if res in ("UP", "DOWN"):
            storage.window_upsert(asset, start, outcome=res)
            _stats["official"] += 1
            n += 1
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    return n


# --------------------------------------------------------------- тик ----

def _new_window(asset: str, start: int, now: float) -> None:
    prev = _last_start.get(asset)
    _last_start[asset] = start
    if prev is None:
        return
    cur = _cur.get(asset)
    if cur is None or cur.start != prev:
        _fails[asset] += 1                     # прошлое окно так и не нашлось
        if (_fails[asset] >= UNSUPPORTED_AFTER and asset not in _supported
                and now >= _skip_until.get(asset, 0)):
            _skip_until[asset] = now + UNSUPPORTED_SKIP_SEC
            log.info("%s: рынков 5m нет %d окон подряд — пропускаю %d ч", asset, _fails[asset],
                     UNSUPPORTED_SKIP_SEC // 3600)


async def _tick_current(asset: str, start: int, now: float) -> None:
    w = _cur.get(asset)
    if w is not None and w.start != start:
        _close(w)
        _cur.pop(asset, None)
        w = None
    if w is None:
        nw = _next.get(asset)
        if nw is None or nw.start != start:
            if nw is not None and nw.start < start:
                _release(_next.pop(asset))
            _maybe_open(asset, start, now)        # найдётся — подхватим на следующем тике
            return
        w = _activate(_next.pop(asset))
        _set_current(asset, w, now)
    _sample(w, now)


def _tick_next(asset: str, nxt: int, now: float) -> None:
    if nxt - now > PREFETCH_SEC:
        return
    nw = _next.get(asset)
    if nw is None or nw.start != nxt:
        _maybe_prefetch(asset, nxt, now)


async def tick(now: float | None = None) -> None:
    now = time.time() if now is None else now
    start = int(now // STEP) * STEP
    for asset in settings.ASSETS:
        if _last_start.get(asset) != start:
            _new_window(asset, start, now)
        if now < _skip_until.get(asset, 0):
            continue
        await _tick_current(asset, start, now)
        _tick_next(asset, start + STEP, now)
    resolve_pending(now)
    await maybe_trade(now)
    maybe_addons(now)


# ------------------------------------------------------------ правило ----

def evaluate(w: Win, now: float) -> tuple[bool, dict]:
    """Можно ли входить в окно w прямо сейчас. (да/нет, подробности/причина)."""
    price, age = chainlink_feed.latest(w.asset, now)
    if w.strike is None:
        return False, {"why": "нет страйка (бот не застал начало окна)"}
    if price is None or age is None or age > settings.CHAINLINK_MAX_AGE_SEC:
        return False, {"why": "нет свежей цены Chainlink"}
    if price == w.strike:
        return False, {"why": "цена на страйке"}
    side = "UP" if price > w.strike else "DOWN"
    token = w.up if side == "UP" else w.down
    ask = book_stream.best_ask(token)
    dist = abs(price - w.strike) / w.strike * 100
    atr = atr_feed.get(w.asset, now)
    datr = abs(price - w.strike) / atr if atr else None
    info = dict(side=side, token=token, ask=ask, price_now=price, dist=dist, atr=atr, datr=datr)
    lo, hi = price_range()
    if ask is None:
        info["why"] = "нет стакана"
    elif not book_stream.is_fresh(token, BOOK_MAX_AGE_MS):
        info["why"] = "стакан не обновлялся 30 с"
    elif not lo - 1e-9 <= ask <= hi + 1e-9:
        info["why"] = f"цена вне {lo:.2f}–{hi:.2f}"
    elif dist < min_dist(w.asset, side) - 1e-12:
        info["why"] = f"до страйка меньше {min_dist(w.asset, side):g}%" + _side_note(w.asset, side, "dist")
    elif min_atr(w.asset, side) > 0 and atr is None:
        info["why"] = "нет ATR (Binance)"
    elif min_atr(w.asset, side) > 0 and datr < min_atr(w.asset, side) - 1e-9:
        info["why"] = f"меньше {min_atr(w.asset, side):g} ATR от страйка" + _side_note(w.asset, side, "atr")
    else:
        hot = hot_why(w.asset, w.start, price, side, atr)
        if hot is None:
            return True, info
        info["why"] = hot
    return False, info


def _side_note(asset: str, side: str, key: str) -> str:
    """« (порог DOWN)» — если для этой стороны у монеты свой порог."""
    return f" (порог {side})" if f"{key}_{side}" in coin_rule_own(asset) else ""


def dry_fill(token: str, cap: float, usdc: float, used: dict | None = None) -> tuple[float | None, float]:
    """(средняя цена, USDC) покупки на usdc по уровням asks не дороже cap. used — {цена: акций}, уже
    «купленных» в этом тике (несколько уровней докупки сразу не берут один и тот же объём дважды)."""
    b = book_stream.get_book(token)
    spent = shares = 0.0
    for price, size in sorted((b or {}).get("asks", {}).items()):
        if price > cap + 1e-9 or spent >= usdc - 1e-9:
            break
        avail = size - (used.get(price, 0.0) if used is not None else 0.0)
        if avail <= 1e-9:
            continue
        take = min(price * avail, usdc - spent)
        spent += take
        shares += take / price
        if used is not None:
            used[price] = used.get(price, 0.0) + take / price
    return (round(spent / shares, 4), round(spent, 2)) if shares > 0 else (None, 0.0)


def today_live_pnl(now: float | None = None) -> float:
    """Итог реальных ставок и реальных докупок, закрытых сегодня (UTC) — для дневного стопа. Вместе с ⏱15:
    реальные ставки и докупки 15m — с того же кошелька, стоп общий."""
    now = time.time() if now is None else now
    day = int(now // 86400) * 86400
    rows = (storage.trades_since(day - 3600) + storage.addons_since(day - 3600)
            + storage.t15_since(day - 3600) + storage.a15_since(day - 3600))
    return sum(t["pnl_usdc"] or 0.0 for t in rows if not t["dry_run"] and t["settled_ts"] and t["settled_ts"] >= day)


def live_blocked(now: float | None = None) -> str | None:
    """Почему реальная ставка сейчас нельзя (None — можно)."""
    if not settings.POLY_PRIVATE_KEY:
        return "нет ключа POLY_PRIVATE_KEY"
    if runtime_state.get("dry_run"):
        return "бот в DRY RUN"
    s = stop()
    if s > 0 and today_live_pnl(now) <= -s:
        return f"дневной стоп −${s:g}"
    return None


async def maybe_trade(now: float | None = None) -> int:
    """Входы по правилу. Возвращает, сколько тестовых ставок записали."""
    now = time.time() if now is None else now
    if not enabled():
        return 0
    hi_t, lo_t = window()
    start = int(now // STEP) * STEP
    done = 0
    for asset in trade_assets():
        w = _cur.get(asset)
        if w is None or w.start != start or w.decided:
            continue
        t_left = w.end - now
        if t_left > hi_t + 1e-9:
            continue                              # окно входа ещё не началось
        if t_left < lo_t - 1e-9:
            w.decided = True                      # окно входа прошло без ставки
            if w.why:
                _why[asset][w.why] += 1
            continue
        if not w.checked:
            w.checked = True
            if storage.trade_exists(asset, start, dry_run=True):
                w.decided = True                  # после перезапуска в том же окне — не дублируем
                continue
        ok, info = evaluate(w, now)
        if not ok:
            w.why = info["why"]
            continue
        done += _enter(w, info, now)
    return done


def _cap(ask: float) -> float:
    """Потолок цены ордера: ask + 1¢, но не выше верхней границы цены + 1¢."""
    return min(round(ask + PRICE_SLIP, 2), round(price_range()[1] + PRICE_SLIP, 2))


def _enter(w: Win, info: dict, now: float) -> int:
    """Тестовая ставка по стакану; если монета 🔴 — ещё и реальная (в фоне, чтобы не держать тик)."""
    ask = info["ask"]
    cap = _cap(ask)
    usdc = bet_usdc(cap, w.asset)
    price, spent = dry_fill(info["token"], cap, usdc)
    if price is None or spent < MIN_TRADE_USDC:
        w.why = "мало объёма в стакане"          # пробуем дальше, пока идёт окно входа
        return 0
    w.decided = True
    base = dict(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"], token_id=info["token"],
                t_left=round(w.end - now, 1), price_now=_num(info["price_now"]), strike=_num(w.strike),
                dist_pct=round(info["dist"], 4), dist_atr=round(info["datr"], 3) if info["datr"] else None,
                atr=_num(info["atr"], 6), ask=ask, rule=rule_tag(w.asset, info["side"]))
    tid = storage.trade_log(**base, price=price, size_usdc=spent, shares=round(spent / price, 4),
                            fee_usdc=fee_for(spent, price), dry_run=1, status="DRY")
    if tid and addon_on(w.asset):
        _arm_addon(w, info, now)
    if (coin_live(w.asset) and side_allowed(w.asset, info["side"])
            and not storage.trade_exists(w.asset, w.start, dry_run=False)):
        _spawn(_enter_live(w, info, base, cap, usdc, now))
    return 1 if tid else 0


async def _live_buy(token: str, cap: float, usdc: float):
    """Реальная покупка FAK-ордером по цене не выше cap: (USDC, акций, id, статус) или None (не исполнился)."""
    from src import polymarket_client
    tick = book_stream.tick_size(token)
    cap = polymarket_client.round_price_for_buy(cap, tick)
    try:
        resp = await polymarket_client.place_buy_order(token, cap, usdc, tick, order_type="FAK")
    except Exception as exc:  # noqa: BLE001
        if polymarket_client.is_no_fill_error(exc):
            return None
        raise
    oid = polymarket_client.response_field(resp, "order_id") or polymarket_client.response_field(resp, "orderID")
    status = str(polymarket_client.response_field(resp, "status") or "matched")
    amounts = polymarket_client.filled_amounts(resp)
    if amounts is None or status.lower() == "delayed" or (amounts[0] <= 0 and status.lower() == "matched"):
        spent, shares = usdc, usdc / cap
    else:
        spent, shares = amounts
    if spent <= 0:
        return None
    return round(spent, 2), shares, str(oid or ""), status


async def _notify(text: str) -> None:
    try:
        from src import telegram_notify
        await telegram_notify.notify(text)
    except Exception:  # noqa: BLE001
        pass


async def _notify_error(asset: str, exc: BaseException, key: str | None = None) -> None:
    """Ошибка ордера, которую повтор не лечит (баланс, ключ), — в Telegram, но одна и та же не чаще
    раза в ERROR_NOTE_EVERY_SEC. key — что считать «той же» (по умолчанию текст ошибки)."""
    key = (key or str(exc)).strip().lower()[:80]
    now = time.time()
    if now - _err_notes.get(key, 0.0) < ERROR_NOTE_EVERY_SEC:
        return
    _err_notes[key] = now
    await _notify(f"❌ ⏱ {asset.upper()}: ошибка ордера — {exc}"[:400])


async def _enter_live(w: Win, info: dict, base: dict, cap: float, usdc: float, now: float) -> None:
    """Реальная покупка. Не исполнилось (или исполнилось частично) — снова через LIVE_RETRY_SEC, пока идёт
    окно входа и условия выполняются, добирая до суммы ставки. В Telegram — только итог покупки."""
    why = live_blocked(now)
    if why:
        day = int(now // 86400)
        if why.startswith("дневной стоп") and _trade_note["stop_day"] != day:
            _trade_note["stop_day"] = day
            await _notify(f"⏱ Реальные ставки остановлены до конца дня — {why}. Тестовые идут дальше.")
        return
    target, limited = live_target(w.asset, usdc)
    if limited == "coin" and target < max(MIN_TRADE_USDC, round(MIN_ORDER_SHARES * cap + 0.01, 2)):
        own = coin_bank(w.asset) or {"free": 0.0, "now": 0.0}
        await _notify_error(w.asset, RuntimeError(
            f"в банке монеты свободно ${max(0.0, own['free']):.2f} (банк ${own['now']:.2f}) — меньше минимального "
            f"ордера; реальные ставки по {w.asset.upper()} не идут, пока банк не пополнишь (🪙 → 💵 → 🏦). "
            "Тест идёт."), key=f"coinbank:{w.asset}")
        return
    from src import polymarket_client
    t0 = time.monotonic()
    side, token = info["side"], info["token"]
    lo_t = window()[1]
    wallet = _bank["wallet"]
    spent = shares = 0.0
    oids: list[str] = []
    status = ""
    orders = errors = 0
    last = ""
    first_fill = None
    cur_cap, cur_ask = cap, info["ask"]
    for i in range(LIVE_MAX_TRIES):
        t_now = now + (time.monotonic() - t0)
        if i:
            if w.end - t_now < lo_t - 1e-9:
                last = last or "окно входа кончилось"
                break
            ok, again = evaluate(w, t_now)
            if not ok or again["side"] != side:
                last = again.get("why") or "сторона сменилась"
                await asyncio.sleep(LIVE_RETRY_SEC)
                continue
            cur_ask = again["ask"]
            cur_cap = _cap(cur_ask)
        left = round(target - spent, 2)
        min_amt = max(MIN_TRADE_USDC, round(MIN_ORDER_SHARES * cur_cap + 0.01, 2))
        if left < min_amt:
            if not spent and limited == "coin":
                last = f"в банке монеты свободно ${target:.2f} — меньше минимального ордера ${min_amt:.2f}"
            elif not spent and wallet is not None:
                last = f"на балансе ${wallet:.2f} — меньше минимального ордера ${min_amt:.2f}"
                await _notify_error(w.asset, RuntimeError(f"на балансе ${wallet:.2f} — меньше ставки, "
                                                          "реальная не ушла"))
            break                                        # набрали (или остаток меньше минимального ордера)
        orders += 1
        try:
            res = await _live_buy(token, cur_cap, left)
        except Exception as exc:  # noqa: BLE001
            last = f"ошибка: {exc}"[:200]
            errors += 1
            if polymarket_client.is_fatal_error(exc) or errors >= LIVE_MAX_ERRORS:
                await _notify_error(w.asset, exc)
                break
            await asyncio.sleep(LIVE_RETRY_SEC)
            continue
        errors = 0
        if res is None:
            last = f"не исполнился по ≤{cur_cap:.2f}"
        else:
            got, got_shares, oid, status = res
            if first_fill is None:
                first_fill = (t_now, cur_ask)
            spent += got
            shares += got_shares
            if oid:
                oids.append(oid)
            if got >= left - 0.01:
                break                                    # исполнилось на всю сумму
            last = f"частично: ${spent:.2f} из ${target:.2f}"
        await asyncio.sleep(LIVE_RETRY_SEC)
    note = f"ордеров {orders}" + (f"; {last}" if last and spent < target - 0.01 else "")
    if spent <= 0:
        storage.trade_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                          status="SKIP_NOFILL", note=note[:300])
        return
    price = round(spent / shares, 4) if shares > 0 else cur_cap
    t_fill, ask_fill = first_fill
    row = dict(base, ts=int(t_fill), t_left=round(w.end - t_fill, 1), ask=ask_fill)
    storage.trade_log(**row, price=price, size_usdc=round(spent, 2), shares=round(shares, 4),
                      fee_usdc=fee_for(spent, price), dry_run=0, status=status or "matched",
                      order_id=",".join(oids)[:500], note=note[:300])
    _spawn(refresh_bank(force=True))
    await _notify(f"✅ ⏱ {w.asset.upper()}: ставка {side} ${spent:.2f} по {price:.3f} за "
                  f"{mmss(w.end - t_fill)} до конца (до страйка {info['dist']:.2f}%"
                  + (f", {info['datr']:.1f} ATR" if info["datr"] else "") + ")")


# ------------------------------------------------------------ ➕ докупка ----

def _arm_addon(w: Win, info: dict, now: float) -> None:
    """После основной ставки: какие уровни ждут отката — только ниже цены входа (ask по сигналу). В базу —
    строка ARMED (уровень 0): окно, где докупка была включена, — итоги «с докупкой» считаются по таким окнам."""
    entry = float(info["ask"])
    levels = [lv for lv in addon_levels(w.asset) if round(lv, 2) < round(entry, 2)]
    w.addon = {"side": info["side"], "token": info["token"], "entry": entry, "left": levels, "lock": None}
    storage.addon_log(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"],
                      token_id=info["token"], level=0.0, entry_ask=entry, t_left=round(w.end - now, 1), ask=entry,
                      size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=1, status="ARMED",
                      rule=rule_tag(w.asset, info["side"]),
                      note=f"уровни {levels_text(levels)}" if levels else "все уровни не ниже цены входа")


def addon_live_why(w: Win, now: float | None = None) -> str | None:
    """Почему реальная докупка по окну не идёт (None — можно): монета 🧪, бот в DRY / стоп / нет ключа,
    по окну нет реальной основной ставки."""
    if not addon_on(w.asset) or not addon_live(w.asset):
        return f"докупка {w.asset.upper()} не 🔴"
    if not coin_live(w.asset):
        return f"{w.asset.upper()} 🧪 — реальных ставок по монете нет"
    why = live_blocked(now)
    if why:
        return why
    real = storage.trade_get(w.asset, w.start, dry_run=False)
    if not real or not (real["size_usdc"] or 0) > 0:
        return "по окну нет реальной основной ставки"
    return None


def maybe_addons(now: float | None = None) -> int:
    """➕ По окнам с основной ставкой: ask нашей стороны опустился до уровня — докупка не дороже него.
    Тестовая — сразу, по стакану; реальная (докупка 🔴) — в фоне. Каждый уровень — раз за окно; уровни, до
    которых цена провалилась разом, — все. Возвращает, сколько тестовых докупок записали."""
    now = time.time() if now is None else now
    start = int(now // STEP) * STEP
    done = 0
    for w in list(_cur.values()):
        ad = w.addon
        if not ad or not ad["left"] or w.start != start:
            continue
        if w.end - now < ADDON_MIN_LEFT_SEC:
            ad["left"] = []                                # последние секунды — без докупки
            continue
        if not addon_on(w.asset):
            continue
        token = ad["token"]
        ask = book_stream.best_ask(token)
        if ask is None or not book_stream.is_fresh(token, BOOK_MAX_AGE_MS):
            continue
        hit = [lv for lv in ad["left"] if ask <= lv + 1e-9]
        if not hit:
            continue
        amt = addon_usd_for(w.asset)                       # $ или % банка монеты
        if amt < MIN_DRY_ADDON_USDC:
            continue                                       # банк пуст — нечего докупать
        used: dict = {}
        live_levels = []
        for lv in hit:
            price, spent = dry_fill(token, lv, amt, used)
            if price is None or spent < MIN_DRY_ADDON_USDC:
                continue                                   # объём съели уровни выше — на следующем тике
            ad["left"].remove(lv)
            base = dict(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=ad["side"], token_id=token,
                        level=lv, entry_ask=ad["entry"], t_left=round(w.end - now, 1), ask=ask,
                        rule=rule_tag(w.asset, ad["side"]))
            if storage.addon_log(**base, price=price, size_usdc=spent, shares=round(spent / price, 4),
                                 fee_usdc=fee_for(spent, price), dry_run=1, status="DRY"):
                done += 1
            if addon_live(w.asset) and coin_live(w.asset):   # монета 🧪 — реальной докупки и не ждём
                live_levels.append((lv, base))
        if live_levels:
            why = addon_live_why(w, now)
            if why is None:
                for lv, base in live_levels:
                    _spawn(_addon_live(w, base, lv, now))
            else:
                _addon_note.update(why=why, ts=now)
    return done


async def _addon_live(w: Win, base: dict, lv: float, now: float) -> None:
    """Реальная докупка на уровне lv. Уровни одного окна — по очереди (банк монеты и кошелёк считаются
    с учётом уже купленного)."""
    t_spawn = time.monotonic()
    ad = w.addon if w.addon is not None else {}
    if ad.get("lock") is None:
        ad["lock"] = asyncio.Lock()
    async with ad["lock"]:
        await _addon_live_run(w, base, lv, now, t_spawn)


async def _addon_live_run(w: Win, base: dict, lv: float, now: float, t_spawn: float) -> None:
    """FAK не дороже уровня на $ уровня (не меньше 5 акций, не больше свободного в банке монеты и на кошельке).
    Цену перебили — ждём возврата к уровню до ADDON_LIVE_WAIT_SEC, не больше ADDON_LIVE_TRIES ордеров.
    Не купилось — строка SKIP_… с причиной; купилось — строка и сообщение в Telegram."""
    from src import polymarket_client

    def t_now() -> float:
        return now + (time.monotonic() - t_spawn)

    why = addon_live_why(w, t_now())                      # пока ждали очереди, могли выключить
    if why:
        _addon_note.update(why=why, ts=t_now())
        return
    token, side, a = base["token_id"], base["side"], w.asset.upper()
    min_amt = max(MIN_TRADE_USDC, round(MIN_ORDER_SHARES * lv + 0.01, 2))
    target, limited = live_target(w.asset, addon_live_usd(lv, w.asset))
    if target < min_amt:
        reason = (f"в банке {a} свободно меньше минимального ордера ${min_amt:.2f}" if limited == "coin"
                  else f"на балансе меньше минимального ордера ${min_amt:.2f}")
        _addon_note.update(why=reason, ts=t_now())
        storage.addon_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                          status="SKIP_NOMONEY", note=reason)
        return
    spent = shares = 0.0
    oids: list[str] = []
    status = last = ""
    orders = errors = 0
    first_fill = None
    t_begin = time.monotonic()
    for _i in range(LIVE_MAX_TRIES):
        if orders >= ADDON_LIVE_TRIES or time.monotonic() - t_begin >= ADDON_LIVE_WAIT_SEC:
            break
        tn = t_now()
        if w.end - tn < ADDON_MIN_LEFT_SEC:
            last = last or f"до конца меньше {ADDON_MIN_LEFT_SEC:g} с"
            break
        ask = book_stream.best_ask(token)
        if ask is None or ask > lv + 1e-9:
            last = f"цена выше {lv:.2f}"
            await asyncio.sleep(LIVE_RETRY_SEC)
            continue
        left = round(target - spent, 2)
        if left < min_amt:
            break
        orders += 1
        try:
            res = await _live_buy(token, lv, left)
        except Exception as exc:  # noqa: BLE001
            last = f"ошибка: {exc}"[:200]
            errors += 1
            if polymarket_client.is_fatal_error(exc) or errors >= LIVE_MAX_ERRORS:
                await _notify_error(w.asset, exc)
                break
            await asyncio.sleep(LIVE_RETRY_SEC)
            continue
        errors = 0
        if res is None:
            last = f"не исполнился по ≤{lv:.2f}"
        else:
            got, got_shares, oid, status = res
            if first_fill is None:
                first_fill = (tn, ask)
            spent += got
            shares += got_shares
            if oid:
                oids.append(oid)
            if got >= left - 0.01:
                break                                     # на всю сумму
            last = f"частично: ${spent:.2f} из ${target:.2f}"
        await asyncio.sleep(LIVE_RETRY_SEC)
    note = f"ордеров {orders}" + (f"; {last}" if last and spent < target - 0.01 else "")
    if spent <= 0:
        storage.addon_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                          status="SKIP_NOFILL", note=note[:300])
        return
    price = round(spent / shares, 4) if shares > 0 else lv
    t_fill, ask_fill = first_fill
    storage.addon_log(**dict(base, ts=int(t_fill), t_left=round(w.end - t_fill, 1), ask=ask_fill), price=price,
                      size_usdc=round(spent, 2), shares=round(shares, 4), fee_usdc=fee_for(spent, price),
                      dry_run=0, status=status or "matched", order_id=",".join(oids)[:500], note=note[:300])
    _spawn(refresh_bank(force=True))
    await _notify(f"➕ ⏱ {a}: докупка {side} ${spent:.2f} по {price:.3f} (уровень {lv:.2f}) за "
                  f"{mmss(w.end - t_fill)} до конца")


def _is_buy(r: dict) -> bool:
    """Покупка, а не пропуск (SKIP_…, $0)."""
    return (r.get("size_usdc") or 0) > 0 and not str(r.get("status") or "").startswith("SKIP")


def _row_pnl(r: dict, outcome: str) -> float:
    """Итог ставки / докупки при исходе outcome: акции × 1 при выигрыше − сумма − комиссия."""
    if not _is_buy(r):
        return 0.0
    return (r["shares"] if outcome == r["side"] else 0.0) - r["size_usdc"] - (r["fee_usdc"] or 0.0)


def buys_text(n: int) -> str:
    return f"{n} {plural(n, 'покупка', 'покупки', 'покупок')}"


def windows_text(n: int) -> str:
    """«в 1 окне», «в 3 окнах»."""
    return f"{n} {plural(n, 'окне', 'окнах', 'окнах')}"


def settle_text(t: dict, outcome: str, pnl: float) -> str:
    """Итог реальной ставки для Telegram — и с докупкой окна: реальной, а нет её — тестовой («было бы»)."""
    win = outcome == t["side"]
    s = f"⏱ {'🟢' if win else '🔴'} {t['asset'].upper()}: исход {outcome}, ставка {t['side']} → {pnl:+.2f} USDC"
    rows = storage.addons_window(t["asset"], t["start_ts"])
    real = [r for r in rows if not r["dry_run"] and _is_buy(r)]
    test = [r for r in rows if r["dry_run"] and _is_buy(r)]
    armed = any(r["status"] == "ARMED" for r in rows)
    if real:
        ap = sum(_row_pnl(r, outcome) for r in real)
        s += (f"\n➕ Докупка реально: {buys_text(len(real))} на ${sum(r['size_usdc'] for r in real):.2f} "
              f"(уровни {levels_text([r['level'] for r in real])}) → {ap:+.2f}; итог окна {pnl + ap:+.2f} USDC")
    elif test:
        ap = sum(_row_pnl(r, outcome) for r in test)
        s += (f"\n➕ С тестовой докупкой было бы {pnl + ap:+.2f} USDC (докупка {ap:+.2f}: {buys_text(len(test))} "
              f"на ${sum(r['size_usdc'] for r in test):.2f}, уровни {levels_text([r['level'] for r in test])})")
    elif armed:
        s += "\n➕ Докупки не было — цена не откатила до уровней ниже входа; итог тот же"
    return s


async def _official(t: dict, now: float, cache: dict) -> str | None:
    key = (t["asset"], t["start_ts"])
    if key in cache:
        return cache[key]
    o = storage.window_outcome(*key)
    if o is None and now - (t["start_ts"] + STEP) >= SETTLE_LAG_SEC:
        try:
            o = await market_discovery.get_resolution(t["slug"])
        except Exception:  # noqa: BLE001
            o = None
        await asyncio.sleep(REQUEST_PAUSE_SEC)
    cache[key] = o
    return o


async def settle_trades(now: float | None = None) -> int:
    """Итог ставок и ➕ докупок по официальному исходу окна. Докупки — первыми: итог реальной ставки в
    Telegram показывает и докупку окна. Возвращает, сколько ставок закрыли."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for a in storage.addons_open(int(now - 30)):
        if a["start_ts"] < now - SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(a, now, cache)
        if o in ("UP", "DOWN"):
            storage.addon_settle(a["id"], o, round(_row_pnl(a, o), 4))
    for t in storage.trades_open(int(now - 30)):
        if t["start_ts"] < now - SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(t, now, cache)
        if o not in ("UP", "DOWN"):
            continue
        skip = str(t["status"] or "").startswith("SKIP")
        pnl = 0.0 if skip else (t["shares"] if o == t["side"] else 0.0) - t["size_usdc"] - (t["fee_usdc"] or 0.0)
        if storage.trade_settle(t["id"], o, round(pnl, 4)):
            n += 1
            if not t["dry_run"] and not skip:
                await _notify(settle_text(t, o, pnl))
    return n


def trim_paths(now: float | None = None) -> int:
    days = int(settings.REC_KEEP_DAYS)
    if days <= 0:
        return 0
    now = time.time() if now is None else now
    return storage.windows_trim(int(now - days * 86400))


# ------------------------------------------------------------- итоги ----

def trade_stats(since_ts: int = 0, live: bool = False, asset: str | None = None, side: str | None = None) -> dict:
    rows = [t for t in storage.trades_since(since_ts) if bool(t["dry_run"]) != live
            and (asset is None or t["asset"] == asset) and (side is None or t["side"] == side)]
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    wins = sum(1 for t in settled if t["outcome"] == t["side"])
    return {"bets": len(bets), "settled": len(settled), "wins": wins,
            "pnl": sum(t["pnl_usdc"] or 0.0 for t in settled),
            "staked": sum(t["size_usdc"] or 0.0 for t in settled),
            "avg_price": (sum(t["price"] for t in bets if t["price"]) / len(bets)) if bets else None}


def _addon_stats(rows: list[dict]) -> dict:
    buys = [r for r in rows if _is_buy(r)]
    settled = [r for r in buys if r["outcome"]]
    return {"bets": len(buys), "settled": len(settled),
            "wins": sum(1 for r in settled if r["outcome"] == r["side"]),
            "pnl": sum(r["pnl_usdc"] or 0.0 for r in settled),
            "staked": sum(r["size_usdc"] or 0.0 for r in settled),
            "avg_price": (sum(r["price"] for r in buys if r["price"]) / len(buys)) if buys else None,
            "windows": len({(r["asset"], r["start_ts"]) for r in buys}),
            "armed": sum(1 for r in rows if r["status"] == "ARMED"),
            "skipped": sum(1 for r in rows if str(r["status"] or "").startswith("SKIP"))}


def addon_stats(since_ts: int = 0, live: bool = False, asset: str | None = None, level: float | None = None) -> dict:
    """➕ Докупки (тестовые или реальные) с since_ts: покупок, закрыто, выигрышей, итог, в скольких окнах."""
    return _addon_stats([r for r in storage.addons_since(since_ts) if bool(r["dry_run"]) != live
                         and (asset is None or r["asset"] == asset)
                         and (level is None or abs((r["level"] or 0) - level) < 1e-6)])


def addon_line(name: str, st: dict) -> str:
    """«Тест докупки: 12 покупок в 5 окнах из 30 (ср. цена 0.931), выигр. 11/12, +0.42 $ (+3.5%)» — «из 30»:
    окон, где после ставки докупка ждала отката."""
    if not st["bets"]:
        return f"{name}: покупок нет" + (f" (ждала отката в {windows_text(st['armed'])})" if st["armed"] else "")
    s = f"{name}: {buys_text(st['bets'])} в {windows_text(st['windows'])}" + (
        f" из {st['armed']}" if st["armed"] >= st["windows"] > 0 else "")
    if st["avg_price"]:
        s += f" (ср. цена {st['avg_price']:.3f})"
    if st["settled"]:
        s += (f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
              + (f" ({st['pnl'] / st['staked'] * 100:+.1f}%)" if st["staked"] else ""))
    return s


def addon_level_line(since_ts: int = 0, live: bool = False, asset: str | None = None) -> str | None:
    """«по уровням: 0.95 — 5 (проигр. 0, +0.21 $) · 0.93 — 2 (проигр. 1, −0.85 $)» (asset — по одной монете)."""
    rows = [r for r in storage.addons_since(since_ts) if bool(r["dry_run"]) != live and _is_buy(r)
            and (asset is None or r["asset"] == asset)]
    parts = []
    for lv in sorted({round(r["level"] or 0, 2) for r in rows}, reverse=True):
        st = _addon_stats([r for r in rows if abs((r["level"] or 0) - lv) < 1e-6])
        lost = st["settled"] - st["wins"]
        parts.append(f"{lv:.2f} — {st['bets']}" + (f" (проигр. {lost}, {st['pnl']:+.2f} $)" if st["settled"] else ""))
    return ("по уровням: " + " · ".join(parts)) if parts else None


def with_addon(since_ts: int = 0, live: bool = True) -> dict | None:
    """Закрытые основные ставки (реальные или тестовые) с since_ts — только в окнах, где докупка была включена
    (строка ARMED), — и ТЕСТОВАЯ докупка в тех же окнах: итог ставок, итог докупки, сколько покупок и в скольких
    окнах. None — таких ставок нет."""
    adds_all = [r for r in storage.addons_since(since_ts) if r["dry_run"]]
    armed = {(r["asset"], r["start_ts"]) for r in adds_all if r["status"] == "ARMED"}
    bets = [t for t in storage.trades_since(since_ts) if bool(t["dry_run"]) != live and t["outcome"] and _is_buy(t)
            and (t["asset"], t["start_ts"]) in armed]
    if not bets:
        return None
    keys = {(t["asset"], t["start_ts"]) for t in bets}
    adds = [r for r in adds_all if _is_buy(r) and r["outcome"] and (r["asset"], r["start_ts"]) in keys]
    return {"bets": len(bets), "main": sum(t["pnl_usdc"] or 0.0 for t in bets),
            "addon": sum(r["pnl_usdc"] or 0.0 for r in adds), "buys": len(adds),
            "windows": len({(r["asset"], r["start_ts"]) for r in adds})}


def with_addon_line(name: str, c: dict) -> str:
    """«Реальные: 12 ставок, +2.40 $ → с тестовой докупкой было бы +3.10 $ (докупка +0.70 $: 9 покупок в 4 окнах)»."""
    s = f"{name}: {bets_text(c['bets'])}, {c['main']:+.2f} $"
    if not c["buys"]:
        return s + " — докупок в этих окнах не было"
    return (s + f" → с тестовой докупкой было бы {c['main'] + c['addon']:+.2f} $ (докупка {c['addon']:+.2f} $: "
            f"{buys_text(c['buys'])} в {windows_text(c['windows'])})")


def addon_lines(since_ts: int = 0) -> list[str]:
    """➕ Для 📊 и экрана докупки: настройки, тест докупки (и по уровням), «было бы» для тестовых и реальных
    ставок, реальная докупка."""
    lines = [f"➕ Докупка: {addon_short()}" + (" (общая)" if coin_addons_text() else "")]
    own = coin_addons_text()
    if own:
        lines.append(f"  Своя у монет: {own}")
    if any(addon_on(a) and addon_amount_mode(a) == "pct" for a in trade_assets()):
        lines.append(f"  На уровень сейчас: {addon_amounts_text()}")
    lines.append("  " + addon_line("Тест докупки", addon_stats(since_ts, live=False)))
    lvl = addon_level_line(since_ts, live=False)
    if lvl:
        lines.append("  " + lvl)
    for live, name in ((False, "Тестовые ставки (окна с докупкой)"), (True, "Реальные ставки (окна с докупкой)")):
        c = with_addon(since_ts, live)
        if c:
            lines.append("  " + with_addon_line(name, c))
    real = addon_stats(since_ts, live=True)
    if real["bets"] or real["skipped"] or addon_live() or any(addon_live(a) for a in settings.ASSETS):
        s = "  " + addon_line("Реальная докупка", real)
        if real["skipped"]:
            s += f" (не купилось: {real['skipped']} — причины в колонке note)"
        lines.append(s)
    if any(addon_on(a) and addon_live(a) for a in settings.ASSETS) and _addon_note["why"]:
        lines.append(f"  ⚠️ Последняя реальная докупка не пошла: {_addon_note['why']} "
                     f"({time.strftime('%d.%m %H:%M', time.gmtime(_addon_note['ts']))} UTC)")
    return lines


def addon_report_line(since_ts: int) -> str | None:
    """Одна строка для 4-часового отчёта (None — докупка выключена и покупок за период нет)."""
    test, real = addon_stats(since_ts, live=False), addon_stats(since_ts, live=True)
    if not (test["bets"] or real["bets"] or any_addon_on()):
        return None
    s = "➕ " + addon_line("Докупка (тест) за период", test)
    c = with_addon(since_ts, live=True)
    if c and c["buys"]:
        s += f"; реальные {c['main']:+.2f} $ → с тестовой докупкой было бы {c['main'] + c['addon']:+.2f} $"
    if real["bets"]:
        s += "; " + addon_line("реальная докупка", real)
    return s


def stats_line(name: str, st: dict) -> str:
    if not st["bets"]:
        return f"{name}: ставок нет"
    s = f"{name}: {bets_text(st['bets'])}"
    if st["avg_price"]:
        s += f" (ср. цена {st['avg_price']:.3f})"
    if st["settled"]:
        s += (f", выигр. {st['wins']}/{st['settled']} = {st['wins'] / st['settled'] * 100:.1f}%, "
              f"{st['pnl']:+.2f} $" + (f" ({st['pnl'] / st['staked'] * 100:+.1f}%)" if st["staked"] else ""))
    return s


def mode_text() -> str:
    coins = live_coins()
    if runtime_state.get("dry_run"):
        return "🧪 DRY RUN — реальных ордеров нет" + (
            f" (🔴 помечены {', '.join(coin_name(a) for a in coins)} — пойдут после «🔴 Включить LIVE»)" if coins else "")
    if not coins:
        return "🔴 LIVE, но ни одна включённая монета не 🔴 — реальных ставок нет"
    return f"🔴 LIVE — реальные ставки по {', '.join(coin_name(a) for a in coins)}" + (
        " (⬆️/⬇️ — только UP / только DOWN)" if any(side_mark(a) for a in coins) else "")


def strategy_lines() -> list[str]:
    day = int(time.time() // 86400) * 86400
    since = rule_since()
    lines = [f"⏱ Правило: {rule_text()}. Ставка: {stakes_text()} (не меньше 5 акций). "
             + ("✅ включено" if enabled() else "⏸ выключено"),
             "  Режим: " + mode_text(),
             "  " + stats_line("Тест всего", trade_stats(0, live=False))]
    if since:
        lines.append("  " + stats_line(f"Тест по нынешнему правилу (с {time.strftime('%d.%m %H:%M', time.gmtime(since))} UTC)",
                                       trade_stats(since, live=False)))
    lines.append("  " + stats_line("Тест сегодня (UTC)", trade_stats(day, live=False)))
    real = trade_stats(0, live=True)
    if real["bets"] or live_coins():
        lines.append("  " + stats_line("Реальные всего", real))
        s = stop()
        lines.append(f"  Реальные сегодня: {today_live_pnl():+.2f} USDC"
                     + (f" (дневной стоп −${s:g})" if s > 0 else " (без дневного стопа)"))
        why = live_blocked() if live_coins() else None
        if why and why != "бот в DRY RUN":
            lines.append(f"  ⚠️ Реальные ставки сейчас не уходят: {why}")
    test_add = addon_stats(0, live=False)
    if any_addon_on() or test_add["bets"]:
        own = coin_addons_text()
        lines.append(f"  ➕ Докупка: {addon_short()}" + (f" (своя у монет: {own})" if own else "") + ". "
                     + addon_line("Тест докупки всего", test_add))
    return lines


def stakes_text() -> str:
    """«общая $5; BTC 5% банка (~$10.67), ETH $20» — для экранов."""
    own = [f"{a.upper()} {stake_label(a)}" for a in settings.ASSETS if runtime_state.coin_stake(a)]
    return f"общая ${stake():g}" + (f"; {', '.join(own)}" if own else "")


def bank_line() -> str:
    b, src = bank()
    if b is not None:
        return f"🏦 Банк для ставок в %: ${b:.2f} ({src})"
    why = "нет ключа POLY_PRIVATE_KEY" if not settings.POLY_PRIVATE_KEY else (_bank["error"] or "ещё не читали")
    return f"🏦 Банк для ставок в %: неизвестен ({why}) — монеты с % ставят общую сумму; можно задать вручную"


def coin_bank_short(asset: str) -> str:
    """«$112.40 (задан $100 02.11 11:40 UTC)» или «не задан»."""
    own = coin_bank(asset)
    if own is None:
        return "не задан"
    return f"${own['now']:.2f} (задан ${own['start']:g} {time.strftime('%d.%m %H:%M', time.gmtime(own['since']))} UTC)"


def coin_bank_line(asset: str) -> str:
    """«🏦 Банк BTC: $100.00 → $112.40 (+12.40 с 02.11 11:40 UTC), в ставках $10.00, свободно $102.40»."""
    own = coin_bank(asset)
    a = asset.upper()
    if own is None:
        return f"🏦 Свой банк {a}: не задан — ставки в % считаются от общего банка."
    since = time.strftime("%d.%m %H:%M", time.gmtime(own["since"]))
    s = (f"🏦 Банк {a}: ${own['start']:.2f} → ${own['now']:.2f} ({own['pnl']:+.2f} с {since} UTC)"
         + (f", в ставках ${own['open']:.2f}" if own["open"] > 0.005 else "")
         + f", свободно ${max(0.0, own['free']):.2f}")
    if own["free"] < min_order_usdc():
        s += f" — ⚠️ меньше минимального ордера (~${min_order_usdc():g}), реальные ставки по {a} стоят"
    return s


def short_stats(name: str, st: dict) -> str:
    """«UP 26 ставок, проигр. 0, +7.10 $ (+5.8%)»."""
    if not st["settled"]:
        return f"{name}: ставок нет" if not st["bets"] else f"{name}: {st['bets']} (ещё не разыграны)"
    lost = st["settled"] - st["wins"]
    return (f"{name} {bets_text(st['settled'])}, проигр. {lost}, {st['pnl']:+.2f} $"
            + (f" ({st['pnl'] / st['staked'] * 100:+.1f}%)" if st["staked"] else ""))


def side_lines(since_ts: int = 0) -> list[str]:
    """По включённым монетам — тестовые ставки отдельно на UP и на DOWN (для решения 🎯)."""
    out = []
    for a in trade_assets():
        up, dn = trade_stats(since_ts, False, a, "UP"), trade_stats(since_ts, False, a, "DOWN")
        if not (up["bets"] or dn["bets"]):
            continue
        out.append(f"{coin_name(a)}: {short_stats('UP', up)} | {short_stats('DOWN', dn)}"
                   + (f" — реальные: {side_label(a)}" if runtime_state.coin_side(a) else ""))
    return out


def banks_warning() -> str | None:
    """Свободные деньги банков монет больше баланса кошелька — монеты будут делить кошелёк."""
    wallet = _bank["wallet"]
    if wallet is None:
        return None
    total = 0.0
    for a in settings.ASSETS:
        own = coin_bank(a)
        if own is not None:
            total += max(0.0, own["free"])
    if total > float(wallet) + 0.5:
        return (f"⚠️ Свободные деньги банков монет ${total:.2f} больше баланса кошелька ${float(wallet):.2f} — "
                "реальные ставки упрутся в кошелёк.")
    return None


def coin_lines() -> list[str]:
    out = []
    for a in settings.ASSETS:
        test, real = trade_stats(0, False, a), trade_stats(0, True, a)
        mark = f"{'✅' if runtime_state.is_asset_enabled(a) else '⏸'} {'🔴' if coin_live(a) else '🧪'}{side_mark(a)}"
        if now_skipped(a):
            mark += " (рынков 5m нет)"
        own = coin_bank(a)
        bank_s = ""
        if own is not None:
            bank_s = f" · 🏦 ${own['now']:.2f}" + (" ⚠️ исчерпан" if own["free"] < min_order_usdc() else "")
        rule_s = f" · 📏 {coin_rule_short(a)}" if coin_rule_own(a) else ""
        add_s = f" · ➕ {addon_short(a)}" if addon_custom(a) else ""
        s = f"{a.upper()} {mark} · 💵 {stake_label(a)}{bank_s}{rule_s}{add_s} — " + stats_line("тест", test)
        if real["bets"] or coin_live(a):
            s += "; " + stats_line("реальные", real)
            miss = sum(1 for t in storage.trades_since(0) if t["asset"] == a and not t["dry_run"]
                       and str(t["status"] or "") == "SKIP_NOFILL")
            if miss:
                s += f" (не купилось за окно входа: {miss})"
        out.append(s)
    return out


def now_skipped(asset: str) -> bool:
    return time.time() < _skip_until.get(asset, 0)


def why_lines() -> list[str]:
    """Почему по включённым монетам окна прошли без ставки (с запуска)."""
    lines = []
    for a in trade_assets():
        c = _why.get(a)
        if c:
            top = ", ".join(f"{k} — {v}" for k, v in c.most_common(3))
            lines.append(f"  {a.upper()}: {top}")
    return (["Окна без ставки (с запуска, главные причины):"] + lines) if lines else []


def recorder_line() -> str:
    rec = storage.window_rows(int(time.time()) - 86400)
    return (f"🔁 Запись: окон за сутки {len(rec)} по {len({r['asset'] for r in rec})} монетам, со страйком "
            f"{sum(1 for r in rec if r['strike'])}, путь с {mmss(settings.REC_FROM_SEC)} до конца каждые "
            f"{settings.REC_STEP_SEC:g} с.")


def feed_warnings(now: float | None = None) -> list[str]:
    """Чего не хватает для ставок по включённым монетам: свежей цены Chainlink, ATR."""
    now = time.time() if now is None else now
    coins = trade_assets()
    if not enabled() or not coins:
        return []
    out = []
    if not any((lambda p: p[0] is not None and p[1] is not None and p[1] <= settings.CHAINLINK_MAX_AGE_SEC)(
            chainlink_feed.latest(a, now)) for a in coins):
        out.append("⚠️ Нет свежей цены Chainlink — ставок не будет (📊 — что с потоком цен).")
    if any(min_atr(a, s) > 0 for a in coins for s in ("UP", "DOWN")) \
            and not any(atr_feed.get(a, now) is not None for a in coins):
        out.append("⚠️ Нет ATR с Binance — при условии по ATR ставок не будет (📊 — ошибка Binance).")
    return out


def chainlink_line(now: float | None = None) -> str:
    """Откуда идёт цена Chainlink и по каким монетам она свежая (без неё ставок нет)."""
    now = time.time() if now is None else now
    tr = chainlink_feed.transports()

    def state(t) -> str:
        if t is None:
            return "—"
        if t.disabled:
            return f"⏸ ({t.disabled})"
        if t.connected and now - t.last_data < 30:
            return "✅"
        return "❌" + (f" ({t.last_error[:80]})" if t.last_error else "")
    fresh = []
    for a in settings.ASSETS:
        price, age = chainlink_feed.latest(a, now)
        if price is not None and age is not None and age <= settings.CHAINLINK_MAX_AGE_SEC:
            fresh.append(a.upper())
    return (f"📡 Chainlink: PolyBolt {state(tr.get('polybolt'))}, RTDS {state(tr.get('rtds'))}; "
            f"свежая цена: {', '.join(fresh) if fresh else 'ни по одной монете — ставок не будет'}")


def stats_text() -> str:
    lines = ["📊 Статистика", ""] + [x for x in strategy_lines() if not x.startswith("  ➕")]
    if any_addon_on() or storage.addons_since(0):
        lines += ["", "За всё время в базе:"] + addon_lines()
    lines += ["", "По монетам (за всё время в базе):"]
    lines += ["  " + x for x in coin_lines()]
    sides = side_lines()
    if sides:
        lines += ["", "Тест по сторонам (за всё время в базе; ⬆️/⬇️ — реальные только UP / только DOWN):"]
        lines += ["  " + x for x in sides]
    w = why_lines()
    if w:
        lines += [""] + w
    if any(runtime_state.coin_stake(a) and runtime_state.coin_stake(a)[0] == "pct" for a in settings.ASSETS) \
            or settings.POLY_PRIVATE_KEY:
        lines += ["", bank_line()]
    own = [a for a in settings.ASSETS if runtime_state.coin_bank(a)]
    if own:
        lines += [""] + [coin_bank_line(a) for a in own]
        warn = banks_warning()
        if warn:
            lines.append(warn)
    lines += ["", recorder_line(), chainlink_line(), atr_feed.status_line(settings.ASSETS)]
    if _stats["discovery_errors"]:
        lines.append(f"Поиск рынков: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    rec = storage.window_rows(since_ts)
    lines = [f"🔁 Окна 5m {from_label} → {to_label} (UTC): {len(rec)}, со страйком {sum(1 for r in rec if r['strike'])}, "
             f"с официальным исходом {sum(1 for r in rec if r['outcome'])}"]
    st = trade_stats(since_ts, live=False)
    lines.append(stats_line("⏱ Тест за период", st))
    real = trade_stats(since_ts, live=True)
    miss = sum(1 for t in storage.trades_since(since_ts) if not t["dry_run"] and str(t["status"] or "") == "SKIP_NOFILL")
    if real["bets"] or miss:
        lines.append(stats_line("Реальные за период", real)
                     + (f"; не купилось за окно входа: {miss} (причины — в колонке note)" if miss else ""))
    add = addon_report_line(since_ts)
    if add:
        lines.append(add)
    up, dn = trade_stats(since_ts, False, side="UP"), trade_stats(since_ts, False, side="DOWN")
    if up["bets"] or dn["bets"]:
        lines.append(f"⏱ Тест по сторонам за период: {short_stats('UP', up)} | {short_stats('DOWN', dn)}")
    for a in trade_assets():
        only = runtime_state.coin_side(a)
        if only:
            off = "DOWN" if only == "UP" else "UP"
            lines.append(f"🎯 {coin_name(a)}: реальные {side_label(a)}; выключенная сторона в тесте за период — "
                         + short_stats(off, trade_stats(since_ts, False, a, off)))
    banks = []
    for a in settings.ASSETS:
        own = coin_bank(a)
        if own is not None:
            banks.append(f"{a.upper()} ${own['now']:.2f} ({own['pnl']:+.2f})"
                         + (" ⚠️ исчерпан" if own["free"] < min_order_usdc() else ""))
    if banks:
        lines.append("🏦 Банки монет: " + ", ".join(banks))
    return "\n".join(lines)


def addon_caption(head: str, since_ts: int) -> str:
    """Подпись к *_addons.csv: заголовок, настройки докупки и итоги с since_ts."""
    return "\n".join([head] + addon_lines(since_ts))


# ---------------------------------------------------------- цикл ----

async def _sleep_to_next_second() -> None:
    await asyncio.sleep(1.0 - (time.time() % 1.0) + 0.05)


async def _tick_loop() -> None:
    last_trim = 0.0
    while True:
        try:
            await tick()
            if time.monotonic() - last_trim >= TRIM_EVERY_SEC:
                last_trim = time.monotonic()
                trim_paths()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot: %s", exc)
        await _sleep_to_next_second()


async def _settle_loop() -> None:
    """Исходы окон и итоги ставок — отдельно от тика: запросы к Gamma не должны его держать.
    Здесь же — баланс кошелька для ставок в % банка."""
    while True:
        try:
            await refresh_bank()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot bank: %s", exc)
        await asyncio.sleep(SETTLE_EVERY_SEC)
        try:
            await settle()
            await settle_trades()
        except Exception as exc:  # noqa: BLE001
            log.warning("late_bot settle: %s", exc)


async def run_forever() -> None:
    await asyncio.sleep(3)
    await asyncio.gather(_tick_loop(), _settle_loop())
