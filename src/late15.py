"""
⏱15 15-минутные рынки Up/Down: запись окон, тестовые и реальные ставки с ➕ докупкой на откате.

Запись. По каждому окну каждой монеты (ASSETS) — страйк (цена Chainlink на старте окна), путь [секунд до конца,
ask UP, ask DOWN, цена Chainlink, ATR, bid UP, bid DOWN, объём ask UP, объём ask DOWN, объём bid UP, объём bid DOWN]
каждые REC15_STEP_SEC секунд с REC15_FROM_SEC до конца и исход (ранний — стакан / Chainlink, и официальный).

Ставка 15m — по монетам LATE15_COINS (по умолчанию BTC), правило подобрано по архиву 15m 18.09–22.11: за 8:30–8:00
до конца сторона по цене 0.96–0.97, цена Chainlink ушла от страйка на ≥0.10% и ≥2 ATR; обе стороны, одна ставка на
окно. Тестовая (по стакану) — всегда; реальная — если 15m 🔴 (кнопка в ⏱ 15m) и бот в 🔴 LIVE: FAK не дороже
ask + 1¢, не исполнилось — снова, пока идёт окно входа и условия держатся.

➕ Докупка 15m: после ставки цена нашей стороны опустилась до уровня (по умолчанию 0.95…0.90) — покупка не дороже
него, каждый уровень — раз за окно, пока до конца не меньше 10 с. Тестовая — всегда (пока включена), реальная —
только если по окну есть реальная ставка 15m.

Отдельно от 5m: свои таблицы (late15_windows, late15_trades, late15_addons) — в итоги и банки монет 5m не попадает;
свой цикл — ошибка здесь не задевает тик 5m. Общее с 5m: кошелёк, режим LIVE / DRY RUN и дневной стоп реальных
ставок (считает реальные 5m и 15m вместе).
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from config import settings
from src import atr_feed, book_stream, chainlink_feed, market_discovery, runtime_state, storage
from src import late_bot as lb          # общие помощники 5m: стакан, покупка, комиссия, кошелёк, тексты

log = logging.getLogger("late15")

STEP = 900
LABEL = "15m"


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
    last_rec_t: float = 1e9
    book_res: str | None = None
    book_res_t: float = 0.0
    early_set: bool = False
    last_flush: float = 0.0
    decided: bool = False
    checked: bool = False
    why: str = ""
    addon: dict | None = None


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
_why = defaultdict(Counter)
_stats = {"windows": 0, "striked": 0, "early": 0, "official": 0, "discovery_errors": 0, "last_error": ""}
_trade_note = {"stop_day": None}
_live_note = {"why": "", "ts": 0.0}       # почему последняя реальная ставка / докупка 15m не пошла (для экрана)


def reset() -> None:
    for d in (_cur, _next, _prefetch_at, _retry_at, _fails, _skip_until, _last_start, _why):
        d.clear()
    _prefetching.clear()
    _opening.clear()
    _pending.clear()
    _supported.clear()
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0
    _trade_note["stop_day"] = None
    _live_note.update(why="", ts=0.0)


# ---------------------------------------------------------- настройки ----

def on() -> bool:
    return bool(runtime_state.get("late15_on"))


def set_on(value: bool) -> None:
    runtime_state.set("late15_on", bool(value))


def live_on() -> bool:
    """🔴 Реальные ставки 15m включены (уходят, только когда бот в 🔴 LIVE)."""
    return bool(runtime_state.get("late15_live"))


def set_live(value: bool) -> None:
    runtime_state.set("late15_live", bool(value))


def coins() -> list[str]:
    """Монеты ставок 15m (из пишущихся ASSETS)."""
    want = [a.strip().lower() for a in (settings.LATE15_COINS or "").split(",") if a.strip()]
    return [a for a in settings.ASSETS if a in want]


def window() -> tuple[int, int]:
    a, b = int(runtime_state.get("late15_from_sec")), int(runtime_state.get("late15_to_sec"))
    hi, lo = max(a, b), min(a, b)
    return max(hi, lo + 1), lo


def price_range() -> tuple[float, float]:
    a, b = float(settings.LATE15_MIN_PRICE), float(settings.LATE15_MAX_PRICE)
    return min(a, b), max(a, b)


def min_dist() -> float:
    return max(0.0, float(settings.LATE15_MIN_DIST_PCT))


def min_atr() -> float:
    return max(0.0, float(settings.LATE15_MIN_ATR))


def stake_mode() -> str:
    """Сумма входа 15m: "usd" — фиксированная $, "pct" — % от банка."""
    return "pct" if runtime_state.get("late15_stake_mode") == "pct" else "usd"


def stake_fixed() -> float:
    return float(runtime_state.get("late15_stake"))


def stake_pct() -> float:
    return float(runtime_state.get("late15_stake_pct"))


def _of_bank(pct: float, cap: float) -> float | None:
    """pct% от банка (свободный баланс кошелька, не читается — банк вручную из ⚙️), не больше cap; None — банк
    неизвестен."""
    b, _src = lb.bank()
    if b is None:
        return None
    return round(min(cap, max(0.0, b) * pct / 100.0), 2)


def stake() -> float:
    """Сумма входа 15m — одна для теста и реальной ставки (до правила «не меньше 5 акций»): фиксированная или % от
    банка в момент входа (банк неизвестен — фиксированная)."""
    amt = _of_bank(stake_pct(), runtime_state.STAKE_LIMITS[1]) if stake_mode() == "pct" else None
    return max(lb.MIN_TRADE_USDC, stake_fixed() if amt is None else amt)


def _bank_note(pct: float, amt: float | None, fixed: float) -> str:
    b, src = lb.bank()
    if amt is None:
        return f"{pct:g}% банка (банк неизвестен — ${fixed:g})"
    return f"{pct:g}% банка (~${amt:.2f} от ${b:.2f}, {src})"


def stake_text() -> str:
    """«$30» или «5% банка (~$52.30 от $1046.00, баланс кошелька)»."""
    if stake_mode() != "pct":
        return f"${stake_fixed():g}"
    return _bank_note(stake_pct(), _of_bank(stake_pct(), runtime_state.STAKE_LIMITS[1]), stake_fixed())


def bet_usdc(cap: float) -> float:
    return max(stake(), round(lb.MIN_ORDER_SHARES * cap + 0.01, 2))


def addon_on() -> bool:
    return bool(runtime_state.get("late15_addon_on"))


def addon_levels() -> list[float]:
    raw = str(runtime_state.get("late15_addon_levels") or "")
    return sorted({round(float(x), 2) for x in raw.split(",") if x.strip()}, reverse=True)


def addon_mode() -> str:
    """Сумма на уровень докупки 15m: "usd" — фиксированная $, "pct" — % от банка."""
    return "pct" if runtime_state.get("late15_addon_mode") == "pct" else "usd"


def addon_fixed() -> float:
    return float(runtime_state.get("late15_addon_usd"))


def addon_pct() -> float:
    return float(runtime_state.get("late15_addon_pct"))


def addon_usd() -> float:
    """$ на уровень сейчас: фиксированная или % от банка (банк неизвестен — фиксированная)."""
    amt = _of_bank(addon_pct(), runtime_state.ADDON_USD_LIMITS[1]) if addon_mode() == "pct" else None
    return addon_fixed() if amt is None else amt


def addon_amount_text() -> str:
    """«$5» или «0.5% банка (~$5.23 от $1046.00, баланс кошелька)»."""
    if addon_mode() != "pct":
        return f"${addon_fixed():g}"
    return _bank_note(addon_pct(), _of_bank(addon_pct(), runtime_state.ADDON_USD_LIMITS[1]), addon_fixed())


def addon_live_usd(level: float) -> float:
    """Реальная докупка на уровне: $ на уровень, но не меньше 5 акций по уровню."""
    return max(addon_usd(), round(lb.MIN_ORDER_SHARES * level + 0.01, 2))


def levels_text(levels: list[float] | None = None) -> str:
    return lb.levels_text(addon_levels() if levels is None else levels)


def rule_tag() -> str:
    hi, lo = window()
    pl, ph = price_range()
    return f"15m|{hi}-{lo}s|{pl:.2f}-{ph:.2f}|d{min_dist():g}%|atr{min_atr():g}"


def window_phrase() -> str:
    hi, lo = window()
    return f"за {lb.mmss(lo)}–{lb.mmss(hi)} до конца"


def rule_short() -> str:
    """«за 8:00–8:30 до конца, цена 0.96–0.97, до страйка ≥0.1% и ≥2 ATR»."""
    pl, ph = price_range()
    atr = f" и ≥{min_atr():g} ATR" if min_atr() > 0 else " (без ATR)"
    return f"{window_phrase()}, цена {pl:.2f}–{ph:.2f}, до страйка ≥{min_dist():g}%{atr}"


def addon_short() -> str:
    if not addon_on():
        return "⏸ выключена"
    return f"✅ {levels_text()}, {addon_amount_text()} на уровень"


def live_blocked(now: float | None = None) -> str | None:
    """Почему реальная ставка 15m сейчас нельзя (None — можно): 15m 🧪, нет ключа, бот в DRY RUN, дневной стоп."""
    if not live_on():
        return "15m 🧪 — только тест"
    return lb.live_blocked(now)


def mode_text() -> str:
    """«🔴 реальные $5» / «🔴 реальные 5% банка — сейчас не идут: бот в DRY RUN» / «🧪 только тест»."""
    if not live_on():
        return "🧪 только тест"
    why = lb.live_blocked()
    amount = f"{stake_pct():g}% банка" if stake_mode() == "pct" else f"${stake_fixed():g}"
    return f"🔴 реальные {amount}" + (f" — сейчас не идут: {why}" if why else "")


def slug_for(asset: str, start: int) -> str:
    return f"{asset}-updown-{LABEL}-{start}"


# --------------------------------------------------------------- окна ----

def _task_done(task: asyncio.Task) -> None:
    _tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        log.warning("late15: фоновая задача упала: %s", task.exception())


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _tasks.add(task)
    task.add_done_callback(_task_done)


async def drain() -> None:
    while _tasks:
        await asyncio.gather(*list(_tasks), return_exceptions=True)


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
    storage.w15_upsert(w.asset, w.start, slug=w.slug)
    _supported.add(w.asset)
    _fails[w.asset] = 0
    _stats["windows"] += 1
    return w


def _maybe_open(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _opening or now < _retry_at.get(key, 0):
        return
    _opening.add(key)
    _spawn(_open(asset, start, now))


async def _open(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            _retry_at[key] = now + (lb.LATE_RETRY_SEC if now - start > 60 else lb.DISCOVERY_RETRY_SEC)
            lb._prune(_retry_at, now)
            return
        cur = _cur.get(asset)
        if not on() or _last_start.get(asset) != start or (cur is not None and cur.start == start):
            _release(w)
            return
        _cur[asset] = _activate(w)
    finally:
        _opening.discard(key)


def _release(w: Win) -> None:
    if not w.own_tokens:
        return
    cur = _cur.get(w.asset)
    busy = {cur.up, cur.down} if cur is not None and cur is not w else set()
    toks = [t for t in (w.up, w.down) if t not in busy]
    if toks:
        book_stream.unsubscribe(toks)


def _maybe_prefetch(asset: str, start: int, now: float) -> None:
    key = (asset, start)
    if key in _prefetching or now < _prefetch_at.get(key, 0):
        return
    _prefetching.add(key)
    _prefetch_at[key] = now + lb.PREFETCH_RETRY_SEC
    lb._prune(_prefetch_at, now)
    _spawn(_prefetch(asset, start, now))


async def _prefetch(asset: str, start: int, now: float) -> None:
    try:
        w = await _fetch_win(asset, start, now)
        if w is None:
            return
        old, cur = _next.get(asset), _cur.get(asset)
        if not on() or (old is not None and old.start == start) or (cur is not None and cur.start == start):
            _release(w)
            return
        if old is not None:
            _release(old)
        _next[asset] = w
    finally:
        _prefetching.discard((asset, start))


def _flush(w: Win) -> None:
    storage.w15_upsert(w.asset, w.start, slug=w.slug, path=json.dumps(w.path, separators=(",", ":")))


def _sample(w: Win, now: float) -> None:
    """Страйк и точка пути — тот же формат, что у 5m (11 полей)."""
    t_left = w.end - now
    if w.strike is None and now >= w.start:
        s = chainlink_feed.price_after(w.asset, w.start)
        if s is None and now >= w.start + lb.STRIKE_WAIT_SEC:
            s = chainlink_feed.price_at(w.asset, w.start)
        if s is not None:
            w.strike = s
            _stats["striked"] += 1
            storage.w15_upsert(w.asset, w.start, strike=lb._num(s))
    step = max(0.5, float(settings.REC15_STEP_SEC))
    if settings.REC_TO_SEC <= t_left <= settings.REC15_FROM_SEC and w.last_rec_t - t_left >= step - 0.5:
        bu, bus, ua, uas = book_stream.top_of_book(w.up)
        bd, bds, da, das = book_stream.top_of_book(w.down)
        price, age = chainlink_feed.latest(w.asset, now)
        if price is not None and age is not None and age > settings.CHAINLINK_MAX_AGE_SEC:
            price = None
        w.path.append([round(t_left, 1), ua, da, lb._num(price), lb._num(atr_feed.get(w.asset, now), 6),
                       bu, bd, lb._num(uas, 6), lb._num(das, 6), lb._num(bus, 6), lb._num(bds, 6)])
        w.last_rec_t = t_left
    if w.end - lb.BOOK_WINDOW_SEC <= now < w.end:
        w.book_res, w.book_res_t = lb.book_winner(w.up, w.down), now
    if now - w.last_flush >= lb.FLUSH_EVERY_SEC:
        w.last_flush = now
        _flush(w)


def _close(w: Win) -> None:
    _flush(w)
    if w.own_tokens:
        book_stream.unsubscribe([w.up, w.down])
    _pending.append(w)


def _shutdown() -> None:
    """⏸ Выключили: дописываем начатые окна (исход придёт сам), рынки следующих окон отпускаем."""
    for w in list(_cur.values()):
        _close(w)
    for w in list(_next.values()):
        _release(w)
    _cur.clear()
    _next.clear()
    _last_start.clear()


def resolve_pending(now: float | None = None) -> int:
    """Ранний исход закончившихся окон: стакан последних секунд, иначе Chainlink конец vs страйк."""
    now = time.time() if now is None else now
    done = 0
    for w in list(_pending):
        if now < w.end:
            continue
        book = w.book_res if w.book_res_t >= w.end - lb.BOOK_LAST_SEC else None
        if book and not w.early_set:
            w.early_set = True
            storage.w15_upsert(w.asset, w.start, early_outcome=book)
            done += 1
        cl_end = chainlink_feed.price_after(w.asset, w.end)
        if cl_end is None and now < w.end + lb.EARLY_WAIT_SEC:
            continue
        _pending.remove(w)
        if not w.early_set and w.strike and cl_end:
            w.early_set = True
            storage.w15_upsert(w.asset, w.start, early_outcome="UP" if cl_end >= w.strike else "DOWN",
                               cl_end=lb._num(cl_end))
            done += 1
        elif cl_end:
            storage.w15_upsert(w.asset, w.start, cl_end=lb._num(cl_end))
    _stats["early"] += done
    return done


async def settle(now: float | None = None) -> int:
    """Официальные исходы закончившихся окон 15m (Gamma)."""
    now = time.time() if now is None else now
    n = 0
    for asset, start, slug in storage.w15_unresolved(int(now - lb.SETTLE_LAG_SEC), int(now - lb.SETTLE_MAX_AGE_SEC)):
        try:
            res = await market_discovery.get_resolution(slug)
        except Exception:  # noqa: BLE001
            res = None
        if res in ("UP", "DOWN"):
            storage.w15_upsert(asset, start, outcome=res)
            _stats["official"] += 1
            n += 1
        await asyncio.sleep(lb.REQUEST_PAUSE_SEC)
    return n


# --------------------------------------------------------------- тик ----

def _new_window(asset: str, start: int, now: float) -> None:
    prev = _last_start.get(asset)
    _last_start[asset] = start
    if prev is None:
        return
    cur = _cur.get(asset)
    if cur is None or cur.start != prev:
        _fails[asset] += 1
        if (_fails[asset] >= lb.UNSUPPORTED_AFTER and asset not in _supported
                and now >= _skip_until.get(asset, 0)):
            _skip_until[asset] = now + lb.UNSUPPORTED_SKIP_SEC
            log.info("%s: рынков 15m нет %d окон подряд — пропускаю %d ч", asset, _fails[asset],
                     lb.UNSUPPORTED_SKIP_SEC // 3600)


def _tick_current(asset: str, start: int, now: float) -> None:
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
            _maybe_open(asset, start, now)
            return
        w = _activate(_next.pop(asset))
        _cur[asset] = w
    _sample(w, now)


def _tick_next(asset: str, nxt: int, now: float) -> None:
    if nxt - now > lb.PREFETCH_SEC:
        return
    nw = _next.get(asset)
    if nw is None or nw.start != nxt:
        _maybe_prefetch(asset, nxt, now)


async def tick(now: float | None = None) -> None:
    now = time.time() if now is None else now
    if not on():
        if _cur or _next:
            _shutdown()
        resolve_pending(now)
        return
    start = int(now // STEP) * STEP
    for asset in settings.ASSETS:
        if _last_start.get(asset) != start:
            _new_window(asset, start, now)
        if now < _skip_until.get(asset, 0):
            continue
        _tick_current(asset, start, now)
        _tick_next(asset, start + STEP, now)
    resolve_pending(now)
    maybe_trade(now)
    maybe_addons(now)


# ------------------------------------------------------------ ставка 15m ----

def evaluate(w: Win, now: float) -> tuple[bool, dict]:
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
    elif not book_stream.is_fresh(token, lb.BOOK_MAX_AGE_MS):
        info["why"] = "стакан не обновлялся 30 с"
    elif not lo - 1e-9 <= ask <= hi + 1e-9:
        info["why"] = f"цена вне {lo:.2f}–{hi:.2f}"
    elif dist < min_dist() - 1e-12:
        info["why"] = f"до страйка меньше {min_dist():g}%"
    elif min_atr() > 0 and atr is None:
        info["why"] = "нет ATR (Binance)"
    elif min_atr() > 0 and datr < min_atr() - 1e-9:
        info["why"] = f"меньше {min_atr():g} ATR от страйка"
    else:
        return True, info
    return False, info


def _cap(ask: float) -> float:
    return min(round(ask + lb.PRICE_SLIP, 2), round(price_range()[1] + lb.PRICE_SLIP, 2))


def _enter(w: Win, info: dict, now: float) -> int:
    """Тестовая ставка по стакану (всегда); 15m 🔴 — ещё и реальная (в фоне, чтобы не держать тик)."""
    ask = info["ask"]
    cap = _cap(ask)
    usdc = bet_usdc(cap)
    price, spent = lb.dry_fill(info["token"], cap, usdc)
    if price is None or spent < lb.MIN_TRADE_USDC:
        w.why = "мало объёма в стакане"
        return 0
    w.decided = True
    base = dict(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"], token_id=info["token"],
                t_left=round(w.end - now, 1), price_now=lb._num(info["price_now"]), strike=lb._num(w.strike),
                dist_pct=round(info["dist"], 4), dist_atr=round(info["datr"], 3) if info["datr"] else None,
                atr=lb._num(info["atr"], 6), ask=ask, rule=rule_tag())
    tid = storage.t15_log(**base, price=price, size_usdc=spent, shares=round(spent / price, 4),
                          fee_usdc=lb.fee_for(spent, price), dry_run=1, status="DRY")
    if tid and addon_on():
        _arm_addon(w, info, now)
    if live_on() and not storage.t15_exists(w.asset, w.start, dry_run=False):
        _spawn(_enter_live(w, info, base, cap, usdc, now))
    return 1 if tid else 0


def maybe_trade(now: float | None = None) -> int:
    now = time.time() if now is None else now
    hi_t, lo_t = window()
    start = int(now // STEP) * STEP
    done = 0
    for asset in coins():
        w = _cur.get(asset)
        if w is None or w.start != start or w.decided:
            continue
        t_left = w.end - now
        if t_left > hi_t + 1e-9:
            continue
        if t_left < lo_t - 1e-9:
            w.decided = True
            if w.why:
                _why[asset][w.why] += 1
            continue
        if not w.checked:
            w.checked = True
            if storage.t15_exists(asset, start, dry_run=True):
                w.decided = True                  # после перезапуска в том же окне — не дублируем
                continue
        ok, info = evaluate(w, now)
        if not ok:
            w.why = info["why"]
            continue
        done += _enter(w, info, now)
    return done


def _wallet_target(usdc: float) -> tuple[float, bool]:
    """Сколько реально ставить: сумма, но не больше баланса кошелька. (сумма, урезал ли кошелёк)."""
    wallet = lb._bank["wallet"]
    if wallet is not None and usdc > wallet:
        return max(0.0, round(float(wallet) - 0.01, 2)), True
    return round(usdc, 2), False


async def _enter_live(w: Win, info: dict, base: dict, cap: float, usdc: float, now: float) -> None:
    """Реальная ставка 15m: FAK не дороже ask + 1¢. Не исполнилось (или частично) — снова через LIVE_RETRY_SEC,
    пока идёт окно входа и условия держатся, добирая до суммы. В Telegram — итог покупки."""
    why = live_blocked(now)
    if why:
        _live_note.update(why=why, ts=now)
        day = int(now // 86400)
        if why.startswith("дневной стоп") and _trade_note["stop_day"] != day:
            _trade_note["stop_day"] = day
            await lb._notify(f"⏱15 Реальные ставки 15m остановлены до конца дня — {why}. Тестовые идут дальше.")
        return
    from src import polymarket_client
    target, by_wallet = _wallet_target(usdc)
    side, token = info["side"], info["token"]
    lo_t = window()[1]
    t0 = time.monotonic()
    spent = shares = 0.0
    oids: list[str] = []
    status = last = ""
    orders = errors = 0
    first_fill = None
    cur_cap, cur_ask = cap, info["ask"]
    for i in range(lb.LIVE_MAX_TRIES):
        t_now = now + (time.monotonic() - t0)
        if i:
            if w.end - t_now < lo_t - 1e-9:
                last = last or "окно входа кончилось"
                break
            ok, again = evaluate(w, t_now)
            if not ok or again["side"] != side:
                last = again.get("why") or "сторона сменилась"
                await asyncio.sleep(lb.LIVE_RETRY_SEC)
                continue
            cur_ask = again["ask"]
            cur_cap = _cap(cur_ask)
        left = round(target - spent, 2)
        min_amt = max(lb.MIN_TRADE_USDC, round(lb.MIN_ORDER_SHARES * cur_cap + 0.01, 2))
        if left < min_amt:
            if not spent and by_wallet:
                last = f"на балансе ${target + 0.01:.2f} — меньше минимального ордера ${min_amt:.2f}"
                await lb._notify_error(w.asset, RuntimeError(f"15m: на балансе ${target + 0.01:.2f} — меньше "
                                                             "ставки, реальная не ушла"), key="15m:wallet")
            break
        orders += 1
        try:
            res = await lb._live_buy(token, cur_cap, left)
        except Exception as exc:  # noqa: BLE001
            last = f"ошибка: {exc}"[:200]
            errors += 1
            if polymarket_client.is_fatal_error(exc) or errors >= lb.LIVE_MAX_ERRORS:
                await lb._notify_error(w.asset, exc)
                break
            await asyncio.sleep(lb.LIVE_RETRY_SEC)
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
                break
            last = f"частично: ${spent:.2f} из ${target:.2f}"
        await asyncio.sleep(lb.LIVE_RETRY_SEC)
    note = f"ордеров {orders}" + (f"; {last}" if last and spent < target - 0.01 else "")
    if spent <= 0:
        _live_note.update(why=last or "не исполнилась", ts=now)
        storage.t15_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                        status="SKIP_NOFILL", note=note[:300])
        return
    price = round(spent / shares, 4) if shares > 0 else cur_cap
    t_fill, ask_fill = first_fill
    storage.t15_log(**dict(base, ts=int(t_fill), t_left=round(w.end - t_fill, 1), ask=ask_fill), price=price,
                    size_usdc=round(spent, 2), shares=round(shares, 4), fee_usdc=lb.fee_for(spent, price),
                    dry_run=0, status=status or "matched", order_id=",".join(oids)[:500], note=note[:300])
    _spawn(lb.refresh_bank(force=True))
    await lb._notify(f"✅ ⏱15 {w.asset.upper()}: ставка 15m {side} ${spent:.2f} по {price:.3f} за "
                     f"{lb.mmss(w.end - t_fill)} до конца (до страйка {info['dist']:.2f}%"
                     + (f", {info['datr']:.1f} ATR" if info["datr"] else "") + ")")


# ------------------------------------------------------- ➕ докупка 15m ----

def _arm_addon(w: Win, info: dict, now: float) -> None:
    """После ставки: какие уровни ждут отката — только ниже цены входа. В базу — строка ARMED (уровень 0)."""
    entry = float(info["ask"])
    levels = [lv for lv in addon_levels() if round(lv, 2) < round(entry, 2)]
    w.addon = {"side": info["side"], "token": info["token"], "entry": entry, "left": levels, "lock": None}
    storage.a15_log(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"],
                    token_id=info["token"], level=0.0, entry_ask=entry, t_left=round(w.end - now, 1), ask=entry,
                    size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=1, status="ARMED", rule=rule_tag(),
                    note=f"уровни {levels_text(levels)}" if levels else "все уровни не ниже цены входа")


def addon_live_why(w: Win, now: float | None = None) -> str | None:
    """Почему реальная докупка 15m по окну не идёт (None — можно)."""
    if not addon_on():
        return "докупка 15m выключена"
    why = live_blocked(now)
    if why:
        return why
    real = storage.t15_get(w.asset, w.start, dry_run=False)
    if not real or not (real["size_usdc"] or 0) > 0:
        return "по окну нет реальной ставки 15m"
    return None


def maybe_addons(now: float | None = None) -> int:
    """➕ По окнам со ставкой 15m: ask нашей стороны опустился до уровня — докупка не дороже него. Тестовая —
    сразу, по стакану; реальная (15m 🔴 и есть реальная ставка) — в фоне. Возвращает число тестовых докупок."""
    now = time.time() if now is None else now
    start = int(now // STEP) * STEP
    done = 0
    for w in list(_cur.values()):
        ad = w.addon
        if not ad or not ad["left"] or w.start != start:
            continue
        if w.end - now < lb.ADDON_MIN_LEFT_SEC:
            ad["left"] = []                                # последние секунды — без докупки
            continue
        if not addon_on():
            continue
        token = ad["token"]
        ask = book_stream.best_ask(token)
        if ask is None or not book_stream.is_fresh(token, lb.BOOK_MAX_AGE_MS):
            continue
        hit = [lv for lv in ad["left"] if ask <= lv + 1e-9]
        if not hit:
            continue
        amt = addon_usd()
        used: dict = {}
        live_levels = []
        for lv in hit:
            price, spent = lb.dry_fill(token, lv, amt, used)
            if price is None or spent < lb.MIN_DRY_ADDON_USDC:
                continue                                   # объём съели уровни выше — на следующем тике
            ad["left"].remove(lv)
            base = dict(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=ad["side"], token_id=token,
                        level=lv, entry_ask=ad["entry"], t_left=round(w.end - now, 1), ask=ask, rule=rule_tag())
            if storage.a15_log(**base, price=price, size_usdc=spent, shares=round(spent / price, 4),
                               fee_usdc=lb.fee_for(spent, price), dry_run=1, status="DRY"):
                done += 1
            if live_on():
                live_levels.append((lv, base))
        if live_levels:
            why = addon_live_why(w, now)
            if why is None:
                for lv, base in live_levels:
                    _spawn(_addon_live(w, base, lv, now))
            else:
                _live_note.update(why=f"докупка: {why}", ts=now)
    return done


async def _addon_live(w: Win, base: dict, lv: float, now: float) -> None:
    """Реальная докупка 15m на уровне lv. Уровни одного окна — по очереди (кошелёк — с учётом купленного)."""
    t_spawn = time.monotonic()
    ad = w.addon if w.addon is not None else {}
    if ad.get("lock") is None:
        ad["lock"] = asyncio.Lock()
    async with ad["lock"]:
        await _addon_live_run(w, base, lv, now, t_spawn)


async def _addon_live_run(w: Win, base: dict, lv: float, now: float, t_spawn: float) -> None:
    """FAK не дороже уровня на $ уровня (не меньше 5 акций, не больше кошелька). Цену перебили — ждём возврата к
    уровню до ADDON_LIVE_WAIT_SEC, не больше ADDON_LIVE_TRIES ордеров. Не купилось — строка SKIP_… с причиной."""
    from src import polymarket_client

    def t_now() -> float:
        return now + (time.monotonic() - t_spawn)

    why = addon_live_why(w, t_now())                      # пока ждали очереди, могли выключить
    if why:
        _live_note.update(why=f"докупка: {why}", ts=t_now())
        return
    token, side, a = base["token_id"], base["side"], w.asset.upper()
    min_amt = max(lb.MIN_TRADE_USDC, round(lb.MIN_ORDER_SHARES * lv + 0.01, 2))
    target, _by_wallet = _wallet_target(addon_live_usd(lv))
    if target < min_amt:
        reason = f"на балансе меньше минимального ордера ${min_amt:.2f}"
        _live_note.update(why=f"докупка: {reason}", ts=t_now())
        storage.a15_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                        status="SKIP_NOMONEY", note=reason)
        return
    spent = shares = 0.0
    oids: list[str] = []
    status = last = ""
    orders = errors = 0
    first_fill = None
    t_begin = time.monotonic()
    for _i in range(lb.LIVE_MAX_TRIES):
        if orders >= lb.ADDON_LIVE_TRIES or time.monotonic() - t_begin >= lb.ADDON_LIVE_WAIT_SEC:
            break
        tn = t_now()
        if w.end - tn < lb.ADDON_MIN_LEFT_SEC:
            last = last or f"до конца меньше {lb.ADDON_MIN_LEFT_SEC:g} с"
            break
        ask = book_stream.best_ask(token)
        if ask is None or ask > lv + 1e-9:
            last = f"цена выше {lv:.2f}"
            await asyncio.sleep(lb.LIVE_RETRY_SEC)
            continue
        left = round(target - spent, 2)
        if left < min_amt:
            break
        orders += 1
        try:
            res = await lb._live_buy(token, lv, left)
        except Exception as exc:  # noqa: BLE001
            last = f"ошибка: {exc}"[:200]
            errors += 1
            if polymarket_client.is_fatal_error(exc) or errors >= lb.LIVE_MAX_ERRORS:
                await lb._notify_error(w.asset, exc)
                break
            await asyncio.sleep(lb.LIVE_RETRY_SEC)
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
                break
            last = f"частично: ${spent:.2f} из ${target:.2f}"
        await asyncio.sleep(lb.LIVE_RETRY_SEC)
    note = f"ордеров {orders}" + (f"; {last}" if last and spent < target - 0.01 else "")
    if spent <= 0:
        storage.a15_log(**base, price=None, size_usdc=0.0, shares=0.0, fee_usdc=0.0, dry_run=0,
                        status="SKIP_NOFILL", note=note[:300])
        return
    price = round(spent / shares, 4) if shares > 0 else lv
    t_fill, ask_fill = first_fill
    storage.a15_log(**dict(base, ts=int(t_fill), t_left=round(w.end - t_fill, 1), ask=ask_fill), price=price,
                    size_usdc=round(spent, 2), shares=round(shares, 4), fee_usdc=lb.fee_for(spent, price),
                    dry_run=0, status=status or "matched", order_id=",".join(oids)[:500], note=note[:300])
    _spawn(lb.refresh_bank(force=True))
    await lb._notify(f"➕ ⏱15 {a}: докупка 15m {side} ${spent:.2f} по {price:.3f} (уровень {lv:.2f}) за "
                     f"{lb.mmss(w.end - t_fill)} до конца")


# -------------------------------------------------------------- итоги окна ----

def settle_text(t: dict, outcome: str, pnl: float) -> str:
    """Итог реальной ставки 15m для Telegram — с докупкой окна: реальной, а нет её — тестовой («было бы»)."""
    win = outcome == t["side"]
    s = f"⏱15 {'🟢' if win else '🔴'} {t['asset'].upper()} 15m: исход {outcome}, ставка {t['side']} → {pnl:+.2f} USDC"
    rows = storage.a15_window(t["asset"], t["start_ts"])
    real = [r for r in rows if not r["dry_run"] and lb._is_buy(r)]
    test = [r for r in rows if r["dry_run"] and lb._is_buy(r)]
    armed = any(r["status"] == "ARMED" for r in rows)
    if real:
        ap = sum(lb._row_pnl(r, outcome) for r in real)
        s += (f"\n➕ Докупка реально: {lb.buys_text(len(real))} на ${sum(r['size_usdc'] for r in real):.2f} "
              f"(уровни {levels_text([r['level'] for r in real])}) → {ap:+.2f}; итог окна {pnl + ap:+.2f} USDC")
    elif test:
        ap = sum(lb._row_pnl(r, outcome) for r in test)
        s += (f"\n➕ С тестовой докупкой было бы {pnl + ap:+.2f} USDC (докупка {ap:+.2f}: {lb.buys_text(len(test))} "
              f"на ${sum(r['size_usdc'] for r in test):.2f}, уровни {levels_text([r['level'] for r in test])})")
    elif armed:
        s += "\n➕ Докупки не было — цена не откатила до уровней ниже входа; итог тот же"
    return s


async def _official(t: dict, now: float, cache: dict) -> str | None:
    key = (t["asset"], t["start_ts"])
    if key in cache:
        return cache[key]
    o = storage.w15_outcome(*key)
    if o is None and now - (t["start_ts"] + STEP) >= lb.SETTLE_LAG_SEC:
        try:
            o = await market_discovery.get_resolution(t["slug"])
        except Exception:  # noqa: BLE001
            o = None
        await asyncio.sleep(lb.REQUEST_PAUSE_SEC)
    cache[key] = o
    return o


async def settle_trades(now: float | None = None) -> int:
    """Итоги ставок и ➕ докупок 15m по официальному исходу окна. Докупки — первыми: итог реальной ставки в
    Telegram показывает и докупку окна. Возвращает, сколько ставок закрыли."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for a in storage.a15_open(int(now - 30)):
        if a["start_ts"] < now - lb.SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(a, now, cache)
        if o in ("UP", "DOWN"):
            storage.a15_settle(a["id"], o, round(lb._row_pnl(a, o), 4))
    for t in storage.t15_open(int(now - 30)):
        if t["start_ts"] < now - lb.SETTLE_MAX_AGE_SEC:
            continue
        o = await _official(t, now, cache)
        if o not in ("UP", "DOWN"):
            continue
        skip = str(t["status"] or "").startswith("SKIP")
        pnl = 0.0 if skip else (t["shares"] if o == t["side"] else 0.0) - t["size_usdc"] - (t["fee_usdc"] or 0.0)
        if storage.t15_settle(t["id"], o, round(pnl, 4)):
            n += 1
            if not t["dry_run"] and not skip:
                await lb._notify(settle_text(t, o, pnl))
    return n


def trim_paths(now: float | None = None) -> int:
    days = int(settings.REC_KEEP_DAYS)
    if days <= 0:
        return 0
    now = time.time() if now is None else now
    return storage.w15_trim(int(now - days * 86400))


# ------------------------------------------------------------- итоги ----

def trade_stats(since_ts: int = 0, asset: str | None = None, side: str | None = None, live: bool = False) -> dict:
    rows = [t for t in storage.t15_since(since_ts) if bool(t["dry_run"]) != live
            and (asset is None or t["asset"] == asset) and (side is None or t["side"] == side)]
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    return {"bets": len(bets), "settled": len(settled),
            "wins": sum(1 for t in settled if t["outcome"] == t["side"]),
            "pnl": sum(t["pnl_usdc"] or 0.0 for t in settled),
            "staked": sum(t["size_usdc"] or 0.0 for t in settled),
            "avg_price": (sum(t["price"] for t in bets if t["price"]) / len(bets)) if bets else None}


def addon_stats(since_ts: int = 0, live: bool = False) -> dict:
    return lb._addon_stats([r for r in storage.a15_since(since_ts) if bool(r["dry_run"]) != live])


def recorder_line(now: float | None = None) -> str:
    now = time.time() if now is None else now
    rec = storage.w15_rows(int(now) - 86400)
    return (f"🔁 Запись 15m: окон за сутки {len(rec)} по {len({r['asset'] for r in rec})} монетам, со страйком "
            f"{sum(1 for r in rec if r['strike'])}, с исходом {sum(1 for r in rec if r['outcome'])}; путь с "
            f"{lb.mmss(min(settings.REC15_FROM_SEC, STEP))} до конца каждые {settings.REC15_STEP_SEC:g} с")


def status_text() -> str:
    if not on():
        return "⏸ выключено — окна 15m не пишутся, ставок нет"
    return "✅ пишет окна 15m и ставит — " + mode_text()


def _part(st: dict) -> str:
    if not st["bets"]:
        return "ставок нет"
    s = lb.bets_text(st["bets"])
    if st["settled"]:
        s += f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
    return s


def day_text(now: float | None = None, live: bool = False) -> str:
    now = time.time() if now is None else now
    return _part(trade_stats(int(now // 86400) * 86400, live=live))


def main_line() -> str:
    """Строка в главное меню."""
    if not on():
        return "⏱ 15m: ⏸ выключено (кнопка ⏱ 15m)"
    s = f"⏱ 15m: ✅ {mode_text()}; сегодня — тест {day_text()}"
    if live_on() or trade_stats(int(time.time() // 86400) * 86400, live=True)["bets"]:
        s += f"; реальные {day_text(live=True)}"
    return s


def _side_short(name: str, st: dict) -> str:
    if not st["bets"]:
        return f"{name} —"
    s = f"{name} {st['bets']}"
    if st["settled"]:
        s += f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
    return s


def why_lines() -> list[str]:
    out = []
    for a in coins():
        c = _why.get(a)
        if c:
            out.append(f"  {a.upper()}: " + ", ".join(f"{k} — {v}" for k, v in c.most_common(3)))
    return (["Окна без ставки (с запуска, главные причины):"] + out) if out else []


def stats_lines() -> list[str]:
    """Блок в 📊: коротко."""
    lines = [f"⏱ 15m: {'✅ вкл' if on() else '⏸ выкл'}, {mode_text()}; {rule_short()}; ставка {stake_text()}",
             "  " + lb.stats_line("Тест 15m всего", trade_stats(0)),
             "  Тест 15m сегодня (UTC): " + day_text()]
    if live_on() or trade_stats(0, live=True)["bets"]:
        lines += ["  " + lb.stats_line("Реальные 15m всего", trade_stats(0, live=True)),
                  "  Реальные 15m сегодня (UTC): " + day_text(live=True)]
    lines.append(f"  ➕ Докупка 15m: {addon_short()}")
    lines.append("  " + lb.addon_line("Тест докупки 15m", addon_stats(0)))
    if live_on() or addon_stats(0, live=True)["bets"]:
        lines.append("  " + lb.addon_line("Реальная докупка 15m", addon_stats(0, live=True)))
    lines.append("  " + recorder_line())
    if _stats["discovery_errors"]:
        lines.append(f"  Поиск рынков 15m: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return lines


def screen_text(head: str = "") -> str:
    """Экран ⏱ 15m."""
    lines = [head + "⏱ 15m — ставки на 15-минутных рынках (правило по архиву 15m) и запись окон.", "",
             "Статус: " + status_text(),
             f"Правило: {rule_short()}; обе стороны; одна ставка на окно",
             f"Сумма: {stake_text()} — тест и реальная, не меньше 5 акций",
             f"➕ Докупка 15m: {addon_short()} — тестовая всегда, реальная — если по окну есть реальная ставка",
             "Монеты: " + (", ".join(a.upper() for a in coins()) or "нет") + "; окна пишутся по всем: "
             + ", ".join(a.upper() for a in settings.ASSETS),
             recorder_line(), "",
             lb.stats_line("Тест 15m всего", trade_stats(0)),
             "Тест сегодня (UTC): " + day_text()]
    live_st = trade_stats(0, live=True)
    if live_on() or live_st["bets"]:
        lines += [lb.stats_line("Реальные 15m всего", live_st), "Реальные сегодня (UTC): " + day_text(live=True)]
    up, dn = trade_stats(0, side="UP"), trade_stats(0, side="DOWN")
    lines.append("Тест по сторонам: " + _side_short("UP", up) + " | " + _side_short("DOWN", dn))
    lines.append(lb.addon_line("Тест докупки 15m", addon_stats(0)))
    if live_on() or addon_stats(0, live=True)["bets"]:
        lines.append(lb.addon_line("Реальная докупка 15m", addon_stats(0, live=True)))
    if live_on() and _live_note["why"] and time.time() - _live_note["ts"] < 3 * 3600:
        lines.append(f"⚠️ Последняя реальная 15m не пошла: {_live_note['why']}")
    w = why_lines()
    if w:
        lines += [""] + w
    if _stats["discovery_errors"]:
        lines.append(f"Поиск рынков 15m: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    lines += ["", "🔴 Реальные ставки 15m идут, только когда 15m 🔴 (кнопка ниже) и бот в 🔴 LIVE; дневной стоп — "
                  "общий с 5m. Файлы 15m — в отчётах (*_windows15.zip, *_trades15.csv, *_addons15.csv) и по 📤."]
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    rec = storage.w15_rows(since_ts)
    lines = [f"🔁 Окна 15m {from_label} → {to_label} (UTC): {len(rec)}, со страйком "
             f"{sum(1 for r in rec if r['strike'])}, с официальным исходом {sum(1 for r in rec if r['outcome'])}",
             lb.stats_line("⏱ Тест 15m за период", trade_stats(since_ts))]
    real = trade_stats(since_ts, live=True)
    if real["bets"]:
        lines.append(lb.stats_line("⏱ Реальные 15m за период", real))
    up, dn = trade_stats(since_ts, side="UP"), trade_stats(since_ts, side="DOWN")
    if up["bets"] or dn["bets"]:
        lines.append("По сторонам (тест): " + _side_short("UP", up) + " | " + _side_short("DOWN", dn))
    return "\n".join(lines)


def trades_caption() -> str:
    lines = [f"⏱ Ставки 15m ({mode_text()}): {rule_short()}; ставка {stake_text()}",
             lb.stats_line("Тест 15m всего", trade_stats(0)),
             "Тест сегодня (UTC): " + day_text()]
    if trade_stats(0, live=True)["bets"]:
        lines += [lb.stats_line("Реальные 15m всего", trade_stats(0, live=True)),
                  "Реальные сегодня (UTC): " + day_text(live=True)]
    return "\n".join(lines)


def addons_caption(since_ts: int = 0) -> str:
    lines = [f"➕ Докупки 15m: {addon_short()}", lb.addon_line("Тест докупки 15m", addon_stats(since_ts))]
    if addon_stats(since_ts, live=True)["bets"]:
        lines.append(lb.addon_line("Реальная докупка 15m", addon_stats(since_ts, live=True)))
    return "\n".join(lines)


# ---------------------------------------------------------- цикл ----

async def _tick_loop() -> None:
    last_trim = 0.0
    while True:
        try:
            await tick()
            if time.monotonic() - last_trim >= lb.TRIM_EVERY_SEC:
                last_trim = time.monotonic()
                trim_paths()
        except Exception as exc:  # noqa: BLE001 — 15m не должен ронять бота
            log.warning("late15: %s", exc)
        await lb._sleep_to_next_second()


async def _settle_loop() -> None:
    while True:
        await asyncio.sleep(lb.SETTLE_EVERY_SEC)
        try:
            await settle()
            await settle_trades()
        except Exception as exc:  # noqa: BLE001
            log.warning("late15 settle: %s", exc)


async def run_forever() -> None:
    await asyncio.sleep(5)
    await asyncio.gather(_tick_loop(), _settle_loop())
