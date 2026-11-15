"""
⏱15 15-минутные рынки Up/Down — ТОЛЬКО запись окон и тестовые ставки. Реальных ордеров здесь нет вовсе.

Зачем. Правило 5m-бота подобрано и проверено по архиву 5m. Своего архива 15m нет (в старых логах — ~11 дней BTC
и по ~2.5 дня у ETH, SOL, XRP), поэтому сначала копим его так же, как по 5m: по каждому окну каждой монеты (ASSETS) —
страйк (цена Chainlink на старте окна), путь [секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR, bid UP,
bid DOWN, объём ask UP, объём ask DOWN, объём bid UP, объём bid DOWN] каждые REC15_STEP_SEC секунд с REC15_FROM_SEC
до конца и исход (ранний — стакан / Chainlink, и официальный). По этому архиву потом подбираем для 15m время входа,
цену и пороги и проверяем на второй половине данных.

Тестовая ставка 15m (по стакану, как тест у 5m) — по монетам LATE15_COINS, по правилу LATE15_*: по умолчанию как у
5m-бота — за 3:30–3:20 до конца сторона по цене 0.96–0.97, цена Chainlink ушла от страйка на ≥0.10% и ≥2 ATR;
обе стороны, без своих порогов монет. Одна ставка на окно.

Отдельно от 5m во всём: свои таблицы (late15_windows, late15_trades) — в итоги 5m, банки монет и дневной стоп 15m
не попадает; свой цикл — ошибка здесь не задевает тик 5m. Вкл/выкл — кнопкой ⏱ 15m (LATE15_ON — по умолчанию).
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
from src import late_bot as lb          # общие помощники 5m: стакан, тестовая покупка, комиссия, тексты

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


def reset() -> None:
    for d in (_cur, _next, _prefetch_at, _retry_at, _fails, _skip_until, _last_start, _why):
        d.clear()
    _prefetching.clear()
    _opening.clear()
    _pending.clear()
    _supported.clear()
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0


# ---------------------------------------------------------- настройки ----

def on() -> bool:
    return bool(runtime_state.get("late15_on"))


def set_on(value: bool) -> None:
    runtime_state.set("late15_on", bool(value))


def coins() -> list[str]:
    """Монеты тестовых ставок 15m (из пишущихся ASSETS)."""
    want = [a.strip().lower() for a in (settings.LATE15_COINS or "").split(",") if a.strip()]
    return [a for a in settings.ASSETS if a in want]


def window() -> tuple[int, int]:
    a, b = int(settings.LATE15_FROM_SEC), int(settings.LATE15_TO_SEC)
    hi, lo = max(a, b), min(a, b)
    return max(hi, lo + 1), lo


def price_range() -> tuple[float, float]:
    a, b = float(settings.LATE15_MIN_PRICE), float(settings.LATE15_MAX_PRICE)
    return min(a, b), max(a, b)


def min_dist() -> float:
    return max(0.0, float(settings.LATE15_MIN_DIST_PCT))


def min_atr() -> float:
    return max(0.0, float(settings.LATE15_MIN_ATR))


def stake() -> float:
    return max(lb.MIN_TRADE_USDC, float(settings.LATE15_STAKE_USDC))


def rule_tag() -> str:
    hi, lo = window()
    pl, ph = price_range()
    return f"15m|{hi}-{lo}s|{pl:.2f}-{ph:.2f}|d{min_dist():g}%|atr{min_atr():g}"


def rule_short() -> str:
    """«за 3:20–3:30 до конца, цена 0.96–0.97, до страйка ≥0.1% и ≥2 ATR»."""
    hi, lo = window()
    pl, ph = price_range()
    atr = f" и ≥{min_atr():g} ATR" if min_atr() > 0 else " (без ATR)"
    return f"за {lb.mmss(lo)}–{lb.mmss(hi)} до конца, цена {pl:.2f}–{ph:.2f}, до страйка ≥{min_dist():g}%{atr}"


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


# ------------------------------------------------- тестовая ставка 15m ----

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
    """Только тест: покупка по стакану (как тестовая ставка 5m), реальных ордеров нет."""
    ask = info["ask"]
    cap = _cap(ask)
    usdc = max(stake(), round(lb.MIN_ORDER_SHARES * cap + 0.01, 2))
    price, spent = lb.dry_fill(info["token"], cap, usdc)
    if price is None or spent < lb.MIN_TRADE_USDC:
        w.why = "мало объёма в стакане"
        return 0
    w.decided = True
    tid = storage.t15_log(ts=int(now), asset=w.asset, start_ts=w.start, slug=w.slug, side=info["side"],
                          token_id=info["token"], t_left=round(w.end - now, 1), price_now=lb._num(info["price_now"]),
                          strike=lb._num(w.strike), dist_pct=round(info["dist"], 4),
                          dist_atr=round(info["datr"], 3) if info["datr"] else None, atr=lb._num(info["atr"], 6),
                          ask=ask, price=price, size_usdc=spent, shares=round(spent / price, 4),
                          fee_usdc=lb.fee_for(spent, price), dry_run=1, status="DRY", rule=rule_tag())
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
            if storage.t15_exists(asset, start):
                w.decided = True
                continue
        ok, info = evaluate(w, now)
        if not ok:
            w.why = info["why"]
            continue
        done += _enter(w, info, now)
    return done


async def settle_trades(now: float | None = None) -> int:
    """Итоги тестовых ставок 15m по официальному исходу окна (без сообщений в Telegram)."""
    now = time.time() if now is None else now
    n = 0
    cache: dict = {}
    for t in storage.t15_open(int(now - 30)):
        if t["start_ts"] < now - lb.SETTLE_MAX_AGE_SEC:
            continue
        key = (t["asset"], t["start_ts"])
        if key not in cache:
            o = storage.w15_outcome(*key)
            if o is None and now - (t["start_ts"] + STEP) >= lb.SETTLE_LAG_SEC:
                try:
                    o = await market_discovery.get_resolution(t["slug"])
                except Exception:  # noqa: BLE001
                    o = None
                await asyncio.sleep(lb.REQUEST_PAUSE_SEC)
            cache[key] = o
        o = cache[key]
        if o not in ("UP", "DOWN"):
            continue
        pnl = (t["shares"] if o == t["side"] else 0.0) - t["size_usdc"] - (t["fee_usdc"] or 0.0)
        if storage.t15_settle(t["id"], o, round(pnl, 4)):
            n += 1
    return n


def trim_paths(now: float | None = None) -> int:
    days = int(settings.REC_KEEP_DAYS)
    if days <= 0:
        return 0
    now = time.time() if now is None else now
    return storage.w15_trim(int(now - days * 86400))


# ------------------------------------------------------------- итоги ----

def trade_stats(since_ts: int = 0, asset: str | None = None, side: str | None = None) -> dict:
    rows = [t for t in storage.t15_since(since_ts)
            if (asset is None or t["asset"] == asset) and (side is None or t["side"] == side)]
    bets = [t for t in rows if not str(t["status"] or "").startswith("SKIP")]
    settled = [t for t in bets if t["outcome"]]
    return {"bets": len(bets), "settled": len(settled),
            "wins": sum(1 for t in settled if t["outcome"] == t["side"]),
            "pnl": sum(t["pnl_usdc"] or 0.0 for t in settled),
            "staked": sum(t["size_usdc"] or 0.0 for t in settled),
            "avg_price": (sum(t["price"] for t in bets if t["price"]) / len(bets)) if bets else None}


def recorder_line(now: float | None = None) -> str:
    now = time.time() if now is None else now
    rec = storage.w15_rows(int(now) - 86400)
    return (f"🔁 Запись 15m: окон за сутки {len(rec)} по {len({r['asset'] for r in rec})} монетам, со страйком "
            f"{sum(1 for r in rec if r['strike'])}, с исходом {sum(1 for r in rec if r['outcome'])}; путь с "
            f"{lb.mmss(min(settings.REC15_FROM_SEC, STEP))} до конца каждые {settings.REC15_STEP_SEC:g} с")


def status_text() -> str:
    return ("✅ пишет окна 15m и ставит тестовые ставки" if on()
            else "⏸ выключено — окна 15m не пишутся, тест не идёт")


def day_text(now: float | None = None) -> str:
    now = time.time() if now is None else now
    st = trade_stats(int(now // 86400) * 86400)
    if not st["bets"]:
        return "ставок нет"
    s = lb.bets_text(st["bets"])
    if st["settled"]:
        s += f", выигр. {st['wins']}/{st['settled']}, {st['pnl']:+.2f} $"
    return s


def main_line() -> str:
    """Строка в главное меню."""
    if not on():
        return "⏱ 15m: ⏸ выключено (только запись и тест — кнопка ⏱ 15m)"
    return f"⏱ 15m (только тест): ✅ пишет окна; сегодня — {day_text()}"


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
    lines = [f"⏱ 15m — только тест (реальных ставок нет): {'✅ вкл' if on() else '⏸ выкл'}; {rule_short()}",
             "  " + lb.stats_line("Тест 15m всего", trade_stats(0)),
             "  Тест 15m сегодня (UTC): " + day_text(),
             "  " + recorder_line()]
    if _stats["discovery_errors"]:
        lines.append(f"  Поиск рынков 15m: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    return lines


def screen_text(head: str = "") -> str:
    """Экран ⏱ 15m."""
    up, dn = trade_stats(0, side="UP"), trade_stats(0, side="DOWN")
    lines = [head + "⏱ 15m — только запись окон и тестовые ставки. Реальных ставок на 15m нет.", "",
             "Статус: " + status_text(),
             f"Тест: {rule_short()}; обе стороны, без своих порогов монет; ставка ${stake():g} (тест, по стакану)",
             "Монеты теста: " + (", ".join(a.upper() for a in coins()) or "нет") + "; окна пишутся по всем: "
             + ", ".join(a.upper() for a in settings.ASSETS),
             recorder_line(), "",
             lb.stats_line("Тест 15m всего", trade_stats(0)),
             "Сегодня (UTC): " + day_text(),
             "По сторонам: " + _side_short("UP", up) + " | " + _side_short("DOWN", dn)]
    per = [(a, trade_stats(0, asset=a)) for a in coins()]
    per = [_side_short(a.upper(), st) for a, st in per if st["bets"]]
    if per:
        lines.append("По монетам: " + "; ".join(per))
    w = why_lines()
    if w:
        lines += [""] + w
    if _stats["discovery_errors"]:
        lines.append(f"Поиск рынков 15m: ошибок {_stats['discovery_errors']}, последняя: {_stats['last_error']}")
    lines += ["", "Зачем: копим архив 15m (~2 недели). Потом по нему подберём для 15m время входа, цену и пороги, "
                  "проверим на второй половине данных — и тогда решим про реальные ставки. Файлы 15m приходят в "
                  "отчётах (*_windows15.zip, *_trades15.csv) и по кнопке 📤 Файлы."]
    return "\n".join(lines)


def report_caption(from_label: str, to_label: str, since_ts: int) -> str:
    rec = storage.w15_rows(since_ts)
    lines = [f"🔁 Окна 15m {from_label} → {to_label} (UTC): {len(rec)}, со страйком "
             f"{sum(1 for r in rec if r['strike'])}, с официальным исходом {sum(1 for r in rec if r['outcome'])}",
             lb.stats_line("⏱ Тест 15m за период", trade_stats(since_ts))]
    up, dn = trade_stats(since_ts, side="UP"), trade_stats(since_ts, side="DOWN")
    if up["bets"] or dn["bets"]:
        lines.append("По сторонам: " + _side_short("UP", up) + " | " + _side_short("DOWN", dn))
    return "\n".join(lines)


def trades_caption() -> str:
    return "\n".join([f"⏱ Тест 15m (реальных ставок нет): {rule_short()}; ставка ${stake():g}",
                      lb.stats_line("Тест 15m всего", trade_stats(0)),
                      "Сегодня (UTC): " + day_text()])


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
