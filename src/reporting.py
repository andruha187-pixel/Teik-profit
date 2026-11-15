"""
Отчёт раз в REPORT_INTERVAL_HOURS — два файла в Telegram:
  *_windows.zip — окна 5m всех монет (и выключенных тоже): страйк, путь цены
                  [секунд до конца, ask UP, ask DOWN, цена Chainlink, ATR] и исходы;
                  CSV внутри zip — путь занимает много места;
  *_trades.csv  — ставки ⏱ (тестовые и реальные);
  *_addons.csv  — ➕ докупки на откате (тестовые и реальные), если за период были;
  *_windows15.zip, *_trades15.csv — ⏱15 окна 15m (тот же формат, путь с 15:00 до конца)
                  и тестовые ставки 15m, если за период были.
В каждом — период и 30 минут прошлого (окна за это время уже с исходами).
Кнопка 📤 Файлы — всё, что в базе: окна по дням, частями до ~40 МБ.
Момент последнего отчёта хранится в bot_settings — переживает рестарт.
"""
from __future__ import annotations
import asyncio
import csv
import io
import logging
import os
import time
import zipfile

from config import settings
from src import late15, late_bot, storage, telegram_notify

log = logging.getLogger("reporting")

_LAST_REPORT_KEY = "last_report_ts"
OVERLAP_SEC = 30 * 60
DAY = 86400
# Telegram отклоняет подпись к файлу длиннее 1024 символов (считает в UTF-16)
CAPTION_LIMIT = 1024
# Бот может прислать файл до 50 МБ — режем окна на части
ZIP_PART_LIMIT = 40 * 1024 * 1024


def _caption_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _fit_caption(text: str, limit: int = CAPTION_LIMIT) -> str:
    if _caption_len(text) <= limit:
        return text
    while text and _caption_len(text) > limit - 1:
        text = text[:-1]
    return text + "…"


def _csv_bytes(columns: list[str], rows: list[tuple]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _write_csv(path: str, columns: list[str], rows: list[tuple]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        f.write(_csv_bytes(columns, rows))


def zip_windows(base: str, since_ts: int, until_ts: int, export=None, kind: str = "windows") -> tuple[list[str], int]:
    """Окна [since_ts, until_ts) — в zip (CSV по UTC-дням; новая часть, если зип больше
    ZIP_PART_LIMIT). Возвращает (пути к zip, сколько окон). export/kind — для окон 15m
    (storage.w15_export, «windows15»); по умолчанию — окна 5m."""
    export = storage.windows_export if export is None else export
    os.makedirs(os.path.dirname(base) or ".", exist_ok=True)
    paths: list[str] = []
    n = 0
    zf = None
    tag = os.path.basename(base)
    t = int(since_ts)
    try:
        while t < until_ts:
            t_end = min(int(until_ts), (t // DAY + 1) * DAY)
            rows = export(t, t_end)
            if rows:
                if zf is None:
                    path = f"{base}_{kind}" + (f"_{len(paths) + 1}" if paths else "") + ".zip"
                    zf = zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED)
                    paths.append(path)
                zf.writestr(f"{tag}_{kind}_{time.strftime('%Y%m%d', time.gmtime(t))}.csv",
                            _csv_bytes(storage.WINDOW_COLUMNS, rows))
                n += len(rows)
                if sum(i.compress_size for i in zf.infolist()) > ZIP_PART_LIMIT:
                    zf.close()
                    zf = None
            t = t_end
    except Exception:
        if zf is not None:
            zf.close()
            zf = None
        for p in paths:
            try:
                os.remove(p)
            except OSError:
                pass
        raise
    finally:
        if zf is not None:
            zf.close()
    return paths, n


def _files15(base: str, captions: dict, rows_since: int, until_ts: int, files: list) -> int:
    """⏱15 окна 15m (zip) и тестовые ставки 15m (CSV) — в список файлов. Ошибка здесь не мешает файлам 5m.
    Захват прошлого на 15 минут больше, чем у 5m: окно 15m длиннее, его исход приходит позже."""
    n = 0
    rows_since = max(0, int(rows_since) - storage.SPAN15)
    try:
        zips, n_win = zip_windows(base, rows_since, until_ts, storage.w15_export, "windows15")
        for i, path in enumerate(zips):
            cap = captions.get("windows15", "🔁 Окна 15m") + (f"\nЧасть {i + 1} из {len(zips)}." if len(zips) > 1 else "")
            files.append((path, cap))
        n += n_win
        t15 = storage.t15_export(rows_since)
        if t15:
            path = f"{base}_trades15.csv"
            _write_csv(path, storage.TRADE_COLUMNS, t15)
            files.append((path, captions.get("trades15") or "⏱ Тест 15m"))
            n += len(t15)
    except Exception as exc:  # noqa: BLE001
        log.warning("Файлы 15m не собрались: %s", exc)
    return n


async def _send(base: str, captions: dict, since_ts: int, rows_since: int, until_ts: int) -> int:
    """Окна (zip), ставки и докупки (CSV) с rows_since, затем окна и тест 15m. Возвращает число строк."""
    files: list[tuple[str, str]] = []
    trades = addons = []
    n15 = 0
    try:
        zips, n_win = zip_windows(base, rows_since, until_ts)
        for i, path in enumerate(zips):
            cap = captions["windows"] + (f"\nЧасть {i + 1} из {len(zips)}." if len(zips) > 1 else "")
            files.append((path, cap))
        trades = storage.trades_export(rows_since)
        if trades:
            path = f"{base}_trades.csv"
            _write_csv(path, storage.TRADE_COLUMNS, trades)
            files.append((path, captions["trades"]))
        addons = storage.addons_export(rows_since)
        if addons:
            path = f"{base}_addons.csv"
            _write_csv(path, storage.ADDON_COLUMNS, addons)
            files.append((path, captions.get("addons") or "➕ Докупки"))
        n15 = _files15(base, captions, rows_since, until_ts, files)
        for path, caption in files:
            await telegram_notify.send_document(path, _fit_caption(caption))
    finally:
        for path, _cap in files:
            try:
                os.remove(path)
            except OSError:
                pass
    return n_win + len(trades) + len(addons) + n15


def _get_last_report_ts() -> int:
    raw = storage.get_setting(_LAST_REPORT_KEY)
    try:
        return int(float(raw)) if raw else int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)
    except ValueError:
        return int(time.time()) - int(settings.REPORT_INTERVAL_HOURS * 3600)


def _set_last_report_ts(ts: int) -> None:
    storage.set_setting(_LAST_REPORT_KEY, ts)


def _label(ts: float) -> str:
    return time.strftime("%Y%m%d-%H%M", time.gmtime(ts))


async def build_and_send_report() -> int:
    since_ts = _get_last_report_ts()
    now_ts = int(time.time())
    from_label, to_label = _label(since_ts), _label(now_ts)
    base = os.path.join(settings.REPORTS_DIR, f"{from_label}_to_{to_label}")
    captions = {"windows": late_bot.report_caption(from_label, to_label, since_ts),
                "trades": "\n".join(late_bot.strategy_lines()),
                "addons": late_bot.addon_caption(f"➕ Докупки {from_label} → {to_label} (UTC), итоги за период:",
                                                 since_ts)}
    captions.update(_captions15(from_label, to_label, since_ts))
    n = await _send(base, captions, since_ts, max(0, since_ts - OVERLAP_SEC), now_ts + 1)
    _set_last_report_ts(now_ts)
    return n


def _captions15(from_label: str, to_label: str, since_ts: int) -> dict:
    try:
        return {"windows15": late15.report_caption(from_label, to_label, since_ts),
                "trades15": late15.trades_caption()}
    except Exception as exc:  # noqa: BLE001 — подпись 15m не должна мешать отчёту 5m
        log.warning("Подпись 15m: %s", exc)
        return {}


async def send_all() -> int:
    """Кнопка 📤: все окна и ставки из базы (и 15m)."""
    now_ts = int(time.time())
    to_label = _label(now_ts)
    firsts = [t for t in (storage.windows_first_ts(), storage.w15_first_ts()) if t is not None]
    since = min(firsts) if firsts else now_ts
    captions = {"windows": "🔁 Окна 5m — всё в базе (путь цены — за последние "
                           f"{settings.REC_KEEP_DAYS} дн.)\n" + late_bot.report_caption(_label(since), to_label, since),
                "trades": "\n".join(late_bot.strategy_lines()),
                "addons": late_bot.addon_caption("➕ Докупки — всё в базе:", 0)}
    captions.update(_captions15(_label(since), to_label, since))
    if "windows15" in captions:
        captions["windows15"] = (f"🔁 Окна 15m — всё в базе (путь цены — за последние {settings.REC_KEEP_DAYS} дн.)\n"
                                 + captions["windows15"])
    return await _send(os.path.join(settings.REPORTS_DIR, f"all_to_{to_label}"), captions, since, since, now_ts + 1)


async def report_loop() -> None:
    """Фоновая задача: отчёт раз в REPORT_INTERVAL_HOURS (момент последнего — в базе)."""
    interval = max(60.0, settings.REPORT_INTERVAL_HOURS * 3600)
    while True:
        wait = _get_last_report_ts() + interval - time.time()
        if wait > 0:
            await asyncio.sleep(min(wait, 300))
            continue
        try:
            await build_and_send_report()
        except Exception as exc:  # noqa: BLE001 — отчёт не должен ронять бота
            log.warning("Отчёт не ушёл: %s", exc)
            await asyncio.sleep(300)
