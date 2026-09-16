"""
Анализ истории сделок конкретного кошелька на Polymarket — реверс-инжиниринг
условий входа. Использует ПУБЛИЧНЫЙ API Polymarket (data-api.polymarket.com/
trades), не требует авторизации — это открытые ончейн-данные, которые сам
Polymarket публикует без ключа.

Идея: для каждой сделки этого кошелька в BTC-updown-15m восстанавливаем те
же метрики, которые считает наш собственный бот (strike price, ATR,
EMA, расхождение в ATR, сколько минут оставалось), плюс объём свечи —
чтобы проверить гипотезы вида "входит только когда цена далеко укатилась"
или "входит на всплеске объёма". Плюс сверяем с реальным исходом рынка,
чтобы независимо подтвердить заявленный винрейт.

Запуск:
    python -m scripts.analyze_wallet 0x4707b735acce66b2ccb5600086de2329685cd1ce
    python -m scripts.analyze_wallet 0x4707... --asset btc

Результат — CSV в data/reports/wallet_<address>_<asset>.csv, который можно
прислать обратно для разбора.
"""
from __future__ import annotations
import argparse
import asyncio
import csv
import os
import sys
import time

import httpx

sys.path.insert(0, ".")
from src import binance_feed, indicators, market_discovery  # noqa: E402

DATA_API = "https://data-api.polymarket.com"


async def fetch_all_trades(address: str, page_size: int = 500) -> list[dict]:
    """Публичный эндпоинт, без авторизации. Полная история сделок кошелька
    по всем рынкам — фильтруем по активу/таймфрейму уже локально."""
    trades = []
    offset = 0
    async with httpx.AsyncClient(timeout=20) as client:
        while True:
            resp = await client.get(f"{DATA_API}/trades", params={
                "user": address, "limit": page_size, "offset": offset, "takerOnly": "false",
            })
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break
            trades.extend(page)
            print(f"  ...загружено {len(trades)} сделок (всего по кошельку, до фильтра)")
            if len(page) < page_size:
                break
            offset += page_size
    return trades


def parse_slug_window(slug: str) -> tuple[int, int] | None:
    """'btc-updown-15m-1789270200' -> (1789270200, 1789271100)."""
    try:
        parts = slug.rsplit("-", 1)
        start = int(parts[1])
        return start, start + 15 * 60
    except (ValueError, IndexError):
        return None


async def get_indicators_at(symbol: str, at_ts: int, atr_period: int = 14, ema_fast: int = 9,
                             ema_slow: int = 21, atr_lookback: int = 60) -> dict | None:
    """Восстанавливаем срез индикаторов НА МОМЕНТ прошлой сделки — то же
    самое, что live-бот вычисляет на каждом тике, только задним числом."""
    lookback_minutes = atr_period + atr_lookback + 5
    start_ms = (at_ts - lookback_minutes * 60) * 1000
    try:
        df = await binance_feed.get_klines(symbol, limit=lookback_minutes + 5,
                                            interval="1m", start_time_ms=start_ms)
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠️ Не удалось получить свечи Binance для ts={at_ts}: {exc}")
        return None

    df = df[df["open_time"] <= at_ts * 1000]
    if df.empty or len(df) < atr_period + 2:
        return None

    snap = indicators.compute_indicator_snapshot(df, atr_period, ema_fast, ema_slow, atr_lookback)
    last_volume = float(df.iloc[-1]["volume"])
    avg_volume = float(df["volume"].tail(atr_lookback).mean())
    snap["volume"] = last_volume
    snap["avg_volume"] = avg_volume
    snap["volume_ratio"] = (last_volume / avg_volume) if avg_volume else None
    return snap


async def analyze(address: str, asset: str) -> str:
    print(f"Тяну историю сделок кошелька {address}...")
    all_trades = await fetch_all_trades(address)
    print(f"Всего сделок по кошельку (все рынки): {len(all_trades)}")

    prefix = f"{asset.lower()}-updown-15m-"
    relevant = [t for t in all_trades if str(t.get("slug", "")).startswith(prefix) and t.get("side") == "BUY"]
    print(f"Из них входов (BUY) в {asset.upper()} 15m: {len(relevant)}")

    if not relevant:
        print("Ничего не нашёл — либо кошелёк не торгует этот рынок, либо неверный формат слага.")
        return ""

    symbol = binance_feed.symbol_for(asset)
    rows = []
    resolution_cache: dict[str, str | None] = {}

    for i, trade in enumerate(sorted(relevant, key=lambda t: t["timestamp"])):
        slug = trade["slug"]
        ts = int(trade["timestamp"])
        window = parse_slug_window(slug)
        if window is None:
            continue
        start_time, end_time = window
        minutes_left = (end_time - ts) / 60

        print(f"  [{i+1}/{len(relevant)}] {slug} @ {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts))} UTC...")

        try:
            strike_price = await binance_feed.get_price_at(symbol, start_time)
        except Exception as exc:  # noqa: BLE001
            print(f"    ⚠️ strike недоступен: {exc}")
            continue

        snap = await get_indicators_at(symbol, ts)
        if snap is None:
            print("    ⚠️ индикаторы недоступны на этот момент — пропуск")
            continue

        distance_price = snap["close"] - strike_price
        distance_atr = abs(distance_price) / snap["atr"] if snap["atr"] else None

        if slug not in resolution_cache:
            try:
                resolution_cache[slug] = await market_discovery.get_resolution(slug)
            except Exception:  # noqa: BLE001
                resolution_cache[slug] = None
        market_outcome = resolution_cache[slug]
        chosen_outcome = str(trade.get("outcome", "")).upper()
        won = (market_outcome == chosen_outcome) if market_outcome else None

        rows.append({
            "timestamp": ts,
            "datetime_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)),
            "slug": slug,
            "chosen_outcome": chosen_outcome,
            "entry_price_prob": trade.get("price"),
            "size_usdc": trade.get("size"),
            "minutes_left": round(minutes_left, 2),
            "strike_price": strike_price,
            "btc_price_at_entry": snap["close"],
            "distance_price": round(distance_price, 2),
            "atr": round(snap["atr"], 2),
            "distance_atr": round(distance_atr, 2) if distance_atr is not None else None,
            "atr_ratio_to_avg": round(snap["atr_ratio_to_avg"], 2),
            "ema_fast": round(snap["ema_fast"], 2),
            "ema_slow": round(snap["ema_slow"], 2),
            "trend_up": snap["trend_up"],
            "volume_last_candle": round(snap["volume"], 3),
            "avg_volume": round(snap["avg_volume"], 3),
            "volume_ratio": round(snap["volume_ratio"], 2) if snap["volume_ratio"] else None,
            "market_outcome": market_outcome,
            "won": won,
            "tx_hash": trade.get("transactionHash"),
        })

        await asyncio.sleep(0.15)  # не спамим Binance/Gamma API без нужды

    if not rows:
        print("Не удалось восстановить контекст ни для одной сделки.")
        return ""

    out_dir = "data/reports"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"wallet_{address.lower()}_{asset.lower()}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    wins = sum(1 for r in rows if r["won"] is True)
    losses = sum(1 for r in rows if r["won"] is False)
    unknown = sum(1 for r in rows if r["won"] is None)
    print(f"\nГотово: {out_path}")
    print(f"Побед: {wins} | Поражений: {losses} | Исход неизвестен: {unknown}")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Анализ сделок кошелька на Polymarket")
    parser.add_argument("address", help="Адрес кошелька (0x...)")
    parser.add_argument("--asset", default="btc", help="Актив (btc, eth, sol, ...), по умолчанию btc")
    args = parser.parse_args()
    asyncio.run(analyze(args.address, args.asset))


if __name__ == "__main__":
    main()
