import os
import time
import asyncio
import tempfile
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from decimal import Decimal

tmp = tempfile.mkdtemp(prefix="bc_live_tp_")
os.environ["DATA_DIR"] = tmp
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["LIVE_MASTER_ENABLE"] = "0"
os.environ["ALLOW_DOUBLE_LIVE"] = "0"
os.environ["POLYMARKET_PRIVATE_KEY"] = ""
os.environ["TAKE_PROFIT_USDC"] = "0.30"

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.TRADE_SYMBOLS == ["BTC", "XRP", "ETH"]
assert len(bot.STRATEGIES) == 6
for symbol in bot.TRADE_SYMBOLS:
    B, C = bot.STRATEGIES_BY_SYMBOL[symbol]
    assert [B["code"], C["code"]] == ["B", "C"]
    assert (B["safe_entry_price_min"], B["safe_entry_price_max"]) == (0.67, 0.75)
    assert (C["safe_entry_price_min"], C["safe_entry_price_max"]) == (0.67, 0.70)
    assert B["dca_min_buy_price"] == bot.MIN_PRICE
    assert B["dca_rebound_mom_max"] is None
    assert C["dca_min_buy_price"] == 0.30
    assert C["dca_rebound_mom_max"] == 0.15
    assert bot.strategy_mode(B["name"]) == "PAPER"
    assert bot.strategy_mode(C["name"]) == "PAPER"

assert bot.TAKE_PROFIT_USDC == 0.30
assert bot.ALLOW_DOUBLE_LIVE is False
assert not hasattr(bot, "evaluate_consensus_variant")

slot = (int(time.time()) // 300) * 300
counter = 0

def market(symbol, tag):
    global counter
    counter += 1
    m = {
        "condition_id": f"cid-{symbol}-{tag}-{counter}",
        "symbol": symbol,
        "question": f"{symbol} Up or Down test",
        "slug": f"{bot.ASSET_CONFIG[symbol]['prefix']}-{slot}",
        "start_ts": slot,
        "end_ts": slot + 300,
        "up_asset": f"{symbol}-UP-{tag}-{counter}",
        "down_asset": f"{symbol}-DN-{tag}-{counter}",
    }
    bot.markets[m["condition_id"]] = m
    bot.persist_market(m)
    return m

def book(asset, bid, ask, size=100.0):
    bot.books[asset] = {
        "bids": {float(bid): float(size)},
        "asks": {float(ask): float(size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }

def seed_up(m, ask=.68, mom=.07):
    ms = bot.now_ms()
    ref = ask - mom
    mid = ref + mom / 2
    book(m["up_asset"], ask-.01, ask)
    book(m["down_asset"], max(.01, 1-ask-.01), max(.01, 1-ask))
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000, ref), (ms-3000, mid), (ms, ask)])
    hd = bot.price_history[m["condition_id"]][m["down_asset"]]
    hd.clear()
    hd.extend([(ms-6000, .45), (ms-3000, .40), (ms, .35)])

def set_up_path(m, ref, mid, ask):
    ms = bot.now_ms()
    book(m["up_asset"], max(.01, ask-.01), ask)
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000, ref), (ms-3000, mid), (ms, ask)])

# ------------------------------------------------------------------
# B: wide entry + old deep reversal DCA remains allowed.
# ------------------------------------------------------------------
m_b = market("BTC", "B")
B = bot.STRATEGIES_BY_SYMBOL["BTC"][0]
seed_up(m_b, .72, .07)
asyncio.run(bot.evaluate_variant(m_b, B, 30.0))
assert bot.position_totals(m_b["condition_id"], B["name"])["bought"] == 5

set_up_path(m_b, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_b, B, 60.0))
assert bot.get_variant_state(m_b["condition_id"], B)["dca_armed"]
assert bot.position_totals(m_b["condition_id"], B["name"])["bought"] == 5

set_up_path(m_b, .19, .22, .25)  # +.06 rebound; B intentionally allows deep DCA.
asyncio.run(bot.evaluate_variant(m_b, B, 70.0))
assert bot.position_totals(m_b["condition_id"], B["name"])["bought"] == 10

# ------------------------------------------------------------------
# C: .72 target skips; valid .69 target + safer DCA constraints.
# ------------------------------------------------------------------
m_c_hi = market("XRP", "C-HI")
C_xrp = bot.STRATEGIES_BY_SYMBOL["XRP"][1]
seed_up(m_c_hi, .72, .07)
asyncio.run(bot.evaluate_variant(m_c_hi, C_xrp, 30.0))
assert bot.position_totals(m_c_hi["condition_id"], C_xrp["name"])["bought"] == 0

m_c = market("ETH", "C")
C = bot.STRATEGIES_BY_SYMBOL["ETH"][1]
seed_up(m_c, .69, .07)
asyncio.run(bot.evaluate_variant(m_c, C, 30.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

set_up_path(m_c, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_c, C, 60.0))
assert bot.get_variant_state(m_c["condition_id"], C)["dca_armed"]

set_up_path(m_c, .19, .22, .25)  # below C floor
asyncio.run(bot.evaluate_variant(m_c, C, 70.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

set_up_path(m_c, .15, .25, .35)  # +.20 above C momentum cap
asyncio.run(bot.evaluate_variant(m_c, C, 80.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

set_up_path(m_c, .25, .30, .35)  # +.10 valid
asyncio.run(bot.evaluate_variant(m_c, C, 90.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 10

# ------------------------------------------------------------------
# PAPER TP: 5 @ .68; .76 is below +.30 NET; .77 clears target.
# ------------------------------------------------------------------
m_tp = market("BTC", "PAPER-TP")
B_tp = bot.STRATEGIES_BY_SYMBOL["BTC"][0]
seed_up(m_tp, .68, .07)
asyncio.run(bot.evaluate_variant(m_tp, B_tp, 30.0))
assert bot.position_totals(m_tp["condition_id"], B_tp["name"])["bought"] == 5

book(m_tp["up_asset"], .76, .77)
candidate = bot.projected_full_exit(m_tp["condition_id"], B_tp["name"])
assert candidate and candidate["total_pnl"] < .30
assert not asyncio.run(bot.maybe_take_profit(m_tp, B_tp, 60.0))

book(m_tp["up_asset"], .77, .78)
candidate = bot.projected_full_exit(m_tp["condition_id"], B_tp["name"])
assert candidate and candidate["total_pnl"] >= .30
assert asyncio.run(bot.maybe_take_profit(m_tp, B_tp, 63.0))
after = bot.position_totals(m_tp["condition_id"], B_tp["name"])
assert after["remaining"] <= 1e-9
with bot.db() as conn:
    r = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m_tp["condition_id"], B_tp["name"]),
    ).fetchone()
assert r and r["winning_outcome"] == "TAKE_PROFIT"
assert r["execution_mode"] == "PAPER"
assert float(r["pnl"]) >= .30

# ------------------------------------------------------------------
# LIVE FAK + LIVE TP with fake official SDK client.
# ------------------------------------------------------------------
@dataclass(frozen=True)
class FakeSigned:
    token_id: str
    price: str
    size: str
    side: str
    post_only: bool = False
    order_type: str = "GTC"

@dataclass
class FakeResponse:
    ok: bool
    making_amount: Decimal
    taking_amount: Decimal
    status: str = "matched"
    order_id: str = "fake-order"
    trade_ids: tuple = ("fake-trade",)
    code: str = ""
    message: str = ""

class FakeClient:
    async def create_limit_order(self, **kwargs):
        return FakeSigned(
            token_id=str(kwargs["token_id"]),
            price=str(kwargs["price"]),
            size=str(kwargs["size"]),
            side=str(kwargs["side"]).upper(),
            post_only=bool(kwargs.get("post_only", False)),
        )

    async def post_order(self, order):
        size = Decimal(order.size)
        price = Decimal(order.price)
        assert order.order_type == "FAK"
        if order.side == "BUY":
            return FakeResponse(True, size * price, size)
        return FakeResponse(True, size, size * price)

bot.LIVE_MASTER_ENABLE = True
bot.live_client_ready = True
bot.live_client = FakeClient()
bot.sdk_post_order_with_allowance_recovery = None

m_live = market("ETH", "LIVE-TP")
B_live = bot.STRATEGIES_BY_SYMBOL["ETH"][0]
bot.state_set(f"mode:{B_live['name']}", "LIVE")
book(m_live["up_asset"], .67, .68)
book(m_live["down_asset"], .31, .32)

buy = asyncio.run(bot.execute_live_fak(
    m_live["condition_id"], B_live, m_live["up_asset"], "Up",
    "ENTRY", "BUY", 5.0,
))
assert buy["ok"] and abs(buy["filled"] - 5.0) < 1e-9

book(m_live["up_asset"], .77, .78)
mark = bot.projected_full_exit(m_live["condition_id"], B_live["name"])
assert mark and mark["total_pnl"] >= .30
assert asyncio.run(bot.maybe_take_profit(m_live, B_live, 60.0))

live_after = bot.position_totals(m_live["condition_id"], B_live["name"])
assert live_after["remaining"] <= 1e-9
assert live_after["execution_mode"] == "LIVE"
with bot.db() as conn:
    lr = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m_live["condition_id"], B_live["name"]),
    ).fetchone()
    sells = conn.execute(
        "SELECT * FROM live_orders WHERE condition_id=? AND variant=? AND action='SELL'",
        (m_live["condition_id"], B_live["name"]),
    ).fetchall()
assert lr and lr["winning_outcome"] == "TAKE_PROFIT"
assert lr["execution_mode"] == "LIVE"
assert float(lr["pnl"]) >= .30
assert len(sells) == 1 and sells[0]["reason"] == "TAKE_PROFIT"

# A fully TP-closed strategy cannot buy again.
assert not asyncio.run(bot.execute_order(
    m_live["condition_id"], B_live, m_live["up_asset"], "Up", "ENTRY"
))

# ------------------------------------------------------------------
# Same-token double LIVE safety.
# ------------------------------------------------------------------
C_live_same = bot.STRATEGIES_BY_SYMBOL["ETH"][1]
bot.state_set(f"mode:{B_live['name']}", "LIVE")
bot.state_set(f"mode:{C_live_same['name']}", "PAPER")
assert bot._other_live_same_symbol(C_live_same)["code"] == "B"
assert bot.ALLOW_DOUBLE_LIVE is False

print("BTC/XRP/ETH B/C PAPER/LIVE + NET TP regression: OK")
print(f"PAPER example PnL: ${float(r['pnl']):+.5f}")
print(f"LIVE example PnL estimate: ${float(lr['pnl']):+.5f}")
