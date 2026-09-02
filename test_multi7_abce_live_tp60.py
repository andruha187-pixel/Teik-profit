import os
import time
import asyncio
import tempfile
import importlib.util
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

tmp = tempfile.mkdtemp(prefix="multi7_abce_live_tp60_")
os.environ["DATA_DIR"] = tmp
os.environ["TELEGRAM_BOT_TOKEN"] = ""
os.environ["TELEGRAM_CHAT_ID"] = ""
os.environ["SYMBOLS"] = "BTC,XRP,BNB,SOL,ETH,DOGE,HYPE"
os.environ["PAPER_START_BALANCE"] = "500"
os.environ["TAKE_PROFIT_USDC"] = "0.60"
os.environ["LIVE_MASTER_ENABLE"] = "0"
os.environ["ALLOW_MULTI_LIVE_PER_TOKEN"] = "0"
os.environ["POLYMARKET_PRIVATE_KEY"] = ""

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("bot", here / "main.py")
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

SYMS = ["BTC","XRP","BNB","SOL","ETH","DOGE","HYPE"]

# ------------------------------------------------------------------
# Configuration parity
# ------------------------------------------------------------------
assert bot.SYMBOLS == SYMS
assert len(bot.STRATEGIES) == 28
assert bot.TAKE_PROFIT_USDC == 0.60
assert bot.ALLOW_MULTI_LIVE_PER_TOKEN is False

# Environment override stays configurable.
os.environ["TAKE_PROFIT_USDC"] = "0.90"
assert bot._take_profit_from_env() == 0.90
os.environ["TAKE_PROFIT_USDC"] = "OFF"
assert bot._take_profit_from_env() is None
os.environ["TAKE_PROFIT_USDC"] = "0.60"
assert bot._take_profit_from_env() == 0.60

for symbol in SYMS:
    A,B,C,E = bot.STRATEGIES_BY_SYMBOL[symbol]
    assert [v["code"] for v in (A,B,C,E)] == ["A","B","C","E"]
    assert all(bot.strategy_mode(v["name"]) == "PAPER" for v in (A,B,C,E))
    assert all(bot.paper_cash(v["name"]) == 500 for v in (A,B,C,E))
    assert (A["safe_entry_price_min"], A["safe_entry_price_max"]) == (0.67,0.75)
    assert (B["safe_entry_price_min"], B["safe_entry_price_max"]) == (0.67,0.75)
    assert (C["safe_entry_price_min"], C["safe_entry_price_max"]) == (0.67,0.70)
    assert (E["safe_entry_price_min"], E["safe_entry_price_max"]) == (0.67,0.75)
    assert not A["dca_enabled"]
    assert B["dca_enabled"]
    assert C["dca_enabled"]
    assert not E["dca_enabled"] and E["consensus_enabled"]
    assert B["dca_min_buy_price"] == bot.MIN_PRICE
    assert B["dca_rebound_mom_max"] is None
    assert C["dca_min_buy_price"] == 0.30
    assert C["dca_max_buy_price"] == 0.60
    assert C["dca_rebound_mom"] == 0.05
    assert C["dca_rebound_mom_max"] == 0.15
    assert E["consensus_min_other_tokens"] == 2
    assert E["consensus_window_sec"] == 10

assert bot._take_profit_from_env() == 0.60


def set_book(asset, bid, ask, size=100.0):
    bot.books[asset] = {
        "bids": {float(bid): float(size)},
        "asks": {float(ask): float(size)},
        "received_ms": bot.now_ms(),
        "source": "test",
    }


slot = (int(time.time()) // 300) * 300
counter = 0

def make_market(symbol, tag):
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


def seed_up(m, ask=.68, mom=.07):
    ms = bot.now_ms()
    ref = ask - mom
    mid = ref + mom/2
    set_book(m["up_asset"], max(.01, ask-.01), ask)
    set_book(m["down_asset"], max(.01, 1-ask-.01), max(.01, 1-ask))
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])
    hd = bot.price_history[m["condition_id"]][m["down_asset"]]
    hd.clear()
    hd.extend([(ms-6000,.45),(ms-3000,.40),(ms,.35)])


def set_up_path(m, ref, mid, ask):
    ms = bot.now_ms()
    set_book(m["up_asset"], max(.01, ask-.01), ask)
    h = bot.price_history[m["condition_id"]][m["up_asset"]]
    h.clear()
    h.extend([(ms-6000,ref),(ms-3000,mid),(ms,ask)])


# ------------------------------------------------------------------
# B old DCA and C safer DCA remain distinct
# ------------------------------------------------------------------
m_b = make_market("BTC", "b")
B = bot.STRATEGIES_BY_SYMBOL["BTC"][1]
seed_up(m_b, .72, .07)
asyncio.run(bot.evaluate_variant(m_b, B, 30.0))
assert bot.position_totals(m_b["condition_id"], B["name"])["bought"] == 5

set_up_path(m_b, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_b, B, 60.0))
assert bot.get_variant_state(m_b["condition_id"], B)["dca_armed"]

set_up_path(m_b, .19, .22, .25)
asyncio.run(bot.evaluate_variant(m_b, B, 70.0))
assert bot.position_totals(m_b["condition_id"], B["name"])["bought"] == 10

m_c = make_market("ETH", "c")
C = bot.STRATEGIES_BY_SYMBOL["ETH"][2]
seed_up(m_c, .69, .07)
asyncio.run(bot.evaluate_variant(m_c, C, 30.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5

set_up_path(m_c, .58, .54, .50)
asyncio.run(bot.evaluate_variant(m_c, C, 60.0))
set_up_path(m_c, .19, .22, .25)
asyncio.run(bot.evaluate_variant(m_c, C, 70.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5
set_up_path(m_c, .15, .25, .35)
asyncio.run(bot.evaluate_variant(m_c, C, 80.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 5
set_up_path(m_c, .25, .30, .35)
asyncio.run(bot.evaluate_variant(m_c, C, 90.0))
assert bot.position_totals(m_c["condition_id"], C["name"])["bought"] == 10

# ------------------------------------------------------------------
# E still uses two OTHER A/BASE SAFE67 passes within 10 seconds
# ------------------------------------------------------------------
for symbol in ("ETH", "SOL"):
    m = make_market(symbol, "vote")
    A_src = bot.STRATEGIES_BY_SYMBOL[symbol][0]
    seed_up(m, .68, .07)
    asyncio.run(bot.evaluate_variant(m, A_src, 30.0))
    assert bot.position_totals(m["condition_id"], A_src["name"])["bought"] == 5

m_e = make_market("BNB", "cons")
E = bot.STRATEGIES_BY_SYMBOL["BNB"][3]
seed_up(m_e, .68, .07)
asyncio.run(bot.evaluate_consensus_variant(m_e, E, 35.0))
assert bot.position_totals(m_e["condition_id"], E["name"])["bought"] == 5
with bot.db() as conn:
    ce = conn.execute(
        "SELECT * FROM consensus_events WHERE condition_id=? AND variant=?",
        (m_e["condition_id"], E["name"]),
    ).fetchone()
assert ce and ce["passed"] == 1 and ce["confirm_count"] >= 2

# ------------------------------------------------------------------
# PAPER TP default +$0.60 NET: .82 is too low, .83 clears it for 5 @ .68.
# ------------------------------------------------------------------
m_tp = make_market("DOGE", "paper-tp")
A_tp = bot.STRATEGIES_BY_SYMBOL["DOGE"][0]
seed_up(m_tp, .68, .07)
asyncio.run(bot.evaluate_variant(m_tp, A_tp, 30.0))
assert bot.position_totals(m_tp["condition_id"], A_tp["name"])["bought"] == 5

set_book(m_tp["up_asset"], .82, .83)
mark_82 = bot.projected_full_exit(m_tp["condition_id"], A_tp["name"])
assert mark_82 and mark_82["total_pnl"] < .60
assert not asyncio.run(bot.maybe_take_profit(m_tp, A_tp, 60.0))

set_book(m_tp["up_asset"], .83, .84)
mark_83 = bot.projected_full_exit(m_tp["condition_id"], A_tp["name"])
assert mark_83 and mark_83["total_pnl"] >= .60
assert asyncio.run(bot.maybe_take_profit(m_tp, A_tp, 63.0))
paper_after = bot.position_totals(m_tp["condition_id"], A_tp["name"])
assert paper_after["remaining"] <= 1e-9
with bot.db() as conn:
    pr = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m_tp["condition_id"], A_tp["name"]),
    ).fetchone()
assert pr and pr["winning_outcome"] == "TAKE_PROFIT"
assert pr["execution_mode"] == "PAPER"
assert float(pr["pnl"]) >= .60

# ------------------------------------------------------------------
# LIVE FAK entry + LIVE TP using a fake official SDK client
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

m_live = make_market("XRP", "live-tp")
A_live = bot.STRATEGIES_BY_SYMBOL["XRP"][0]
bot.state_set(f"mode:{A_live['name']}", "LIVE")
seed_up(m_live, .68, .07)
asyncio.run(bot.evaluate_variant(m_live, A_live, 30.0))
live_pos = bot.position_totals(m_live["condition_id"], A_live["name"])
assert live_pos["execution_mode"] == "LIVE"
assert abs(live_pos["bought"] - 5.0) < 1e-9

set_book(m_live["up_asset"], .83, .84)
assert asyncio.run(bot.maybe_take_profit(m_live, A_live, 60.0))
live_after = bot.position_totals(m_live["condition_id"], A_live["name"])
assert live_after["remaining"] <= 1e-9
with bot.db() as conn:
    lr = conn.execute(
        "SELECT * FROM market_results WHERE condition_id=? AND variant=?",
        (m_live["condition_id"], A_live["name"]),
    ).fetchone()
    sells = conn.execute(
        "SELECT * FROM live_orders WHERE condition_id=? AND variant=? AND action='SELL'",
        (m_live["condition_id"], A_live["name"]),
    ).fetchall()
assert lr and lr["winning_outcome"] == "TAKE_PROFIT"
assert lr["execution_mode"] == "LIVE"
assert float(lr["pnl"]) >= .60
assert len(sells) == 1 and sells[0]["reason"] == "TAKE_PROFIT"

# ------------------------------------------------------------------
# Same-token multiple-LIVE protection defaults to blocked.
# ------------------------------------------------------------------
B_xrp = bot.STRATEGIES_BY_SYMBOL["XRP"][1]
bot.state_set(f"mode:{A_live['name']}", "LIVE")
bot.state_set(f"mode:{B_xrp['name']}", "PAPER")
other = bot._other_live_same_symbol(B_xrp)
assert other and other["code"] == "A"
assert bot.ALLOW_MULTI_LIVE_PER_TOKEN is False

print("MULTI7 A/B/C/E PAPER/LIVE + NET TP60 regression: OK")
print(f"PAPER TP example NET PnL: ${float(pr['pnl']):+.5f}")
print(f"LIVE TP example NET PnL estimate: ${float(lr['pnl']):+.5f}")
