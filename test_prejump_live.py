import os
import shutil
import asyncio
import importlib.util
from dataclasses import dataclass
from decimal import Decimal

TEST_DIR = "/tmp/prejump_live_regression"
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ["DATA_DIR"] = TEST_DIR
os.environ["PREJUMP_SCORE"] = "0.40"
os.environ["LIVE_MASTER_ENABLE"] = "0"

spec = importlib.util.spec_from_file_location("bot", os.path.join(os.path.dirname(__file__), "main.py"))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.VERSION.startswith("20.11-")
assert len(bot.STRATEGIES) == 7
assert abs(bot.prejump_score() - 0.40) < 1e-12
assert not bot.trading_enabled()
assert all(bot.strategy_mode(v["name"]) == "PAPER" for v in bot.STRATEGIES)
assert abs(bot.entry_shares(bot.STRATEGIES[0]) - 5.0) < 1e-12
assert abs(bot.take_profit_usdc() - 0.60) < 1e-12

# Runtime score persistence + hard floor.
bot.set_prejump_score(0.42)
assert abs(bot.prejump_score() - 0.42) < 1e-12
bot.PREJUMP_SCORE_RUNTIME = 0.40
bot.load_prejump_score()
assert abs(bot.prejump_score() - 0.42) < 1e-12
try:
    bot.set_prejump_score(0.39)
    raise AssertionError("score below 0.40 must be rejected")
except ValueError:
    pass
bot.set_prejump_score(0.40)

V = bot.STRATEGIES_BY_SYMBOL["XRP"][0]
market = {
    "condition_id": "sig-40",
    "symbol": "XRP",
    "up_asset": "UP1",
    "down_asset": "DN1",
    "start_ts": 0,
    "end_ts": 300,
}
feature_041 = {
    "ext_score": 0.41,
    "up_votes": 2,
    "down_votes": 0,
    "fresh_venues": 2,
    "binance": {"fresh": True, "score": 0.35},
    "bybit": {"fresh": True, "score": 0.40},
    "coinbase": None,
}

calls = []
async def fake_refresh(_asset):
    return 0.58, 0.60, 25
async def fake_execute(*args, **kwargs):
    calls.append((args, kwargs))
    return True
bot._refresh_entry_book_if_needed = fake_refresh
bot.pm_fast_momentum = lambda *_a, **_k: 0.01
bot.execute_order = fake_execute

async def signal_tests():
    bot.set_prejump_score(0.42)
    r = await bot.evaluate_prejump_variant(market, V, 15.0, feature_041)
    assert not r and not calls, "0.41 must not pass threshold 0.42"

    market2 = dict(market, condition_id="sig-40-pass")
    bot.set_prejump_score(0.40)
    r = await bot.evaluate_prejump_variant(market2, V, 15.0, feature_041)
    assert r and len(calls) == 1, "0.41 must pass threshold 0.40"

    market3 = dict(market, condition_id="too-early")
    r = await bot.evaluate_prejump_variant(market3, V, 0.5, feature_041)
    assert not r and len(calls) == 1

    market4 = dict(market, condition_id="too-late")
    r = await bot.evaluate_prejump_variant(market4, V, 161.0, feature_041)
    assert not r and len(calls) == 1

asyncio.run(signal_tests())

with bot.db() as conn:
    row = conn.execute(
        "SELECT ext_score,threshold FROM prejump_signals WHERE condition_id='sig-40-pass'"
    ).fetchone()
assert row is not None and abs(row["ext_score"] - 0.41) < 1e-12 and abs(row["threshold"] - 0.40) < 1e-12

# LIVE master is a hard block even if the function is called directly.
bot.LIVE_MASTER_ENABLE = False
res = asyncio.run(bot.execute_live_fak("blocked", V, "UPX", "Up", "ENTRY", "BUY", 5))
assert not res["ok"] and res["filled"] == 0 and "LIVE_MASTER_ENABLE=0" in res["error"]

# Fake SDK path verifies exact requested shares + FAK conversion without touching a network.
@dataclass(frozen=True)
class FakeSigned:
    token_id: str
    price: str
    size: str
    side: str
    post_only: bool = False
    order_type: str = "LIMIT"

class FakeAccepted:
    ok = True
    code = ""
    message = ""
    status = "matched"
    order_id = "fake-order"
    trade_ids = ("fake-trade",)
    def __init__(self, making, taking):
        self.making_amount = str(making)
        self.taking_amount = str(taking)

class FakeClient:
    async def create_limit_order(self, **kwargs):
        return FakeSigned(
            token_id=str(kwargs["token_id"]), price=str(kwargs["price"]),
            size=str(kwargs["size"]), side=str(kwargs["side"]),
            post_only=bool(kwargs.get("post_only", False)),
        )
    async def post_order(self, order):
        assert order.order_type == "FAK"
        size = Decimal(order.size)
        price = Decimal(order.price)
        if order.side == "BUY":
            return FakeAccepted(size * price, size)
        return FakeAccepted(size, size * price)

# Re-enable real execution gate only against the in-process fake client.
bot.LIVE_MASTER_ENABLE = True
bot.live_client_ready = True
bot.live_client = FakeClient()
bot.sdk_post_order_with_allowance_recovery = None
now = bot.now_ms()
bot.books["UPLIVE"] = {
    "bids": {0.59: 100.0},
    "asks": {0.60: 100.0},
    "received_ms": now,
}
res = asyncio.run(bot.execute_live_fak("live-fake", V, "UPLIVE", "Up", "ENTRY", "BUY", 5.0))
assert res["ok"] and abs(res["filled"] - 5.0) < 1e-9 and abs(res["avg"] - 0.60) < 1e-9
assert bot.position_totals("live-fake", V["name"])["execution_mode"] == "LIVE"

# Mode cannot cross a still-open LIVE position into PAPER.
bot.state_set(f"mode:{V['name']}", "OFF")
ok, msg = bot._set_mode_direct(V, "PAPER")
assert not ok and "open LIVE position" in msg

print("PRE-JUMP PAPER/LIVE regression: OK")
