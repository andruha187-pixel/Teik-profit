import os
import shutil
import asyncio
import importlib.util

TEST_DIR = "/tmp/prejump_multi_safe"
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ["DATA_DIR"] = TEST_DIR
os.environ["PREJUMP_SCORE"] = "0.40"
os.environ["BTC_PREJUMP_SCORE"] = "0.43"
os.environ["BTC_PREJUMP_PRICE_MAX"] = "0.55"
os.environ["SOL_PREJUMP_PM_MOM_MAX"] = "0.02"
os.environ["XRP_PREJUMP_SCORE"] = "0.41"
os.environ["BNB_PREJUMP_PM_MOM_MIN"] = "0.01"
os.environ["DOGE_PREJUMP_MAX_SPREAD"] = "0.02"
os.environ["ETH_PREJUMP_SCORE"] = "0.455"
os.environ["ETH_PREJUMP_PM_MOM_MAX"] = "0.02"
os.environ["LIVE_MASTER_ENABLE"] = "0"

spec = importlib.util.spec_from_file_location("bot", os.path.join(os.path.dirname(__file__), "main.py"))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()
bot.set_prejump_score(0.40)

assert bot.VERSION.startswith("20.11-")
assert abs(bot.BTC_PREJUMP_SCORE - 0.43) < 1e-12
assert abs(bot.BTC_PREJUMP_PRICE_MAX - 0.55) < 1e-12
assert abs(bot.SOL_PREJUMP_PM_MOM_MAX - 0.02) < 1e-12
assert abs(bot.XRP_PREJUMP_SCORE - 0.41) < 1e-12
assert abs(bot.BNB_PREJUMP_PM_MOM_MIN - 0.01) < 1e-12
assert abs(bot.DOGE_PREJUMP_MAX_SPREAD - 0.02) < 1e-12
assert abs(bot.ETH_PREJUMP_SCORE - 0.455) < 1e-12
assert abs(bot.ETH_PREJUMP_PM_MOM_MAX - 0.02) < 1e-12

ask_now = 0.54
bid_now = 0.53
mom_now = 0.00
calls = []

async def fake_refresh(_asset):
    return bid_now, ask_now, 5

async def fake_execute(*args, **kwargs):
    calls.append((args, kwargs))
    return True

bot._refresh_entry_book_if_needed = fake_refresh
bot.pm_fast_momentum = lambda *_a, **_k: mom_now
bot.execute_order = fake_execute

def market(symbol, cid):
    return {
        "condition_id": cid,
        "symbol": symbol,
        "up_asset": f"{cid}-UP",
        "down_asset": f"{cid}-DN",
        "start_ts": 0,
        "end_ts": 300,
    }

def feature(score):
    return {
        "ext_score": score,
        "up_votes": 2,
        "down_votes": 0,
        "fresh_venues": 3,
        "binance": {"fresh": True, "score": score},
        "bybit": {"fresh": True, "score": score},
        "coinbase": {"fresh": True, "score": score},
    }

async def expect_fail_then_stays_skipped(symbol, cid, score, ask, bid, mom, later_score=None, later_ask=None, later_bid=None, later_mom=None):
    global ask_now, bid_now, mom_now
    before = len(calls)
    ask_now, bid_now, mom_now = ask, bid, mom
    v = bot.STRATEGIES_BY_SYMBOL[symbol][0]
    m = market(symbol, cid)
    r = await bot.evaluate_prejump_variant(m, v, 30.0, feature(score))
    assert not r, (symbol, "expected initial skip")
    assert len(calls) == before, (symbol, "unexpected execution on skip")
    st = bot.get_variant_state(cid, v)
    assert st["gate_decided"] and not st["gate_passed"], (symbol, st)
    ask_now = ask if later_ask is None else later_ask
    bid_now = bid if later_bid is None else later_bid
    mom_now = mom if later_mom is None else later_mom
    r = await bot.evaluate_prejump_variant(m, v, 31.0, feature(score if later_score is None else later_score))
    assert not r and len(calls) == before, (symbol, "market resurrected after first-signal skip")

async def expect_pass(symbol, cid, score, ask, bid, mom):
    global ask_now, bid_now, mom_now
    before = len(calls)
    ask_now, bid_now, mom_now = ask, bid, mom
    v = bot.STRATEGIES_BY_SYMBOL[symbol][0]
    m = market(symbol, cid)
    r = await bot.evaluate_prejump_variant(m, v, 30.0, feature(score))
    assert r, (symbol, "expected pass")
    assert len(calls) == before + 1, (symbol, "execution count")
    st = bot.get_variant_state(cid, v)
    assert st["gate_decided"] and st["gate_passed"], (symbol, st)

async def run():
    # BTC: score >= .43 AND first-signal ask <= .55.
    await expect_fail_then_stays_skipped("BTC", "btc-fail", 0.42, 0.54, 0.53, 0.00, later_score=0.46)
    await expect_pass("BTC", "btc-pass", 0.44, 0.55, 0.54, 0.00)

    # SOL: first-signal PM momentum <= +.02.
    await expect_fail_then_stays_skipped("SOL", "sol-fail", 0.44, 0.54, 0.53, 0.03, later_mom=0.01)
    await expect_pass("SOL", "sol-pass", 0.44, 0.54, 0.53, 0.02)

    # XRP: first-signal score >= .41.
    await expect_fail_then_stays_skipped("XRP", "xrp-fail", 0.405, 0.54, 0.53, 0.00, later_score=0.45)
    await expect_pass("XRP", "xrp-pass", 0.41, 0.54, 0.53, 0.00)

    # BNB: first-signal PM momentum >= +.01.
    await expect_fail_then_stays_skipped("BNB", "bnb-fail", 0.44, 0.54, 0.53, 0.00, later_mom=0.02)
    await expect_pass("BNB", "bnb-pass", 0.44, 0.54, 0.53, 0.01)

    # DOGE: fresh non-crossed spread must be <= .02.
    await expect_fail_then_stays_skipped("DOGE", "doge-fail", 0.44, 0.56, 0.53, 0.00, later_ask=0.55, later_bid=0.54)
    await expect_pass("DOGE", "doge-pass", 0.44, 0.55, 0.53, 0.00)

    # ETH: first-signal score >= .455 AND PM momentum <= +.02.
    await expect_fail_then_stays_skipped("ETH", "eth-score-fail", 0.45, 0.54, 0.53, 0.00, later_score=0.49)
    await expect_fail_then_stays_skipped("ETH", "eth-mom-fail", 0.47, 0.54, 0.53, 0.03, later_mom=0.01)
    await expect_pass("ETH", "eth-pass", 0.455, 0.54, 0.53, 0.02)

    # HYPE remains intentionally unfiltered; base PRE-JUMP passes unchanged.
    await expect_pass("HYPE", "hype-base", 0.40, 0.54, 0.53, 0.00)

asyncio.run(run())
print("MULTI SAFE + ETH first-signal regressions: OK")
