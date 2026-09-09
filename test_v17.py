import os, tempfile, importlib.util, asyncio, time

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")
os.environ["DATA_DIR"] = tempfile.mkdtemp(prefix="prelead_v17_test_")
os.environ["TP_LIVE_SIM_DELAY_MS"] = "10"
os.environ["TP_LIVE_SIM_MIN_HOLD_MS"] = "0"
os.environ["TAKE_PROFIT_USDC"] = "0.90"

spec = importlib.util.spec_from_file_location("bot", os.path.join(os.path.dirname(__file__), "main.py"))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.STRATEGY_CODES == ("PRE_LEAD_SAFE",)
assert abs(bot.take_profit_usdc() - 0.90) < 1e-9
assert abs(bot.set_take_profit_usdc(0.80) - 0.80) < 1e-9
assert abs(bot.set_take_profit_usdc(bot.take_profit_usdc() + bot.TAKE_PROFIT_STEP_USDC) - 0.90) < 1e-9

# Test frozen-limit delayed TP fill.
strategy = bot.STRATEGIES_BY_SYMBOL["BTC"][0]
market = {"condition_id":"testcid", "symbol":"BTC", "up_asset":"up", "down_asset":"down",
          "start_ts":time.time()-10, "end_ts":time.time()+100, "slug":"btc-updown-5m-1"}
price, sh = 0.50, 5.0
gross = price*sh
fee = bot.fee_usdc(sh, price)
with bot.db() as c:
    c.execute("""INSERT INTO paper_trades(
        trade_ms,condition_id,symbol,variant,asset,outcome,requested_shares,filled_shares,
        avg_price,gross_cost,fee,total_cost,book_age_ms,fills_json
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (bot.now_ms()-5000, "testcid", "BTC", strategy["name"], "up", "Up", sh, sh,
     price, gross, fee, gross+fee, 0, "[]"))
    c.commit()
# +0.90 target needs a higher bid than 0.70 from this entry.
bot.set_take_profit_usdc(0.80)
bot.books["up"] = {"bids": {0.70:10.0}, "asks": {0.71:10.0}, "received_ms": bot.now_ms(), "source":"ws"}

async def main():
    assert await bot.maybe_take_profit(market, strategy)
    await asyncio.sleep(0.03)
    with bot.db() as c:
        assert c.execute("SELECT COUNT(*) FROM paper_exits").fetchone()[0] == 1
        ev = c.execute("SELECT status FROM tp_live_sim_events ORDER BY id DESC LIMIT 1").fetchone()
        assert ev["status"] == "FILLED"

asyncio.run(main())
print("OK v1.7 PRE_LEAD_SAFE LIVE-SIM tests")
