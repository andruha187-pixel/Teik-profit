import os
import tempfile
import importlib.util
from pathlib import Path

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='prejump_parser_')
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''
os.environ['SYMBOLS'] = 'BTC,XRP,BNB,SOL,ETH,DOGE,HYPE'

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('bot',HERE/'main.py')
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
now=bot.now_ms()

# Binance USD-M official-shape aggTrade and partial depth.
bot.handle_binance_payload('BTC', {
    'e':'depthUpdate','E':now,'T':now,'s':'BTCUSDT',
    'b':[['100.0','10'],['99.9','20']],
    'a':[['100.1','8'],['100.2','10']],
})
bot.handle_binance_payload('BTC', {
    'e':'aggTrade','E':now,'T':now,'s':'BTCUSDT','p':'100.05','q':'2','m':False,
})
assert bot.book_metrics('binance','BTC')['best_bid'] == 100.0
_,_,flow=bot.bucket_flow('binance','BTC',1)
assert flow > 0.99  # m=False => taker BUY

# m=True => taker SELL.
bot.handle_binance_payload('BTC', {
    'e':'aggTrade','E':now+1,'T':now+1,'s':'BTCUSDT','p':'100.05','q':'4','m':True,
})
_,_,flow2=bot.bucket_flow('binance','BTC',1)
assert flow2 < 0

# Bybit orderbook snapshot, taker trade and liquidation semantics.
bot.handle_bybit_message('ETH', {
    'topic':'orderbook.50.ETHUSDT','type':'snapshot','ts':now,
    'data':{'s':'ETHUSDT','b':[['2000','10']], 'a':[['2001','8']], 'u':1,'seq':1},
})
bot.handle_bybit_message('ETH', {
    'topic':'publicTrade.ETHUSDT','type':'snapshot','ts':now,
    'data':[{'T':now,'s':'ETHUSDT','S':'Buy','v':'3','p':'2000.5'}],
})
assert bot.book_metrics('bybit','ETH')['best_ask'] == 2001.0
_,_,byflow=bot.bucket_flow('bybit','ETH',1)
assert byflow > .99

# S=Buy in Bybit liquidation means a LONG was liquidated, i.e. forced SELL.
bot.handle_bybit_message('ETH', {
    'topic':'allLiquidation.ETHUSDT','type':'snapshot','ts':now,
    'data':[{'T':now,'s':'ETHUSDT','S':'Buy','v':'2','p':'1995'}],
})
_,_,liqflow=bot.bucket_flow('bybit','ETH',3,liquidations=True)
assert liqflow < -.99

# Coinbase L2 and market_trades. Trade side is maker side, so BUY maker => SELL taker.
bot.handle_coinbase_message({
    'channel':'l2_data','timestamp':'2026-09-05T00:00:00Z','sequence_num':1,
    'events':[{'type':'snapshot','product_id':'BTC-USD','updates':[
        {'side':'bid','event_time':'2026-09-05T00:00:00Z','price_level':'100','new_quantity':'5'},
        {'side':'offer','event_time':'2026-09-05T00:00:00Z','price_level':'100.1','new_quantity':'4'},
    ]}],
})
bot.handle_coinbase_message({
    'channel':'market_trades','timestamp':'2026-09-05T00:00:00Z','sequence_num':2,
    'events':[{'type':'update','trades':[
        {'trade_id':'1','product_id':'BTC-USD','price':'100.05','size':'1','side':'BUY','time':'2026-09-05T00:00:00Z'}
    ]}],
})
assert bot.book_metrics('coinbase','BTC')['best_bid'] == 100.0
# Historical timestamp may be outside current 1s window; inspect signed bucket directly.
cb = bot.venue_trade_buckets['coinbase']['BTC']
assert cb and cb[-1][1] < 0

print('External source message parser regression: OK')
print('Binance aggTrade maker/taker sign: OK')
print('Bybit trade + liquidation direction: OK')
print('Coinbase maker-side inversion + L2: OK')
