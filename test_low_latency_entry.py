import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass

TEST_DIR='/tmp/prejump_low_latency'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['LIVE_ENTRY_MAX_SLIPPAGE']='0.05'
os.environ['FAST_INTERVAL']='0.10'
# Even if an old deployment still has this legacy env set, v20.11 must not
# force REST on the first accepted PRE-JUMP FAK.
os.environ['LIVE_ENTRY_FORCE_REST_BOOK']='1'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')
assert abs(bot.FAST_INTERVAL - 0.10) < 1e-12
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.LIVE_MASTER_ENABLE=True
bot.live_client_ready=True
bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='lowlat'; trade_ids=('t',)
    def __init__(self,size,price):
        self.making_amount=str(size*price); self.taking_amount=str(size)
class Client:
    async def create_limit_order(self, **kw):
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        return Accepted(float(order.size), float(order.price))
bot.live_client=Client()

cid='lowlat-first'
bot.markets[cid]={'condition_id':cid,'symbol':'BTC','up_asset':'UP','down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
bot.books['UP']={'bids':{0.58:100},'asks':{0.59:100},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}
refresh_calls=[]
async def fake_refresh(asset):
    refresh_calls.append(asset)
    bot.books[asset]={'bids':{0.59:100},'asks':{0.60:100},'received_ms':bot.now_ms(),'source':'rest','tick_size':0.01}
    return True
bot.refresh_book=fake_refresh

# First accepted PRE-JUMP BUY: fresh WS book must be used, zero REST calls.
r=asyncio.run(bot.execute_live_fak(cid,V,'UP','Up','ENTRY','BUY',5.0,reference_price=0.59,force_rest=False))
assert r['ok'] and r['filled'] > 0, r
assert refresh_calls == [], refresh_calls

# Explicit retry path still refreshes REST.
cid2='lowlat-retry'
bot.markets[cid2]=dict(bot.markets[cid],condition_id=cid2)
bot.books['UP']={'bids':{0.58:100},'asks':{0.59:100},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}
r=asyncio.run(bot.execute_live_fak(cid2,V,'UP','Up','ENTRY','BUY',5.0,reference_price=0.59,force_rest=True))
assert r['ok'], r
assert refresh_calls == ['UP'], refresh_calls

print('PRE-JUMP LIVE low-latency first-entry regression: OK')
