import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass

TEST_DIR='/tmp/prejump_ultra_retry'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['LIVE_ENTRY_MAX_SLIPPAGE']='0.05'
os.environ['LIVE_ENTRY_NO_MATCH_RETRIES']='1'
os.environ['LIVE_ENTRY_RETRY_DELAY_MS']='0'
os.environ['LIVE_ENTRY_RETRY_FORCE_REST']='0'
os.environ['LIVE_PREWARM_ENABLE']='0'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')
V=bot.STRATEGIES_BY_SYMBOL['DOGE'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.LIVE_MASTER_ENABLE=True
bot.live_client_ready=True
bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='retry2'; trade_ids=('t2',)
    def __init__(self,size,price):
        self.making_amount=str(size*price); self.taking_amount=str(size)
class Client:
    def __init__(self): self.posts=0
    async def create_limit_order(self, **kw):
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts += 1
        if self.posts == 1:
            raise RuntimeError('RequestRejectedError: no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found')
        return Accepted(float(order.size), min(float(order.price),0.58))
client=Client(); bot.live_client=client

cid='ultra-retry'
bot.markets[cid]={'condition_id':cid,'symbol':'DOGE','up_asset':'UP','down_asset':'DN','start_ts':time.time()-20,'end_ts':time.time()+280}
bot.books['DN']={'bids':{0.53:100},'asks':{0.54:100},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}
feature={'ext_score':-0.45,'up_votes':0,'down_votes':2,'fresh_venues':3,
         'binance':{'fresh':True,'score':-0.45},'bybit':{'fresh':True,'score':-0.46},'coinbase':{'fresh':True,'score':-0.44}}
bot.latest_feature=lambda symbol: feature
refresh_calls=[]
async def forbidden_refresh(asset):
    refresh_calls.append(asset)
    raise AssertionError('REST refresh must not run on immediate retry')
bot.refresh_book=forbidden_refresh

signal_ms=bot.now_ms()
ok=asyncio.run(bot.execute_order(cid,V,'DN','Down','ENTRY',reference_price=0.54,signal_detected_ms=signal_ms,event_received_ms=signal_ms-2,evaluation_path='event'))
assert ok
assert client.posts == 2
assert refresh_calls == []
ctx=bot.live_entry_latency[(cid,V['name'])]
assert len(ctx.get('attempts') or []) == 2, ctx
assert ctx['attempts'][0]['label'] == 'first'
assert ctx['attempts'][1]['label'] == 'retry1'
print('PRE-JUMP v20.11 immediate WS NO_MATCH retry: OK')
