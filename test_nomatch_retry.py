import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass

TEST_DIR='/tmp/prejump_nomatch_retry'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['LIVE_ENTRY_MAX_SLIPPAGE']='0.01'
os.environ['LIVE_ENTRY_NO_MATCH_RETRIES']='1'
os.environ['LIVE_ENTRY_RETRY_DELAY_MS']='0'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.LIVE_MASTER_ENABLE=True
bot.live_client_ready=True
bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='ok2'; trade_ids=('t2',)
    def __init__(self, size, execution_price):
        self.making_amount=str(size*execution_price)
        self.taking_amount=str(size)
class RetryClient:
    def __init__(self): self.posts=0; self.prices=[]
    async def create_limit_order(self, **kw):
        self.prices.append(float(kw['price']))
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts += 1
        if self.posts == 1:
            raise RuntimeError('RequestRejectedError: no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found')
        return Accepted(float(order.size), 0.60)

client=RetryClient(); bot.live_client=client
condition='retry-ok'
bot.markets[condition]={
    'condition_id':condition,'symbol':'BTC','up_asset':'UPR','down_asset':'DNR',
    'start_ts':time.time()-20,'end_ts':time.time()+280,
}
async def fake_refresh(asset):
    bot.books[asset]={'bids':{0.59:100.0},'asks':{0.60:100.0},'received_ms':bot.now_ms(),'source':'rest-test'}
    return True
bot.refresh_book=fake_refresh
feature={
    'ext_score':0.45,'up_votes':2,'down_votes':0,'fresh_venues':2,
    'binance':{'fresh':True,'score':0.40},'bybit':{'fresh':True,'score':0.41},'coinbase':None,
}
bot.latest_feature=lambda symbol: feature

ok=asyncio.run(bot.execute_order(condition,V,'UPR','Up','ENTRY',reference_price=0.60))
assert ok, 'one deterministic NO_MATCH should be retried and fill'
assert client.posts == 2, client.posts
assert all(p <= 0.61 + 1e-12 for p in client.prices), client.prices
with bot.db() as conn:
    rows=conn.execute("SELECT status,filled_shares FROM live_orders WHERE condition_id=? ORDER BY id",(condition,)).fetchall()
assert len(rows)==2
assert rows[0]['status']=='REJECTED_NO_MATCH' and float(rows[0]['filled_shares'])==0
assert float(rows[1]['filled_shares'])==5
assert not bot.live_action_ambiguous(condition,V['name'],'BUY','ENTRY')

# Unknown transport error must remain fail-closed and never retry.
class BadClient(RetryClient):
    async def post_order(self, order):
        self.posts += 1
        raise TimeoutError('socket timeout after submission')
bad=BadClient(); bot.live_client=bad
condition2='retry-blocked'
bot.markets[condition2]=dict(bot.markets[condition],condition_id=condition2,start_ts=time.time()-20)
ok=asyncio.run(bot.execute_order(condition2,V,'UPX','Up','ENTRY',reference_price=0.60))
assert not ok
assert bad.posts==1
assert bot.live_action_ambiguous(condition2,V['name'],'BUY','ENTRY')

print('PRE-JUMP LIVE NO_MATCH retry regression: OK')
