import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass
from decimal import Decimal

DATA='/tmp/prejump_v203_data'
shutil.rmtree(DATA, ignore_errors=True); os.makedirs(DATA,exist_ok=True)
os.environ['DATA_DIR']=DATA
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['TAKE_PROFIT_USDC']='0.60'
spec=importlib.util.spec_from_file_location('bot',os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}",'LIVE')
bot.LIVE_MASTER_ENABLE=True; bot.live_client_ready=True; bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='x'; trade_ids=('t',)
    def __init__(self, side,size,price):
        size=Decimal(str(size)); price=Decimal(str(price))
        if side=='BUY': self.making_amount=str(size*price); self.taking_amount=str(size)
        else: self.making_amount=str(size); self.taking_amount=str(size*price)
class Client:
    def __init__(self): self.posts=[]; self.sell_attempts=0
    async def create_limit_order(self, **kw):
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts.append(order.side)
        if order.side=='BUY': return Accepted('BUY',order.size,order.price)
        self.sell_attempts += 1
        if self.sell_attempts == 1:
            raise RuntimeError('RequestRejectedError: not enough balance / allowance: the balance is not enough -> balance: 0, order amount: 5000000')
        return Accepted('SELL',order.size,order.price)

c=Client(); bot.live_client=c
cid='btc-tp-sync'; asset='DN'
bot.markets[cid]={'condition_id':cid,'symbol':'BTC','up_asset':'UP','down_asset':asset,'start_ts':time.time()-30,'end_ts':time.time()+270}
bot.books[asset]={'bids':{0.88:100.0},'asks':{0.89:100.0},'received_ms':bot.now_ms(),'tick_size':0.01}
# Buy at 0.55 by temporarily using book.
bot.books[asset]['asks']={0.55:100.0}; bot.books[asset]['bids']={0.54:100.0}
res=asyncio.run(bot.execute_live_fak(cid,V,asset,'Down','ENTRY','BUY',5.0))
assert res['ok'] and abs(res['filled']-5)<1e-9
# Restore profitable bid.
bot.books[asset]={'bids':{0.88:100.0},'asks':{0.89:100.0},'received_ms':bot.now_ms(),'tick_size':0.01}
# prevent REST dependency
async def no_refresh(_asset): return 0
bot.ensure_sell_book=no_refresh
# Immediate TP must be delayed, no SELL post.
bot.LIVE_TP_MIN_HOLD_MS=2000
r=asyncio.run(bot.maybe_take_profit(bot.markets[cid],V,30))
assert not r and c.sell_attempts==0, (r,c.posts)
# After hold, first SELL gets deterministic balance/allowance reject; must NOT be ambiguous.
bot.LIVE_TP_MIN_HOLD_MS=0; bot.LIVE_TP_BALANCE_RETRY_DELAY_MS=0
r=asyncio.run(bot.maybe_take_profit(bot.markets[cid],V,31))
assert not r and c.sell_attempts==1
assert not bot.live_action_ambiguous(cid,V['name'],'SELL','TAKE_PROFIT')
with bot.db() as conn:
    row=conn.execute("SELECT status FROM live_orders WHERE condition_id=? AND action='SELL' ORDER BY id DESC LIMIT 1",(cid,)).fetchone()
assert row['status']=='REJECTED_BALANCE_ALLOWANCE', row['status']
# Later TP cycle retries and fills.
bot.live_tp_retry_after_ms[(cid,V['name'],'TAKE_PROFIT')]=0
bot.books[asset]['received_ms']=bot.now_ms()
r=asyncio.run(bot.maybe_take_profit(bot.markets[cid],V,32))
assert r and c.sell_attempts==2
pos=bot.position_totals(cid,V['name'])
assert pos['remaining'] < 1e-8, pos

# Migration must repair an old v20.2-style ambiguous TP balance rejection so an
# already-open position is not permanently poisoned after redeploy.
with bot.db() as conn:
    conn.execute("""INSERT INTO live_orders(
        submitted_ms,condition_id,variant,symbol,asset,outcome,action,reason,
        requested_shares,limit_price,order_id,status,filled_shares,avg_price,
        gross_amount,fee_estimate,net_or_total,trade_ids_json,response_json,error
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
        bot.now_ms(),'old-poison',V['name'],'BTC','DNO','Down','SELL','TAKE_PROFIT',
        5,0.8,'','AMBIGUOUS',0,None,0,0,0,'[]','{}',
        'RequestRejectedError: not enough balance / allowance: balance: 0, order amount: 5000000'
    ))
    conn.commit()
bot.init_db()
with bot.db() as conn:
    migrated=conn.execute("SELECT status FROM live_orders WHERE condition_id='old-poison'").fetchone()['status']
assert migrated=='REJECTED_BALANCE_ALLOWANCE', migrated
assert not bot.live_action_ambiguous('old-poison',V['name'],'SELL','TAKE_PROFIT')

print('PRE-JUMP LIVE TP balance propagation regression: OK')
