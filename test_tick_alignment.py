import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass
from decimal import Decimal

TEST_DIR='/tmp/prejump_tick_alignment'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['LIVE_ENTRY_MAX_SLIPPAGE']='0.01'
os.environ['LIVE_PRICE_TICK_FALLBACK']='0.01'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
V=bot.STRATEGIES_BY_SYMBOL['ETH'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.LIVE_MASTER_ENABLE=True
bot.live_client_ready=True
bot.sdk_post_order_with_allowance_recovery=None

@dataclass(frozen=True)
class Signed:
    token_id:str; price:str; size:str; side:str; post_only:bool=False; order_type:str='LIMIT'
class Accepted:
    ok=True; code=''; message=''; status='matched'; order_id='tick-ok'; trade_ids=('tick',)
    def __init__(self, size, price):
        self.making_amount=str(Decimal(str(size))*Decimal(str(price)))
        self.taking_amount=str(size)
class TickClient:
    def __init__(self): self.price=None; self.posts=0
    async def create_limit_order(self, **kw):
        self.price=str(kw['price'])
        d=Decimal(self.price)
        # Simulate SDK requirement: exact 0.01 increments / <=2 decimals.
        assert d % Decimal('0.01') == 0, self.price
        assert max(0, -d.as_tuple().exponent) <= 2, self.price
        return Signed(str(kw['token_id']),self.price,str(kw['size']),str(kw['side']),False)
    async def post_order(self, order):
        self.posts += 1
        return Accepted(order.size, order.price)

client=TickClient(); bot.live_client=client
condition='tick-safe'
bot.markets[condition]={
    'condition_id':condition,'symbol':'ETH','up_asset':'UPT','down_asset':'DNT',
    'start_ts':time.time()-20,'end_ts':time.time()+280,
}
async def fake_refresh(asset):
    # A valid 0.64 ask, but reference 0.635 + 0.01 produces raw cap 0.645.
    # The bot must floor BUY to 0.64 for tick=0.01.
    bot.books[asset]={'bids':{0.63:100.0},'asks':{0.64:100.0},'received_ms':bot.now_ms(),'source':'rest-test','tick_size':0.01}
    return True
bot.refresh_book=fake_refresh
res=asyncio.run(bot.execute_live_fak(condition,V,'UPT','Up','ENTRY','BUY',5.0,reference_price=0.635,force_rest=True))
assert res['ok'], res
assert client.price == '0.64', client.price
assert client.posts == 1

# A local create/sign validation failure is definitely pre-submission and must
# not poison the market/action as AMBIGUOUS.
class LocalRejectClient(TickClient):
    async def create_limit_order(self, **kw):
        raise ValueError('local validation test')
local=LocalRejectClient(); bot.live_client=local
condition2='local-reject'
bot.markets[condition2]=dict(bot.markets[condition],condition_id=condition2)
res=asyncio.run(bot.execute_live_fak(condition2,V,'UPT','Up','ENTRY','BUY',5.0,reference_price=0.635,force_rest=True))
assert not res['ok'] and res['status']=='REJECTED_LOCAL', res
assert local.posts == 0
assert not bot.live_action_ambiguous(condition2,V['name'],'BUY','ENTRY')

print('PRE-JUMP LIVE tick alignment/local rejection regression: OK')
