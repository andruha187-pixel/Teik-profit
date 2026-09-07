import os, shutil, asyncio, importlib.util, time
from dataclasses import dataclass

TEST_DIR='/tmp/prejump_presign_prewarm'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['LIVE_PRESIGN_PREWARM_ENABLE']='1'
os.environ['LIVE_PRESIGN_PREWARM_LEAD_SEC']='12'
os.environ['LIVE_PREWARM_ENABLE']='0'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')

V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.live_client_ready=True
bot.LIVE_MASTER_ENABLE=True

@dataclass(frozen=True)
class Signed:
    token_id:str
    price:str
    size:str
    side:str
    post_only:bool=False
    order_type:str='LIMIT'

class Client:
    def __init__(self):
        self.builds=[]
        self.posts=0
    async def create_limit_order(self, **kw):
        self.builds.append(dict(kw))
        await asyncio.sleep(0)
        return Signed(str(kw['token_id']),str(kw['price']),str(kw['size']),str(kw['side']),False)
    async def post_order(self, *args, **kwargs):
        self.posts += 1
        raise AssertionError('prewarm must NEVER post an order')

client=Client(); bot.live_client=client
slot=((int(time.time())//300)+1)*300
cid='presign-market'
market={'condition_id':cid,'symbol':'BTC','up_asset':'UPTOKEN','down_asset':'DNTOKEN','start_ts':slot,'end_ts':slot+300}
bot.markets[cid]=market
bot.books['UPTOKEN']={'bids':{0.49:10},'asks':{0.51:10},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}
bot.books['DNTOKEN']={'bids':{0.49:10},'asks':{0.51:10},'received_ms':bot.now_ms(),'source':'ws','tick_size':0.01}

warmed, expected = asyncio.run(bot.prewarm_live_slot_signers(slot))
assert (warmed, expected)==(2,2), (warmed, expected)
assert len(client.builds)==2, client.builds
assert client.posts==0
assert {'UPTOKEN','DNTOKEN'} <= bot.live_presign_warmed_assets
assert all(x['side']=='BUY' for x in client.builds)
assert all(float(x['price'])==0.50 for x in client.builds)
# Idempotent: repeated slot call does not build again.
warmed2, expected2 = asyncio.run(bot.prewarm_live_slot_signers(slot))
assert (warmed2, expected2)==(2,2)
assert len(client.builds)==2
print('PRE-JUMP v20.11 local-only per-token presign prewarm: OK')
