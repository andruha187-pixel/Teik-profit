import os, shutil, asyncio, importlib.util, time

TEST_DIR='/tmp/prejump_event_driven'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['PREJUMP_SCORE']='0.40'
os.environ['FAST_INTERVAL']='0.10'
os.environ['EVENT_DRIVEN_LIVE_ENTRY']='1'
os.environ['EVENT_DRIVEN_MIN_INTERVAL_MS']='0'
os.environ['LIVE_ENTRY_MAX_SLIPPAGE']='0.05'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')
assert bot.EVENT_DRIVEN_LIVE_ENTRY
assert abs(bot.LIVE_ENTRY_MAX_SLIPPAGE - 0.05) < 1e-12
V=bot.STRATEGIES_BY_SYMBOL['BTC'][0]
bot.state_set(f"mode:{V['name']}", 'LIVE')
bot.state_set('trading_enabled','1')

cid='evt'
start=time.time()-30
bot.markets[cid]={'condition_id':cid,'symbol':'BTC','up_asset':'UP','down_asset':'DN','start_ts':start,'end_ts':start+300}
now=bot.now_ms()
bot.books['UP']={'bids':{0.53:100},'asks':{0.54:100},'received_ms':now,'source':'ws','tick_size':0.01}
bot.books['DN']={'bids':{0.42:100},'asks':{0.43:100},'received_ms':now,'source':'ws','tick_size':0.01}
# Give PM momentum a 1-second reference at the same price.
bot.fast_pm_history[cid]['UP'].append((now-1000,0.54))
bot.fast_pm_history[cid]['UP'].append((now,0.54))

feature={
    'sample_ms':now,'symbol':'BTC','ext_score':0.44,'up_votes':2,'down_votes':0,'fresh_venues':3,
    'fresh_names':['binance','bybit','coinbase'],
    'binance':{'fresh':True,'score':0.44},
    'bybit':{'fresh':True,'score':0.44},
    'coinbase':{'fresh':True,'score':0.10},
}
bot.build_external_snapshot=lambda symbol, sample_ms=None: dict(feature)

calls=[]
async def fake_execute(condition, variant, asset, outcome, signal_type, reference_price=None, **kwargs):
    calls.append((condition, asset, outcome, reference_price, kwargs))
    return True
bot.execute_order=fake_execute

trigger=bot.now_ms()-4
asyncio.run(bot._event_driven_evaluate_symbol_once('BTC', trigger))
assert len(calls)==1, calls
kw=calls[0][4]
assert kw.get('evaluation_path')=='event', kw
assert kw.get('event_received_ms')==trigger, kw
assert kw.get('signal_detected_ms') is not None
assert 0 <= kw['signal_detected_ms']-trigger < 500
print('PRE-JUMP event-driven LIVE entry regression: OK')
