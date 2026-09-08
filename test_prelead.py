import os, tempfile, importlib.util, asyncio, zipfile
from pathlib import Path

_tmp = tempfile.mkdtemp(prefix='prelead_safe_test_')
os.environ['DATA_DIR'] = _tmp
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''

spec = importlib.util.spec_from_file_location('lab', Path(__file__).with_name('main.py'))
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

assert lab.STRATEGY_CODES == (
    'PRE_JUMP','PRE_JUMP42','PRE_JUMP10','PRE_JUMP42_10','PRE_LEAD','PRE_LEAD_SAFE'
)
assert abs(lab.PRELEAD_SAFE_PROJECTED_SCORE - 0.55) < 1e-12
assert abs(lab.PRELEAD_SAFE_PRICE_MAX - 0.56) < 1e-12

# Original PRE_LEAD math remains unchanged: 0.36 -> 0.38 over 300ms projects to 0.40.
sym='BTC'; now=1_000_000
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': 0.36})
current={'sample_ms': now, 'ext_score': 0.38}
lab.lead_feature_history[sym].append(current)
ok, d = lab.prelead_projection(sym, current, 'Up')
assert ok, d
assert abs(d['projected_score'] - 0.40) < 1e-9, d

# SAFE overlay rejects a normal PRE_LEAD whose projection is below 0.55.
ok_safe, why = lab.prelead_safe_filter(d, 0.54)
assert not ok_safe and why == 'safe_projection_below_min', (ok_safe, why, d)

# A strong projection at a cheap ask passes SAFE.
strong = dict(d); strong['projected_score'] = 0.55
ok_safe, why = lab.prelead_safe_filter(strong, 0.56)
assert ok_safe, (ok_safe, why)

# Price just above 0.56 fails even with a strong projection.
ok_safe, why = lab.prelead_safe_filter(strong, 0.57)
assert not ok_safe and why == 'safe_price_above_max', (ok_safe, why)

# Down direction remains symmetric in the shared PRE_LEAD logic.
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': -0.36})
cur_dn={'sample_ms': now, 'ext_score': -0.38}
lab.lead_feature_history[sym].append(cur_dn)
ok, d = lab.prelead_projection(sym, cur_dn, 'Down')
assert ok, d

# Limit-price simulator never consumes above cap.
lab.books['A']={'asks':{0.54:3.0,0.56:3.0,0.60:10.0}, 'bids':{}, 'received_ms':lab.now_ms()}
fills, filled = lab.simulate_buy('A', 5.0, max_price=0.56)
assert abs(filled-5.0)<1e-9 and all(p<=0.56 for p,q in fills), (fills,filled)

lab.init_db()
path, summaries = lab.make_report(0, 3600)
assert path.exists()
with zipfile.ZipFile(path) as z:
    names=set(z.namelist())
    for name in ('prelead_execution.csv','prelead_alignment.csv',
                 'prelead_safe_execution.csv','prelead_safe_alignment.csv'):
        assert name in names, name

# Both lead branches have separate PAPER accounts after init.
assert lab.paper_cash('BTC_PRE_LEAD') == lab.PAPER_START_BALANCE
assert lab.paper_cash('BTC_PRE_LEAD_SAFE') == lab.PAPER_START_BALANCE

# Delayed execution helper works independently for PRE_LEAD_SAFE too.
lab.PRELEAD_SIM_DELAY_MS = 0
market={
    'condition_id':'safe-test-cid','symbol':'BTC','start_ts':lab.time.time()-10,'end_ts':lab.time.time()+290,
    'up_asset':'UPA','down_asset':'DNA'
}
strategy=next(x for x in lab.STRATEGIES_BY_SYMBOL['BTC'] if x['code']=='PRE_LEAD_SAFE')
lab.markets[market['condition_id']]=market
lab.books['UPA']={'asks':{0.56:10.0},'bids':{0.55:10.0},'received_ms':lab.now_ms(),'source':'test'}
feat={'sample_ms':lab.now_ms(),'ext_score':0.38,'up_votes':2,'down_votes':0,'fresh_venues':3,'fresh_names':['binance','bybit','coinbase']}
asyncio.run(lab._execute_prelead_after_delay(market,strategy,'UPA','Up',0.56,feat,lab.now_ms()))
with lab.db() as c:
    tr=c.execute("SELECT * FROM paper_trades WHERE condition_id=? AND variant=?",('safe-test-cid',strategy['name'])).fetchone()
    ex=c.execute("SELECT * FROM lead_exec_events WHERE condition_id=? AND variant=?",('safe-test-cid',strategy['name'])).fetchone()
assert tr is not None and ex is not None and ex['status']=='FILLED'
assert abs(float(ex['cap_price']) - 0.61) < 1e-9, ex['cap_price']

print('OK PRE_LEAD + PRE_LEAD_SAFE tests')
