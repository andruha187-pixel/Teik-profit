import os, tempfile, importlib.util
from pathlib import Path

_tmp = tempfile.mkdtemp(prefix='prelead_test_')
os.environ['DATA_DIR'] = _tmp
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''

spec = importlib.util.spec_from_file_location('lab', Path(__file__).with_name('main.py'))
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

assert lab.STRATEGY_CODES[:4] == ('PRE_JUMP','PRE_JUMP42','PRE_JUMP10','PRE_JUMP42_10')
assert lab.STRATEGY_CODES[4] == 'PRE_LEAD'

# A clean 300ms ramp 0.36 -> 0.38 projects to 0.40 and should pass.
sym='BTC'; now=1_000_000
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': 0.36})
current={'sample_ms': now, 'ext_score': 0.38}
lab.lead_feature_history[sym].append(current)
ok, d = lab.prelead_projection(sym, current, 'Up')
assert ok, d
assert abs(d['projected_score'] - 0.40) < 1e-9, d

# Flat/weak ramp should not pass.
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': 0.375})
lab.lead_feature_history[sym].append(current)
ok, d = lab.prelead_projection(sym, current, 'Up')
assert not ok and d['reason'] in {'delta_too_small','projection_below_target'}, d

# If normal 0.40 is already crossed, PRE_LEAD must not claim an early signal.
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': 0.38})
cur_hi={'sample_ms': now, 'ext_score': 0.405}
lab.lead_feature_history[sym].append(cur_hi)
ok, d = lab.prelead_projection(sym, cur_hi, 'Up')
assert not ok and d['reason'] == 'target_already_crossed', d

# Down direction is symmetric.
lab.lead_feature_history[sym].clear()
lab.lead_feature_history[sym].append({'sample_ms': now-300, 'ext_score': -0.36})
cur_dn={'sample_ms': now, 'ext_score': -0.38}
lab.lead_feature_history[sym].append(cur_dn)
ok, d = lab.prelead_projection(sym, cur_dn, 'Down')
assert ok, d

# Limit-price simulator must never consume levels above the cap.
lab.books['A']={'asks':{0.54:3.0,0.56:3.0,0.60:10.0}, 'bids':{}, 'received_ms':lab.now_ms()}
fills, filled = lab.simulate_buy('A', 5.0, max_price=0.56)
assert abs(filled-5.0)<1e-9 and all(p<=0.56 for p,q in fills), (fills,filled)
fills2, filled2 = lab.simulate_buy('A', 7.0, max_price=0.56)
assert abs(filled2-6.0)<1e-9, (fills2,filled2)

lab.init_db()
# Empty report should still be generated with new PRE_LEAD files.
path, summaries = lab.make_report(0, 3600)
assert path.exists()
import zipfile
with zipfile.ZipFile(path) as z:
    names=set(z.namelist())
    assert 'prelead_execution.csv' in names
    assert 'prelead_alignment.csv' in names

print('OK PRE_LEAD tests')

# Delayed execution helper: with a fresh in-cap book, the PRE_LEAD paper order fills
# and a lead_exec_events audit row is written.
import asyncio
lab.PRELEAD_SIM_DELAY_MS = 0
market={
    'condition_id':'test-cid','symbol':'BTC','start_ts':lab.time.time()-10,'end_ts':lab.time.time()+290,
    'up_asset':'UPA','down_asset':'DNA'
}
strategy=next(x for x in lab.STRATEGIES_BY_SYMBOL['BTC'] if x['code']=='PRE_LEAD')
lab.markets[market['condition_id']]=market
lab.books['UPA']={'asks':{0.54:10.0},'bids':{0.53:10.0},'received_ms':lab.now_ms(),'source':'test'}
feat={'sample_ms':lab.now_ms(),'ext_score':0.38,'up_votes':2,'down_votes':0,'fresh_venues':3,'fresh_names':['binance','bybit','coinbase']}
asyncio.run(lab._execute_prelead_after_delay(market,strategy,'UPA','Up',0.54,feat,lab.now_ms()))
with lab.db() as c:
    tr=c.execute("SELECT * FROM paper_trades WHERE condition_id=? AND variant=?",('test-cid',strategy['name'])).fetchone()
    ex=c.execute("SELECT * FROM lead_exec_events WHERE condition_id=? AND variant=?",('test-cid',strategy['name'])).fetchone()
assert tr is not None and ex is not None and ex['status']=='FILLED'
print('OK delayed execution test')
