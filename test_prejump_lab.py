import os
import time
import asyncio
import tempfile
import importlib.util
import zipfile
from pathlib import Path

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='prejump_lab_test_')
os.environ['TELEGRAM_BOT_TOKEN'] = ''
os.environ['TELEGRAM_CHAT_ID'] = ''
os.environ['SYMBOLS'] = 'BTC,XRP,BNB,SOL,ETH,DOGE,HYPE'
os.environ['PAPER_START_BALANCE'] = '500'
os.environ['TAKE_PROFIT_USDC'] = '0.60'

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bot', HERE/'main.py')
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)
bot.init_db()

assert bot.VERSION == '1.0-multi7-prejump-lab'
assert len(bot.STRATEGIES) == 28
assert bot.trading_enabled()
assert bot.STRATEGY_CODES == ('BASE','EXT_CONFIRM','EXT_VETO','PRE_JUMP')

slot = (int(time.time()) // 300) * 300
counter = 0

def market(symbol, tag):
    global counter
    counter += 1
    m = {
        'condition_id': f'cid-{symbol}-{tag}-{counter}',
        'symbol': symbol,
        'question': f'{symbol} Up or Down test',
        'slug': f"{bot.ASSET_CONFIG[symbol]['prefix']}-{slot}",
        'start_ts': slot,
        'end_ts': slot + 300,
        'up_asset': f'{symbol}-UP-{tag}-{counter}',
        'down_asset': f'{symbol}-DN-{tag}-{counter}',
    }
    bot.markets[m['condition_id']] = m
    bot.persist_market(m)
    return m

def pm_book(asset, bid, ask, size=100):
    bot.books[asset] = {
        'bids': {float(bid): float(size)},
        'asks': {float(ask): float(size)},
        'received_ms': bot.now_ms(),
        'source': 'test',
    }

def seed_base(m, up=(.61,.64,.69), down=(.39,.36,.31)):
    now = bot.now_ms()
    pm_book(m['up_asset'], up[-1]-.01, up[-1])
    pm_book(m['down_asset'], down[-1]-.01, down[-1])
    hu = bot.base_price_history[m['condition_id']][m['up_asset']]
    hd = bot.base_price_history[m['condition_id']][m['down_asset']]
    hu.clear(); hd.clear()
    hu.extend([(now-6000,up[0]),(now-3000,up[1]),(now,up[2])])
    hd.extend([(now-6000,down[0]),(now-3000,down[1]),(now,down[2])])

def strong_up_feature(symbol):
    now = bot.now_ms()
    for venue in ('binance','bybit'):
        bot.venue_sample_history[venue][symbol].clear()
        bot.venue_sample_history[venue][symbol].extend([
            {'sample_ms':now-10000,'price':99.5,'bid_depth':100000,'ask_depth':100000},
            {'sample_ms':now-3000,'price':99.8,'bid_depth':100000,'ask_depth':100000},
            {'sample_ms':now-1000,'price':100.0,'bid_depth':100000,'ask_depth':100000},
        ])
        bot.replace_external_book(
            venue, symbol,
            [[100.09,2000],[100.08,1000]],
            [[100.11,300],[100.12,300]], now,
        )
        bot.venue_last_price[venue][symbol] = 100.10
        for i in range(20):
            bot.update_external_trade(venue,symbol,now-i*50,100.10,10,'BUY')
    f = bot.build_external_snapshot(symbol, now)
    for venue in ('binance','bybit','coinbase'):
        bot.append_venue_feature_history(venue,symbol,f.get(venue))
    f = bot.enrich_feature_with_polymarket(f, None, None)
    bot.feature_history[symbol].append(f)
    return f

# External feature score is strongly bullish on 2 venues.
f = strong_up_feature('BTC')
assert f['ext_score'] > .55
assert f['up_votes'] >= 2
assert bot.directional_external(f,'Up')['score'] > .55
assert bot.directional_external(f,'Down')['score'] < -.55

# BASE family: all three should enter when SAFE67 + bullish external confirmation.
m1 = market('BTC','base-good')
seed_base(m1)
f = strong_up_feature('BTC')
asyncio.run(bot.evaluate_base_family(m1, 30.0))
for code in ('BASE','EXT_CONFIRM','EXT_VETO'):
    s = next(x for x in bot.STRATEGIES_BY_SYMBOL['BTC'] if x['code']==code)
    pos = bot.position_totals(m1['condition_id'], s['name'])
    assert abs(pos['bought'] - 5.0) < 1e-9, (code,pos)

# EXT_CONFIRM must skip if the external venues do not confirm.
m2 = market('XRP','confirm-miss')
seed_base(m2)
bot.feature_history['XRP'].append({
    'sample_ms':bot.now_ms(),'symbol':'XRP','ext_score':0.0,'up_votes':0,'down_votes':0,
    'fresh_venues':2,'fresh_names':['binance','bybit'],
    'binance':{'fresh':True,'score':0.0},'bybit':{'fresh':True,'score':0.0},'coinbase':None,
})
asyncio.run(bot.evaluate_base_family(m2, 30.0))
base = next(x for x in bot.STRATEGIES_BY_SYMBOL['XRP'] if x['code']=='BASE')
confirm = next(x for x in bot.STRATEGIES_BY_SYMBOL['XRP'] if x['code']=='EXT_CONFIRM')
veto = next(x for x in bot.STRATEGIES_BY_SYMBOL['XRP'] if x['code']=='EXT_VETO')
assert bot.position_totals(m2['condition_id'],base['name'])['bought'] == 5
assert bot.position_totals(m2['condition_id'],confirm['name'])['bought'] == 0
assert bot.position_totals(m2['condition_id'],veto['name'])['bought'] == 5
with bot.db() as conn:
    row = conn.execute('SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?',(m2['condition_id'],confirm['name'])).fetchone()
assert row['reason'] == 'EXT_CONFIRM_MISSING' and row['passed'] == 0

# EXT_VETO blocks a SAFE67 signal if external flow strongly points the other way.
m3 = market('ETH','veto')
seed_base(m3)
now = bot.now_ms()
opp = {
    'sample_ms':now,'symbol':'ETH','ext_score':-.75,'up_votes':0,'down_votes':2,
    'fresh_venues':2,'fresh_names':['binance','bybit'],
    'binance':{'fresh':True,'score':-.8},'bybit':{'fresh':True,'score':-.7},'coinbase':None,
}
bot.feature_history['ETH'].append(opp)
asyncio.run(bot.evaluate_base_family(m3, 30.0))
veto3 = next(x for x in bot.STRATEGIES_BY_SYMBOL['ETH'] if x['code']=='EXT_VETO')
assert bot.position_totals(m3['condition_id'],veto3['name'])['bought'] == 0
with bot.db() as conn:
    row = conn.execute('SELECT * FROM gate_decisions WHERE condition_id=? AND variant=?',(m3['condition_id'],veto3['name'])).fetchone()
assert row['reason'] == 'EXT_VETO_BLOCK'

# PRE_JUMP enters early at 0.60 when Binance+Bybit are strongly aligned.
m4 = market('SOL','prejump')
pm_book(m4['up_asset'],.59,.60)
pm_book(m4['down_asset'],.39,.40)
now = bot.now_ms()
h = bot.fast_pm_history[m4['condition_id']][m4['up_asset']]
h.extend([(now-1000,.59),(now,.60)])
f = strong_up_feature('SOL')
# enrich with this market after helper made a no-market snapshot.
f = bot.enrich_feature_with_polymarket(f,m4,30.0)
bot.feature_history['SOL'].append(f)
asyncio.run(bot.evaluate_prejump(m4,30.0))
pre = next(x for x in bot.STRATEGIES_BY_SYMBOL['SOL'] if x['code']=='PRE_JUMP')
pos = bot.position_totals(m4['condition_id'],pre['name'])
assert pos['bought'] == 5
assert pos['buys'][0]['avg_price'] <= .60 + 1e-9

# PAPER TP is a whole-position NET target. 0.70 bid isn't enough; 0.75 is.
pm_book(m4['up_asset'],.70,.71)
mark = bot.projected_full_exit(m4['condition_id'],pre['name'])
assert mark and mark['total_pnl'] < .60
assert not asyncio.run(bot.maybe_take_profit(m4,pre))
pm_book(m4['up_asset'],.75,.76)
mark = bot.projected_full_exit(m4['condition_id'],pre['name'])
assert mark and mark['total_pnl'] >= .60
assert asyncio.run(bot.maybe_take_profit(m4,pre))
assert bot.position_totals(m4['condition_id'],pre['name'])['remaining'] <= 1e-9

# Jump detector stores 1/3/5/10/20 second pre-context fields.
m5 = market('DOGE','jump')
now = bot.now_ms()
for sec in (20,10,5,3,1,0):
    bot.feature_history['DOGE'].append({
        'sample_ms':now-sec*1000,'symbol':'DOGE','ext_score':0.1+sec/100,
        'up_votes':1,'down_votes':0,'fresh_venues':2,
    })
hj = bot.fast_pm_history[m5['condition_id']][m5['up_asset']]
hj.extend([(now-5000,.60),(now,.69)])
bot.detect_jump(m5,m5['up_asset'],'Up',30.0)
with bot.db() as conn:
    jump = conn.execute('SELECT * FROM jump_events WHERE condition_id=?',(m5['condition_id'],)).fetchone()
assert jump is not None
assert abs(jump['move'] - .09) < 1e-9
assert 'ext_score' in jump['pre_10s_json']

# Trial horizon updater recognizes executable TP opportunity within the horizon.
# Use a fresh BASE trade so trial remains active.
m6 = market('BNB','trial')
seed_base(m6)
f = strong_up_feature('BNB')
asyncio.run(bot.evaluate_base_family(m6,30.0))
base6 = next(x for x in bot.STRATEGIES_BY_SYMBOL['BNB'] if x['code']=='BASE')
pm_book(m6['up_asset'],.85,.86)
# Make the trial look 2 seconds old so it is eligible for all 3/5/10/20 horizons.
with bot.db() as conn:
    tr = conn.execute('SELECT * FROM trials WHERE condition_id=? AND variant=?',(m6['condition_id'],base6['name'])).fetchone()
    conn.execute('UPDATE trials SET entry_ms=? WHERE id=?',(bot.now_ms()-2000,tr['id']))
    conn.commit()
bot.update_trial_horizons()
with bot.db() as conn:
    tr2 = conn.execute('SELECT * FROM trials WHERE id=?',(tr['id'],)).fetchone()
assert tr2['hit_tp_3s'] == 1 and tr2['hit_tp_20s'] == 1

# Hourly ZIP contains the datasets needed for offline lead/lag analysis.
hour = (int(time.time())//3600)*3600
path,summaries = bot.make_report(hour,hour+3600)
assert len(summaries) == 28
with zipfile.ZipFile(path,'r') as z:
    names=set(z.namelist())
required = {
    'report.txt','strategy_summary.csv','external_features.csv','jump_events.csv',
    'signal_events.csv','paper_trades.csv','paper_exits.csv',
    'trials_3_5_10_20s.csv','market_results.csv','source_health.csv',
}
assert required.issubset(names), required-names

print('MULTI7 PRE-JUMP LAB regression: OK')
print(f"Strong external composite example: {f['ext_score']:+.3f}")
print('BASE / EXT_CONFIRM / EXT_VETO / PRE_JUMP paths: OK')
print('Jump pre-context 1/3/5/10/20s: OK')
print('PAPER NET TP + horizon labels: OK')
