import os
import time
import asyncio
import tempfile
import importlib.util

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='copybot-recovery-test-')
os.environ['RECOVERY_ENABLE'] = '1'
os.environ['RECOVERY_AMBIGUOUS_GRACE_MS'] = '1'
os.environ['RECOVERY_RECONCILE_INTERVAL_MS'] = '1'
os.environ['RECOVERY_FAK_RETRY_MS'] = '1'
os.environ['TELEGRAM_NOTIFY_TRADES'] = '0'

spec = importlib.util.spec_from_file_location('copybot_recovery', os.path.join(os.path.dirname(__file__), 'main.py'))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

W = '0x' + 'a' * 40
A1 = 'asset-reconcile'
A2 = 'asset-retry'


def payload(asset, tx):
    return {
        'proxyWallet': W,
        'transactionHash': tx,
        'asset': asset,
        'conditionId': '0x' + '3' * 64,
        'side': 'BUY', 'price': 0.95, 'size': 157.88,
        'timestamp': int(time.time()),
        'title': 'Bitcoin Up or Down - recovery test',
        'slug': 'btc-updown-15m-9999999999',
        'outcome': 'Down',
    }


def get_job(asset):
    with bot.db() as conn:
        r = conn.execute('SELECT * FROM recovery_orders WHERE asset=? ORDER BY id DESC LIMIT 1', (asset,)).fetchone()
        return dict(r) if r else None


async def main():
    bot.init_db()
    bot.load_runtime_caches()
    bot.set_setting('running', '1')
    bot.set_setting('mode', 'LIVE')
    bot.set_setting('notify_trades', '0')
    bot.LIVE_MASTER_ENABLE = True
    bot.live_client_ready = True
    bot.live_client = object()

    # Timestamp audit helper: a 2-second-old public timestamp must show an age.
    p_age = payload('asset-age', '0xage')
    p_age['timestamp'] = int(time.time()) - 2
    age = bot.estimated_source_age_ms(p_age, bot.now_ms())
    assert age is not None and 1000 <= age <= 3500, age

    # 1) AMBIGUOUS: recovery MUST reconcile our own fills before any new POST.
    p1 = payload(A1, '0xamb')
    ok = bot.recovery_schedule_db(
        key='evt-amb', wallet=W, info={'label':'phantom.1'}, payload=p1, source='rest:activity',
        requested=10.0, accounted=0.0, accounted_gross=0.0, limit_price=0.99,
        mode='LIVE', initial_status='AMBIGUOUS', initial_error='timeout after POST', detected_ms=bot.now_ms(),
    )
    assert ok
    j1 = get_job(A1)
    assert j1 and j1['state'] == 'RECONCILE'

    submit_calls = []
    original_reconcile = bot.own_recent_matching_fills
    original_submit = bot.submit_live_fak
    original_notice = bot.notify_trades

    async def fake_reconcile(job):
        return {'shares': 10.0, 'gross': 9.70, 'matches': 1}

    async def should_not_submit(*args, **kwargs):
        submit_calls.append((args, kwargs))
        raise AssertionError('blind duplicate POST after AMBIGUOUS')

    bot.own_recent_matching_fills = fake_reconcile
    bot.submit_live_fak = should_not_submit
    bot.notify_trades = lambda: False
    await bot.process_recovery_job(j1)
    j1b = get_job(A1)
    assert j1b['state'] == 'DONE', j1b
    assert abs(float(j1b['accounted_shares']) - 10.0) < 1e-9
    assert not submit_calls
    pos = bot.position_get(W, A1, 'LIVE')
    assert pos and abs(pos['shares'] - 10.0) < 1e-9

    # 2) Deterministic NO_MATCH: recovery retries the SAME limit and fills later.
    p2 = payload(A2, '0xnomatch')
    ok = bot.recovery_schedule_db(
        key='evt-nm', wallet=W, info={'label':'phantom.1'}, payload=p2, source='rest:activity',
        requested=8.0, accounted=0.0, accounted_gross=0.0, limit_price=0.98,
        mode='LIVE', initial_status='REJECTED_NO_MATCH', initial_error='FAK NO_MATCH', detected_ms=bot.now_ms(),
    )
    assert ok
    j2 = get_job(A2)
    assert j2 and j2['state'] == 'RETRY'
    seen = {}

    async def fake_fill(asset, side, shares, limit_price, detected_ms, detected_perf_ns=None):
        seen.update(asset=asset, side=side, shares=shares, limit=float(limit_price))
        return {
            'ok': True, 'status': 'MATCHED', 'filled': shares, 'avg': 0.97,
            'gross': shares * 0.97, 'fee': 0.0, 'order_id': 'recovery-order',
            'response_json': '{}', 'error': '', 'build_sign_ms': 1, 'build_sign_us': 1000,
            'detect_to_submit_ms': 1, 'detect_to_submit_us': 1000, 'api_ms': 2,
        }

    bot.submit_live_fak = fake_fill
    # Generic/non-warmed asset path is immediately eligible; FAK itself enforces cap.
    await bot.process_recovery_job(j2)
    j2b = get_job(A2)
    assert j2b['state'] == 'DONE', j2b
    assert seen['asset'] == A2 and seen['side'] == 'BUY'
    assert abs(seen['limit'] - 0.98) < 1e-12
    assert abs(seen['shares'] - 8.0) < 1e-9
    pos2 = bot.position_get(W, A2, 'LIVE')
    assert pos2 and abs(pos2['shares'] - 8.0) < 1e-9

    # 3) Upgrade backfill: a recent v2.2-style AMBIGUOUS audit row becomes one
    # durable recovery job, but a second scan must not reset/duplicate it.
    p3 = payload('asset-backfill', '0xbackfill')
    detected3 = bot.now_ms()
    row3 = bot.base_order_row('evt-backfill', W, {'label':'phantom.1'}, p3, 'rest:activity', detected3, 'LIVE', 120.0, 0.05)
    row3.update({
        'requested_shares': 12.0, 'limit_price': 0.99, 'status': 'AMBIGUOUS',
        'filled_shares': 0.0, 'avg_price': None, 'gross_amount': 0.0, 'fee_estimate': 0.0,
        'order_id': '', 'response_json': '{}', 'build_sign_ms': None, 'detect_to_submit_ms': None,
        'api_ms': None, 'total_reaction_ms': 0, 'error': 'timeout after POST', 'created_ms': detected3,
    })
    bot.record_order(row3)
    assert bot.backfill_recent_recovery_jobs() == 1
    jb = get_job('asset-backfill')
    assert jb and jb['state'] == 'RECONCILE'
    assert bot.backfill_recent_recovery_jobs() == 0

    bot.own_recent_matching_fills = original_reconcile
    bot.submit_live_fak = original_submit
    bot.notify_trades = original_notice


asyncio.run(main())
print('PASS: v2.3 durable delivery recovery regression tests')
