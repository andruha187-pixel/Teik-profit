import os
import time
import asyncio
import tempfile
import importlib.util

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='copybot-feed-test-')
os.environ['REST_MAX_COPY_AGE_SEC'] = '30'
os.environ['REST_AUDIT_LOOKBACK_SEC'] = '300'

spec = importlib.util.spec_from_file_location('copybot_feed', os.path.join(os.path.dirname(__file__), 'main.py'))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)

W = '0x' + 'a' * 40
P = '0x' + 'b' * 40


def trade(age):
    return {
        'proxyWallet': W,
        'transactionHash': '0x' + ('1' if age < 30 else '2') * 64,
        'asset': '123',
        'conditionId': '0x' + '3' * 64,
        'side': 'BUY', 'price': 0.54, 'size': 10,
        'timestamp': int(time.time() - age),
        'title': 'BTC Up or Down', 'outcome': 'Up',
    }


async def test_case():
    # Mixed-case addresses normalize identically.
    assert bot.normalize_address('0x' + 'A' * 40) == W

    async def profile_get(url, params=None, timeout=8):
        return {'proxyWallet': P, 'name': 'phantom.1'}
    bot.get_json = profile_get
    proxy, name = await bot.resolve_proxy_wallet(W)
    assert proxy == P and name == 'phantom.1'

    calls = []
    async def fake_ingest(payload, source='rtds', force_skip_reason=None):
        calls.append((payload, source, force_skip_reason))

    # A Data API record that appears 10s after exchange timestamp must still be
    # discovered/copy-eligible. v2.0 wrongly discarded anything older than 4s.
    async def get_10s(url, params=None, timeout=8):
        return [trade(10)]
    bot.get_json = get_10s
    bot.ingest_trade = fake_ingest
    bot.seen_hot.clear()
    await bot._poll_rest_wallet(W, use_activity=False)
    assert len(calls) == 1
    assert calls[0][1] == 'rest:trades'
    assert calls[0][2] is None

    # A very late new record must still be surfaced/audited, but not copied.
    calls.clear()
    async def get_60s(url, params=None, timeout=8):
        return [trade(60)]
    bot.get_json = get_60s
    bot.seen_hot.clear()
    await bot._poll_rest_wallet(W, use_activity=False)
    assert len(calls) == 1
    assert calls[0][2].startswith('REST_LATE:')


asyncio.run(test_case())
print('PASS: v2.1 feed detection regression tests')
