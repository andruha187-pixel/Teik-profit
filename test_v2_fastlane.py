import os
import tempfile
import importlib.util
import asyncio
from dataclasses import dataclass

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='copybot-v2-test-')
os.environ['BTC15_FASTLANE_ENABLE'] = '1'

spec = importlib.util.spec_from_file_location('copybotv2', os.path.join(os.path.dirname(__file__), 'main.py'))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)


def sample_event(slot=1789005600):
    slug = f'btc-updown-15m-{slot}'
    return {
        'slug': slug,
        'title': 'Bitcoin Up or Down',
        'markets': [{
            'conditionId': '0xcond',
            'slug': slug,
            'question': 'Bitcoin Up or Down - 15m',
            'outcomes': '["Up","Down"]',
            'clobTokenIds': '["111","222"]',
        }],
    }


def test_parse_market():
    ev = sample_event()
    m = bot.btc15_parse_market(ev['markets'][0], ev)
    assert m['up_asset'] == '111'
    assert m['down_asset'] == '222'
    assert m['start_ts'] == 1789005600
    assert m['end_ts'] == 1789006500


def test_fastlane_book_snapshot():
    ev = sample_event()
    m = bot.btc15_parse_market(ev['markets'][0], ev)
    asyncio.run(bot.btc15_register_market(m))
    bot.btc15_apply_book('111', {
        'bids': [{'price':'0.53','size':'20'}],
        'asks': [{'price':'0.54','size':'15'}],
        'tick_size': '0.01',
    })
    p = {'asset':'111', 'slug':m['slug'], 'side':'BUY', 'price':0.53, 'size':5}
    snap = bot.btc15_fastlane_snapshot(p)
    assert snap['fast_lane'] is True
    assert snap['book_fresh'] is True
    assert abs(snap['book_price'] - 0.54) < 1e-9


def test_non_btc_not_fastlane():
    p = {'asset':'999', 'slug':'eth-updown-15m-1789005600', 'side':'BUY', 'price':0.5, 'size':5}
    assert bot.btc15_fastlane_snapshot(p)['fast_lane'] is False


def test_signer_prewarm_is_local_only():
    @dataclass
    class Signed:
        order_type: str = 'GTC'
        post_only: bool = False

    class FakeClient:
        def __init__(self):
            self.create_calls = 0
            self.post_calls = 0
        async def create_limit_order(self, **kwargs):
            self.create_calls += 1
            assert kwargs['token_id'] == '333'
            return Signed()
        async def post_order(self, order):
            self.post_calls += 1
            raise AssertionError('prewarm must never post')

    async def go():
        bot.live_client_ready = True
        bot.live_client = FakeClient()
        bot.btc15_signer_warmed.discard('333')
        ok = await bot.btc15_signer_prewarm({'slug':'btc-updown-15m-1'}, '333', 'Up')
        assert ok
        assert bot.live_client.create_calls == 1
        assert bot.live_client.post_calls == 0
        assert '333' in bot.btc15_signer_warmed

    asyncio.run(go())


def test_v2_schema():
    bot.init_db()
    with bot.db() as conn:
        cols = {r[1] for r in conn.execute('PRAGMA table_info(copy_orders)').fetchall()}
    required = {'build_sign_us','detect_to_submit_us','fast_lane','fast_book_age_ms','fast_book_price','fast_signer_warm'}
    assert required <= cols


if __name__ == '__main__':
    test_parse_market()
    test_fastlane_book_snapshot()
    test_non_btc_not_fastlane()
    test_signer_prewarm_is_local_only()
    test_v2_schema()
    print('PASS: v2 BTC15 fast-lane regression tests')
