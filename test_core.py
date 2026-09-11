import os
import tempfile
import importlib.util
import asyncio
from dataclasses import dataclass

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='copybot-test-')

spec = importlib.util.spec_from_file_location('copybot', os.path.join(os.path.dirname(__file__), 'main.py'))
bot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bot)


def test_price_math():
    assert str(bot.build_limit_from_target(0.54, 0.05, 'BUY')) == '0.59'
    assert str(bot.build_limit_from_target(0.54, 0.05, 'SELL')) == '0.49'
    assert str(bot.build_limit_from_target(0.95, 0.10, 'BUY')) == '0.99'
    assert str(bot.build_limit_from_target(0.03, 0.05, 'SELL')) == '0.01'


def test_cross_feed_dedupe():
    p1 = {
        'proxyWallet': '0x' + '1' * 40,
        'transactionHash': '0xabc',
        'asset': '123', 'side': 'BUY', 'price': 0.54, 'size': 10, 'timestamp': 1,
    }
    p2 = dict(p1)
    p2['price'] = 0.55
    p2['size'] = 8
    assert bot.event_key(p1) == bot.event_key(p2)


def test_source_notional():
    assert abs(bot.source_trade_usdc({'price': 0.62, 'size': 40}) - 24.8) < 1e-9
    assert abs(bot.source_trade_usdc({'price': 0.62, 'size': 40, 'usdcSize': 24.77}) - 24.77) < 1e-9


def test_buy_sizing_modes():
    # Source: 40sh @ .62 = $24.80. Our BUY slippage cap in this example is .67.
    fixed = bot.buy_copy_sizing(0.62, 40, 0.67, 'FIXED', fixed_usdc=5, scale_pct=100, max_usdc=100)
    assert fixed[3] == 'FIXED'
    assert fixed[0] * 0.67 <= 5.0 + 1e-9
    assert fixed[0] * 0.67 > 4.99
    assert not fixed[4]

    same_usd = bot.buy_copy_sizing(0.62, 40, 0.67, 'SAME_USD', fixed_usdc=5, scale_pct=100, max_usdc=100)
    assert same_usd[3] == 'SAME_USD'
    assert abs(same_usd[2] - 24.8) < 1e-9
    assert same_usd[0] * 0.67 <= 24.8 + 1e-9
    assert same_usd[0] * 0.67 > 24.79
    assert not same_usd[4]

    same_shares = bot.buy_copy_sizing(0.62, 40, 0.67, 'SAME_SHARES', fixed_usdc=5, scale_pct=100, max_usdc=100)
    assert same_shares[3] == 'SAME_SHARES'
    assert same_shares[0] == 40.0
    assert abs(same_shares[1] - 26.8) < 1e-9
    assert not same_shares[4]

    scale = bot.buy_copy_sizing(0.62, 40, 0.67, 'SCALE', fixed_usdc=5, scale_pct=50, max_usdc=100)
    assert scale[3] == 'SCALE'
    assert scale[0] * 0.67 <= 12.4 + 1e-9
    assert scale[0] * 0.67 > 12.39
    assert not scale[4]


def test_max_copy_cap_all_modes():
    # SAME USD and SCALE are clipped by max USD.
    for mode, pct in [('SAME_USD', 100), ('SCALE', 200)]:
        r = bot.buy_copy_sizing(0.62, 400, 0.67, mode, fixed_usdc=500, scale_pct=pct, max_usdc=20)
        assert r[4]
        assert r[0] * 0.67 <= 20.0 + 1e-9
        assert r[0] * 0.67 > 19.99

    # SAME SHARES also obeys the hard USD cap.
    r = bot.buy_copy_sizing(0.62, 400, 0.67, 'SAME_SHARES', fixed_usdc=5, scale_pct=100, max_usdc=20)
    assert r[4]
    assert r[0] < 400
    assert r[0] * 0.67 <= 20.0 + 1e-9

    # FIXED cannot bypass MAX COPY either.
    r = bot.buy_copy_sizing(0.62, 40, 0.67, 'FIXED', fixed_usdc=50, scale_pct=100, max_usdc=20)
    assert r[4]
    assert r[0] * 0.67 <= 20.0 + 1e-9


def test_db_position_accounting_and_v11_schema():
    bot.init_db()
    bot.load_runtime_caches()
    with bot.db() as conn:
        cols = {r[1] for r in conn.execute('PRAGMA table_info(copy_orders)').fetchall()}
    assert {'size_mode', 'source_amount_usdc', 'scale_pct', 'max_copy_usdc', 'size_capped'} <= cols
    assert bot.copy_size_mode() in {'FIXED', 'SAME_USD', 'SAME_SHARES', 'SCALE'}
    assert bot.max_copy_usdc() > 0

    wallet = '0x' + '2' * 40
    bot.position_apply_buy(wallet, 'asset', 'PAPER', {'title':'T','outcome':'YES'}, 10, 0.5, 5.0)
    p = bot.position_get(wallet, 'asset', 'PAPER')
    assert abs(p['shares'] - 10) < 1e-9
    pnl = bot.position_apply_sell(wallet, 'asset', 'PAPER', 2, 1.2)
    assert abs(pnl - 0.2) < 1e-9
    p = bot.position_get(wallet, 'asset', 'PAPER')
    assert abs(p['shares'] - 8) < 1e-9
    assert abs(p['total_cost'] - 4.0) < 1e-9



def test_copy_trade_same_usd_integration():
    bot.init_db()
    bot.load_runtime_caches()
    bot.set_setting('mode', 'PAPER')
    bot.set_setting('size_mode', 'SAME_USD')
    bot.set_setting('max_copy_usdc', '100')
    bot.set_setting('slippage', '0.05')
    bot.set_setting('paper_cash', '500')
    wallet = '0x' + '3' * 40
    info = {'label': 'Source'}
    payload = {
        'proxyWallet': wallet, 'transactionHash': '0xint', 'asset': 'asset-int',
        'side': 'BUY', 'price': 0.62, 'size': 40, 'timestamp': 1,
        'title': 'Integration', 'outcome': 'YES',
    }
    original_paper_fak = bot.paper_fak
    original_notify = bot.notify_trades

    async def fake_paper_fak(asset, side, shares, limit_price):
        assert asset == 'asset-int'
        assert side == 'BUY'
        assert abs(float(limit_price) - 0.67) < 1e-9
        assert shares * 0.67 <= 24.8 + 1e-9
        return {'ok': True, 'status': 'PAPER_FILLED', 'filled': shares, 'avg': 0.66, 'gross': shares * 0.66, 'api_ms': 1}

    async def go():
        bot.paper_fak = fake_paper_fak
        bot.notify_trades = lambda: False
        try:
            await bot.copy_trade('event-int', wallet, info, payload, 'test', bot.now_ms())
        finally:
            bot.paper_fak = original_paper_fak
            bot.notify_trades = original_notify

    asyncio.run(go())
    with bot.db() as conn:
        r = conn.execute("SELECT * FROM copy_orders WHERE event_key='event-int' ORDER BY id DESC LIMIT 1").fetchone()
    assert r is not None
    assert r['size_mode'] == 'SAME_USD'
    assert abs(r['source_amount_usdc'] - 24.8) < 1e-9
    assert r['copy_amount_usdc'] <= 24.8 + 1e-9
    assert r['size_capped'] == 0
    assert r['filled_shares'] > 0

def test_fake_live_fak():
    @dataclass
    class Signed:
        order_type: str = 'GTC'
        post_only: bool = False

    class Resp:
        ok = True
        making_amount = 3.0
        taking_amount = 5.0
        status = 'matched'
        order_id = 'test'
        trade_ids = ('t1',)

    class FakeClient:
        async def create_limit_order(self, **kwargs):
            assert kwargs['price'] == '0.59'
            assert kwargs['size'] == '5.0'
            assert kwargs['side'] == 'BUY'
            return Signed()
        async def post_order(self, order):
            assert order.order_type == 'FAK'
            return Resp()

    async def go():
        bot.LIVE_MASTER_ENABLE = True
        bot.live_client_ready = True
        bot.live_client = FakeClient()
        bot.sdk_post_order_with_allowance_recovery = None
        result = await bot.submit_live_fak('asset', 'BUY', 5.0, 0.59, bot.now_ms())
        assert result['ok']
        assert abs(result['filled'] - 5.0) < 1e-9
        assert abs(result['avg'] - 0.6) < 1e-9

    asyncio.run(go())



def test_audit_messages_and_reasons():
    wallet = '0x' + '2' * 40
    info = {'label': 'Watcher'}
    payload = {
        'proxyWallet': wallet, 'transactionHash': '0xabcdef1234567890abcdef1234567890',
        'asset': 'asset-audit', 'side': 'BUY', 'price': 0.54, 'size': 10,
        'title': 'Audit market', 'outcome': 'YES',
    }
    bot.shadow_cache[(wallet, 'asset-audit')] = 0.0
    msg = bot.source_detect_message(wallet, info, payload, 'rtds:trades')
    assert 'НОВАЯ ПОЗИЦИЯ НАЙДЕНА' in msg
    assert 'rtds:trades' in msg
    msg2 = bot.source_detect_message(wallet, info, payload, 'rest:fallback')
    assert 'REST fallback' in msg2
    assert 'NO_MATCH' in bot.human_copy_reason('REJECTED_NO_MATCH', '')
    assert 'STOP' in bot.human_copy_reason('SKIPPED', 'BOT_STOPPED')


if __name__ == '__main__':
    test_price_math()
    test_cross_feed_dedupe()
    test_source_notional()
    test_buy_sizing_modes()
    test_max_copy_cap_all_modes()
    test_db_position_accounting_and_v11_schema()
    test_copy_trade_same_usd_integration()
    test_fake_live_fak()
    test_audit_messages_and_reasons()
    print('PASS: v1.2 core regression tests')
