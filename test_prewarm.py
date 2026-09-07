import os, shutil, asyncio, importlib.util

TEST_DIR='/tmp/prejump_prewarm'
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)
os.environ['DATA_DIR']=TEST_DIR
os.environ['LIVE_MASTER_ENABLE']='1'
os.environ['LIVE_PREWARM_ENABLE']='1'
os.environ['LIVE_PREWARM_INTERVAL_SEC']='15'

spec=importlib.util.spec_from_file_location('bot', os.path.join(os.path.dirname(__file__),'main.py'))
bot=importlib.util.module_from_spec(spec); spec.loader.exec_module(bot); bot.init_db()
assert bot.VERSION.startswith('20.11-')

class Balance:
    balance='1000000'
class Client:
    def __init__(self): self.calls=0
    async def get_balance_allowance(self, **kw):
        self.calls += 1
        return Balance()
client=Client(); bot.live_client=client; bot.live_client_ready=True
bot.live_prewarm_last_ms=0
ok=asyncio.run(bot.prewarm_live_transport('test'))
assert ok and client.calls==1
# Cooldown prevents an immediate second request.
ok2=asyncio.run(bot.prewarm_live_transport('test2'))
assert not ok2 and client.calls==1
print('PRE-JUMP v20.11 read-only SDK transport prewarm: OK')
