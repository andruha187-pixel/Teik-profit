# MULTI7 SAFE67 A/B/C/E — PAPER/LIVE + NET TP 0.60

Trading build of the uploaded MULTI7 A/B/C/E PAPER bot.

Version:

```text
19.0-multi7-abce-paper-live-tp60
```

## Tokens and strategies

Default tokens:

```text
BTC
XRP
BNB
SOL
ETH
DOGE
HYPE
```

Four strategies per token = **28 independent strategy accounts**:

```text
A
B
C
E
```

Each has its own mode:

```text
PAPER
LIVE
OFF
```

Fresh database defaults every strategy to `PAPER`.
Global trading starts `OFF`; use `START`.

## Strategy logic preserved

### A — SAFE67 BASE

```text
FIRST V2 eligible:
price 0.55..0.75
momentum 0.03..0.30

ENTRY:
price 0.67..0.75
momentum 0.05..0.10
default 5 shares

No DCA
No stop-loss
```

### B — SAFE67 old reversal DCA

```text
ENTRY:
price 0.67..0.75
momentum 0.05..0.10
default 5 shares

DCA arm:
held-side ask <= 0.50
elapsed <= 120 sec
NO BUY on arm tick

Later:
momentum >= +0.05
ask <= 0.60
default +5 shares
one DCA
```

B intentionally keeps no `0.30` floor and no `+0.15` rebound cap.

### C — tighter entry + safer reversal DCA

```text
ENTRY:
price 0.67..0.70
momentum 0.05..0.10
default 5 shares

DCA arm:
ask <= 0.50
elapsed <= 120 sec
NO BUY on arm tick

Later:
ask 0.30..0.60
momentum +0.05..+0.15
default +5 shares
one DCA
```

### E — cross-token consensus

```text
target entry:
price 0.67..0.75
momentum 0.05..0.10

confirmation:
>= 2 DISTINCT OTHER tokens
with A/BASE SAFE67 PASS
same direction
previous 10 sec

default ENTRY 5 shares
No DCA
```

The target token does not count itself. One other token counts once.

A signals are still evaluated as consensus sources even when A's trading mode is
`OFF`; `OFF` blocks order execution, not signal/gate recording.

No strategy switches sides and there is no stop-loss.

## Default NET take-profit

Default:

```text
TAKE_PROFIT_USDC=0.60
```

This is **+$0.60 NET for the whole remaining position**, not per share.

The bot's threshold calculation includes:

```text
entry gross cost
+ entry commission
- prior exit net
- projected current sell net
including projected exit commission
```

Change it in hosting Environment Variables:

```text
TAKE_PROFIT_USDC=0.30
TAKE_PROFIT_USDC=0.60
TAKE_PROFIT_USDC=1.00
```

Disable:

```text
TAKE_PROFIT_USDC=OFF
```

or:

```text
TAKE_PROFIT_USDC=0
```

After changing an environment variable, redeploy/restart the service.

For B/C after a DCA, `0.60` is still the target for the **whole remaining
position**, including all buys and fees.

### PAPER TP

PAPER requires enough visible bid depth to sell the entire remaining position.
It does not record a partial PAPER take-profit merely to hit the threshold.

### LIVE TP

LIVE checks the same bot-tracked NET target, freshness-checks the book and uses
the protected real-order path:

```text
signed LIMIT -> FAK SELL
```

A genuine partial LIVE TP fill is recorded. Once a real TP has partially filled,
TP becomes latched and the bot continues trying to flatten the bot-tracked
remainder on later cycles.

An ambiguous submission remains fail-closed; it is not blindly duplicated.

`STOP` blocks new ENTRY/DCA actions, but TP monitoring continues for already
open bot-tracked PAPER/LIVE positions.

## LIVE safety

Master gate:

```text
LIVE_MASTER_ENABLE=0
```

First deploy with `0`. In Telegram use:

```text
WALLET
```

Verify:

```text
SDK: READY
Wallet: expected address
Collateral: expected balance
LIVE master: OFF
```

Then set:

```text
LIVE_MASTER_ENABLE=1
```

and redeploy.

Every individual strategy still needs a second 60-second Telegram confirmation:

```text
MODE BTC B LIVE
CONFIRM LIVE BTC B
```

Examples:

```text
MODE ETH C LIVE
CONFIRM LIVE ETH C

MODE SOL E LIVE
CONFIRM LIVE SOL E
```

Switch back:

```text
MODE BTC B PAPER
MODE BTC B OFF
```

Mode crossing PAPER <-> LIVE is blocked while that strategy holds an open
position in the other execution mode.

## Multiple LIVE strategies on one token

Default:

```text
ALLOW_MULTI_LIVE_PER_TOKEN=0
```

This prevents, for example, BTC A and BTC B from both being LIVE at the same
time. The strategies can share a signal and would otherwise send independent
real orders.

If you deliberately want several A/B/C/E strategies LIVE on the same token:

```text
ALLOW_MULTI_LIVE_PER_TOKEN=1
```

then redeploy.

Different tokens can be LIVE at the same time.

## Sizes

Whole token:

```text
SIZE BTC 5 5
```

sets:

```text
A ENTRY = 5
E ENTRY = 5
B ENTRY = 5, DCA = 5
C ENTRY = 5, DCA = 5
```

Per strategy:

```text
SIZE BTC A 5
SIZE BTC B 5 5
SIZE BTC C 5 5
SIZE BTC E 5
```

Use the other token names in the same way.

Sizes cannot be changed while that strategy has an open bot-tracked position.

## Telegram controls

```text
START
STOP
MODES
SIZES
BALANCE
POSITIONS
STATISTICS
TRADES
WALLET
EMERGENCY STOP
```

## LIVE execution

The real-order wrapper is the same protected pattern used in the earlier
PAPER/LIVE bot:

```text
fresh book check
signed LIMIT order
converted to FAK
actual accepted fill amount persisted
```

If the response after submission is ambiguous, that market/action is marked
fail-closed and the bot does not automatically submit a possible duplicate.

LIVE settlement PnL is bot-tracked from accepted fill amounts. Winning LIVE
shares that remain to market settlement are **not auto-redeemed** by this bot.

## Hosting variables

Minimum first-deploy block:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

PORT=8080
DATA_DIR=/var/data

POLYMARKET_PRIVATE_KEY=
POLYMARKET_WALLET_ADDRESS=

LIVE_MASTER_ENABLE=0
ALLOW_MULTI_LIVE_PER_TOKEN=0

TAKE_PROFIT_USDC=0.60

PAPER_START_BALANCE=500
ENTRY_ORDER_SIZE=5
DCA_ORDER_SIZE=5
```

Never put the real `POLYMARKET_PRIVATE_KEY` in GitHub. Keep it only in the
hosting Environment/Secret store.

Optional:

```text
POLYMARKET_RELAYER_API_KEY=
POLYMARKET_RELAYER_API_KEY_ADDRESS=
```

Leave them blank unless your wallet setup specifically uses them.

The complete template is in `.env.example`.

## Coolify

A `Dockerfile` is included.

Expose:

```text
8080
```

Persistent storage mount:

```text
/var/data
```

Health endpoint:

```text
/health
```

Database:

```text
/var/data/safe67_multi7_abce_paper_live_tp60.db
```

## Render

Build:

```text
pip install -r requirements.txt
```

Start:

```text
python main.py
```

Persistent disk should also be mounted at:

```text
/var/data
```

## Reports

The hourly ZIP reporter is deliberately disabled in this LIVE trading build.
Persistent SQLite still stores PAPER trades, LIVE orders, exits, signals,
consensus decisions, trajectories and results.

## Regression

Run:

```text
python test_multi7_abce_live_tp60.py
```

Expected:

```text
MULTI7 A/B/C/E PAPER/LIVE + NET TP60 regression: OK
```

The regression verifies:

- 7 tokens × A/B/C/E = 28 strategies;
- uploaded A/B/C/E entry and DCA settings;
- B can still take the old deep rebound DCA;
- C rejects DCA below 0.30 and rebound momentum above +0.15;
- E still requires two other-token A/BASE confirmations;
- `TAKE_PROFIT_USDC` defaults to 0.60 and can be changed/disabled via ENV;
- PAPER TP does not close below +$0.60 NET and closes above it;
- fake-SDK LIVE ENTRY uses the FAK wrapper;
- fake-SDK LIVE TP sends a SELL FAK and fully closes;
- multiple LIVE strategies on the same token are blocked by default.
