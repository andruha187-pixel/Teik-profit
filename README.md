# BTC / XRP / ETH — B + C PAPER/LIVE bot for Coolify

Version:

```text
18.0-btc-xrp-eth-bc-paper-live-tp
```

This trading build keeps only six strategies:

```text
BTC B
BTC C
XRP B
XRP C
ETH B
ETH C
```

Each strategy has its own independent mode:

```text
PAPER
LIVE
OFF
```

Default mode after a fresh database is `PAPER`. Global trading starts `OFF`
until `START` is pressed.

There are no hourly ZIP reports in this trading build. All bot-tracked PAPER
and LIVE actions remain in persistent SQLite.

## B strategy

B keeps the previous wide SAFE67 entry and old reversal DCA control:

```text
FIRST V2 eligible:
price    0.55..0.75
momentum 0.03..0.30
lookback 2 decision ticks

ENTRY:
price    0.67..0.75
momentum 0.05..0.10
default  5 shares

DCA ARM:
held-side ask <= 0.50
elapsed <= 120 sec
NO BUY on the arm tick

LATER DCA:
momentum >= +0.05
ask <= 0.60
default +5 shares
one DCA only
```

B intentionally has no new `0.30` DCA floor and no `+0.15` rebound cap.

No stop-loss. No side switching.

## C strategy

C keeps the tighter entry and safer DCA:

```text
FIRST V2 eligible:
price    0.55..0.75
momentum 0.03..0.30

ENTRY:
price    0.67..0.70
momentum 0.05..0.10
default  5 shares

DCA ARM:
held-side ask <= 0.50
elapsed <= 120 sec
NO BUY on the arm tick

LATER DCA:
ask      0.30..0.60
momentum +0.05..+0.15
default  +5 shares
one DCA only
```

No stop-loss. No side switching.

## Configurable NET take-profit

The same `.env` parameter is used for all six strategies:

```text
TAKE_PROFIT_USDC=0.30
```

`0.30` means the bot tries to close the **whole remaining position** once the
bot-tracked executable result reaches at least **+$0.30 NET** after:

```text
entry cost
+ entry fee
+ estimated exit fee
```

Examples:

```text
TAKE_PROFIT_USDC=0.30
TAKE_PROFIT_USDC=0.50
TAKE_PROFIT_USDC=1.00
TAKE_PROFIT_USDC=OFF
```

`OFF`, `NONE`, `DISABLED` or `0` disables TP.

For a position that already has a DCA, the target is still `$0.30` for the
**entire remaining position**, not `$0.30` per share.

PAPER TP walks visible bids and requires enough visible depth for the entire
remaining position before it closes.

LIVE TP first checks the same bot-tracked NET threshold, refreshes the bid book,
then uses the same protected real-order path as the trading bot: signed limit
order converted to `FAK`.

If a real TP receives a genuine partial fill, TP becomes latched and the bot
continues trying to flatten the bot-tracked remainder on later cycles.
An ambiguous submission remains fail-closed and is not blindly retried.

`STOP` blocks new ENTRY/DCA actions, but TP monitoring continues for already
open bot-tracked positions.

## PAPER / LIVE safety

LIVE is guarded at several levels.

First, Coolify must explicitly contain:

```text
LIVE_MASTER_ENABLE=1
```

Second, each individual strategy must be switched to LIVE in Telegram and
confirmed within 60 seconds.

Example:

```text
MODE BTC B LIVE
CONFIRM LIVE BTC B
```

A real order uses:

```text
signed LIMIT order
-> FAK
-> actual accepted fill amount is persisted
```

Before execution the order book is refreshed when required.

If the exchange/network result after submission is ambiguous, the market/action
is marked fail-closed so the bot does not automatically submit a possible
duplicate real order.

## B and C simultaneously LIVE

By default:

```text
ALLOW_DOUBLE_LIVE=0
```

This is intentional.

If `BTC B` is LIVE, the bot blocks `BTC C` from becoming LIVE at the same time,
and vice versa. The same rule applies to XRP and ETH.

Why: B and C may generate the same entry on the same 5-minute market. If both
are LIVE they are two independent strategies and can submit two independent
real orders.

If you deliberately want that behavior:

```text
ALLOW_DOUBLE_LIVE=1
```

and redeploy.

PAPER strategies are not affected by this rule. For example, you can run:

```text
BTC B = LIVE
BTC C = PAPER
```

## Telegram commands

Modes:

```text
MODES

MODE BTC B PAPER
MODE BTC B OFF
MODE BTC B LIVE
CONFIRM LIVE BTC B

MODE BTC C PAPER
MODE BTC C LIVE
CONFIRM LIVE BTC C
```

Use `XRP` or `ETH` in exactly the same way.

Sizes for both B/C on one token:

```text
SIZE BTC 5 5
```

This sets:

```text
BTC B ENTRY = 5
BTC B DCA   = 5
BTC C ENTRY = 5
BTC C DCA   = 5
```

Per-strategy sizing:

```text
SIZE BTC B 5 5
SIZE BTC C 5 5
SIZE XRP B 5 5
SIZE XRP C 5 5
SIZE ETH B 5 5
SIZE ETH C 5 5
```

A strategy cannot be resized while it has an open bot-tracked position.

Other commands/buttons:

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

## Coolify deployment

The repository already contains a `Dockerfile`.

Use GitHub as the source and let Coolify build the Dockerfile.

Application port:

```text
8080
```

Health endpoint:

```text
/health
```

Create persistent storage and mount it at:

```text
/var/data
```

The database is:

```text
/var/data/btc_xrp_eth_bc_paper_live_tp.db
```

Without a persistent `/var/data` volume, a rebuild/redeploy may lose the
bot-tracked database state.

## Environment variables

The complete template is in `.env.example`.

For the first deploy, the important block is:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

PORT=8080
DATA_DIR=/var/data

POLYMARKET_PRIVATE_KEY=
POLYMARKET_WALLET_ADDRESS=

LIVE_MASTER_ENABLE=0
ALLOW_DOUBLE_LIVE=0

TAKE_PROFIT_USDC=0.30

PAPER_START_BALANCE=500
ENTRY_ORDER_SIZE=5
DCA_ORDER_SIZE=5
```

Never commit a real private key into GitHub or `.env.example`. Put it only in
Coolify Environment Variables / Secrets.

These are optional and should remain blank unless your Polymarket wallet setup
specifically uses them:

```text
POLYMARKET_RELAYER_API_KEY=
POLYMARKET_RELAYER_API_KEY_ADDRESS=
```

The full strategy defaults are also exposed in `.env.example`, but you do not
need to copy every one of them into Coolify unless you want to override the
defaults.

## Safe first LIVE launch

Deploy initially with:

```text
LIVE_MASTER_ENABLE=0
ALLOW_DOUBLE_LIVE=0
```

Then in Telegram press:

```text
WALLET
```

Verify:

```text
SDK: READY
Wallet: the expected wallet
Collateral: the expected balance
```

Only after that change Coolify to:

```text
LIVE_MASTER_ENABLE=1
```

and redeploy.

For the first real-money test, keep only one strategy LIVE, for example:

```text
MODE BTC B LIVE
CONFIRM LIVE BTC B
START
```

Keep the other five PAPER or OFF until you confirm the real entry/TP behavior.

## Dependencies

The trading build uses:

```text
aiohttp>=3.10,<4
websockets>=13,<16
python-dotenv>=1.0,<2
polymarket-client==0.7.0
```

## Verification

Run:

```text
python test_bc_paper_live_tp.py
```

Expected:

```text
BTC/XRP/ETH B/C PAPER/LIVE + NET TP regression: OK
PAPER example PnL: $+0.31186
LIVE example PnL estimate: $+0.31186
```

The regression explicitly checks:

- BTC B accepts the old deep `.25` DCA after a valid rebound;
- C rejects a target entry above `.70`;
- C rejects DCA below `.30`;
- C rejects rebound momentum above `+.15`;
- C accepts a valid `.35 / +.10` DCA;
- PAPER TP does not close below `+$0.30 NET` and does close above it;
- the protected LIVE FAK buy path works with a fake SDK client;
- LIVE TP sends a SELL FAK and flattens the bot-tracked position;
- a fully TP-closed strategy cannot buy that market again;
- same-token double-LIVE protection defaults to OFF.
