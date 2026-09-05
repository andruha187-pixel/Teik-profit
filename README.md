# MULTI7 PRE-JUMP LAB

Version: `1.0-multi7-prejump-lab`

A separate **PAPER-only research bot** for studying whether external crypto-exchange microstructure can improve the current Polymarket SAFE67 entry, veto bad entries, or detect a move before Polymarket reaches the usual 0.67 entry zone.

This project does **not** send real Polymarket orders and does not replace the current LIVE bot.

## Markets

Default symbols:

```text
BTC,XRP,BNB,SOL,ETH,DOGE,HYPE
```

There are four independent PAPER strategies per token, therefore 28 PAPER accounts. Each starts at `$500` by default.

## Strategies

### BASE

Benchmark reproducing the current SAFE67-style entry, ENTRY only:

```text
first V2 eligible:
  ask       0.55..0.75
  momentum +0.03..+0.30

after first V2 signal, SAFE entry:
  ask       0.67..0.75
  momentum +0.05..+0.10
  size      5 shares
```

The BASE signal is sampled on the normal approximately 3-second decision loop. No DCA, no stop, no switch.

### EXT_CONFIRM

Same SAFE67 entry as BASE, but the external microstructure must confirm the same direction at the moment of the first SAFE67 gate:

```text
directional external score >= +0.30
same-direction venue votes >= 2
```

No DCA, no stop, no switch.

### EXT_VETO

Same SAFE67 signal as BASE, except an entry is blocked when external order flow strongly contradicts the Polymarket direction:

```text
directional external score <= -0.20
opposing venue votes >= 2
```

This strategy tests whether external data is more useful as a filter than as a trigger.

### PRE_JUMP

Experimental earlier entry intended to test the idea of entering before Polymarket reaches 0.67:

```text
Polymarket ask           0.52..0.66
absolute external score >= 0.55
same-direction votes    >= 2
elapsed                  5..160 sec
Polymarket 1s momentum  -0.01..+0.05
```

By default PRE_JUMP additionally requires **both Binance and Bybit** to vote in the same direction:

```text
PREJUMP_REQUIRE_BINANCE_BYBIT=1
```

This intentionally tries to avoid buying after the Polymarket move has already happened.

## External data sources

The lab uses public WebSocket market data; no Binance/Bybit/Coinbase trading API keys are required.

### Binance USD-M Futures

Default high-frequency endpoint:

```text
wss://fstream.binance.com/public/stream
```

Per token the bot consumes:

```text
{symbol}@aggTrade
{symbol}@depth20@100ms
```

It derives taker BUY/SELL notional flow and a top-20 order-book snapshot.

### Bybit USDT Linear

Endpoint:

```text
wss://stream.bybit.com/v5/public/linear
```

Per token:

```text
orderbook.50.SYMBOL
publicTrade.SYMBOL
allLiquidation.SYMBOL
```

The bot uses order-book changes, taker-side trades and liquidation direction.

### Coinbase Spot

Endpoint:

```text
wss://advanced-trade-ws.coinbase.com
```

Default mapped products:

```text
BTC -> BTC-USD
ETH -> ETH-USD
SOL -> SOL-USD
XRP -> XRP-USD
DOGE -> DOGE-USD
```

Coinbase BNB/HYPE are left unmapped by default. The bot consumes `level2`, `market_trades` and `heartbeats`.

## What the external score contains

For each available venue the lab calculates:

```text
price return:        1s / 3s / 10s
taker flow:          1s / 3s / 10s
flow acceleration:   flow_1s - flow_10s
order-book imbalance
microprice offset
spread
bid/ask depth
1s bid-depth change
1s ask-depth change
liquidity pressure
3s liquidation flow
```

Current experimental venue score:

```text
0.28 * taker_flow_1s
0.17 * taker_flow_3s
0.16 * tanh(return_1s / 6 bps)
0.10 * tanh(return_3s / 15 bps)
0.10 * order_book_imbalance
0.07 * tanh(microprice_bps / 1.5)
0.08 * liquidity_pressure
0.04 * liquidation_flow_3s
```

The result is clamped to `[-1,+1]`.

Cross-exchange weighting:

```text
Binance  40%
Bybit    40%
Coinbase 20%
```

Only fresh sources are included and weights are renormalized when a venue is unavailable.

A venue vote requires:

```text
abs(venue_score) >= 0.25
```

These thresholds are deliberately hypotheses for PAPER collection, not claimed optimums.

## Chainlink note

This first lab version does **not** claim to read the actual Chainlink Data Streams settlement reference. Direct Data Streams access requires credentials/authentication.

Instead the bot stores:

```text
external_open_proxy
external_gap_bps
```

`external_open_proxy` is the median of fresh exchange prices around the beginning of the 5-minute market. It is explicitly only an exchange-price proxy, **not Chainlink**.

A direct Chainlink adapter can be added later if Data Streams credentials are available.

## The important research dataset: what happened BEFORE a jump

A Polymarket jump is currently defined as:

```text
outcome ASK rises >= 0.08 within 5 seconds
```

For every detected jump, `jump_events.csv` stores external-feature snapshots from approximately:

```text
1 second before
3 seconds before
5 seconds before
10 seconds before
20 seconds before
```

That allows later analysis of which combinations of flow, order-book changes, cross-exchange votes and liquidations repeatedly occur before Polymarket moves.

## Trading objective labels

The research target is not only whether the 5-minute market ultimately resolves correctly. For every PAPER entry the bot also asks:

> Would this entry have offered a fully executable `+$0.60 NET` exit within the next 3, 5, 10 or 20 seconds?

`trials_3_5_10_20s.csv` records:

```text
max_net_3s
max_net_5s
max_net_10s
max_net_20s
hit_tp_3s
hit_tp_5s
hit_tp_10s
hit_tp_20s
```

This is aligned with the current bot's small take-profit trading style.

## PAPER take-profit

Default:

```text
TAKE_PROFIT_USDC=0.60
```

It is the NET profit target for the whole position after entry fees and projected exit fees. The PAPER exit requires enough visible Polymarket bid liquidity to liquidate the full remaining position.

## Sampling rates

External feature / PRE_JUMP loop:

```text
FAST_INTERVAL=0.25
```

Feature persistence:

```text
FEATURE_PERSIST_INTERVAL=0.50
```

BASE / EXT_CONFIRM / EXT_VETO decision sampling:

```text
BASE_DECISION_INTERVAL=3.0
```

The external collectors themselves process incoming WebSocket messages as they arrive; the 250 ms interval is the feature/decision calculation cadence.

## Hourly ZIP report

The bot sends one ZIP per completed hour containing:

```text
report.txt
strategy_summary.csv
external_features.csv
jump_events.csv
signal_events.csv
paper_trades.csv
paper_exits.csv
trials_3_5_10_20s.csv
market_results.csv
source_health.csv
```

These files are intended to be sent back to ChatGPT for offline comparison and threshold search.

After a successfully sent report, old high-frequency feature/source-health rows are pruned with overlap to control disk usage. Jump events, trials, signals, trades and results remain available.

## Telegram

Buttons:

```text
START
STOP
STATISTICS
SOURCES
POSITIONS
TRADES
LAB INFO
```

External data collection always runs. `STOP` only blocks new PAPER entries; it does not stop data collection or TP monitoring for already-open PAPER positions.

For this research bot, PAPER entry collection defaults to ON after a fresh database so a forgotten START does not waste hours of data.

Use `SOURCES` to verify Binance, Bybit and Coinbase freshness before trusting PRE_JUMP results.

## Coolify / Render

This is PAPER-only, so **do not add a Polymarket private key**.

Minimum environment variables:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
PORT=8080
DATA_DIR=/var/data
SYMBOLS=BTC,XRP,BNB,SOL,ETH,DOGE,HYPE

ENABLE_BINANCE=1
ENABLE_BYBIT=1
ENABLE_COINBASE=1
TAKE_PROFIT_USDC=0.60
```

Use persistent storage:

```text
/var/data
```

Health endpoint:

```text
/health
```

A Dockerfile is included. On Coolify use Dockerfile build and expose port `8080`.

## Recommended first run

Do not tune the thresholds after a handful of trades. Let the lab collect at least a full day, preferably 1–3 days, then compare:

```text
BASE vs EXT_CONFIRM vs EXT_VETO vs PRE_JUMP
TP-hit rate at 3/5/10/20 sec
win rate at settlement
PnL after fees
score distribution before winning vs losing jumps
1 vs 2 vs 3 confirming venues
PRE_JUMP entry price bands
```

The goal of this version is to **collect enough synchronized raw evidence to discover whether a repeatable pre-jump edge exists**, not to assume one in advance.

## Tests

```text
python test_prejump_lab.py
python test_source_parsers.py
```

Expected:

```text
MULTI7 PRE-JUMP LAB regression: OK
External source message parser regression: OK
```

The parser regression uses official-shaped WebSocket fixtures. Live external connectivity cannot be guaranteed by an offline test environment; after deployment use Telegram `SOURCES` to verify real source freshness.
