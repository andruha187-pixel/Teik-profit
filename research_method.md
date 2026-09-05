# PRE-JUMP LAB — research method and data dictionary

## Research question

For each Polymarket 5-minute outcome, determine whether external market microstructure from large crypto exchanges contains useful information **before** the Polymarket outcome price makes a meaningful upward jump.

Two different questions are measured separately:

1. **Final direction quality:** did the purchased outcome eventually win the market?
2. **Short-horizon exit quality:** did the position become fully sellable at the configured NET take-profit within 3/5/10/20 seconds?

The second question is especially important for a small-TP strategy.

## External feature semantics

Positive values are bullish for the underlying token; negative values are bearish.

- `ret_bps_1s/3s/10s`: external trade-price return over the stated horizon.
- `flow_1s/3s/10s`: signed taker notional divided by total taker notional. BUY taker flow is positive, SELL is negative.
- `flow_accel`: `flow_1s - flow_10s`.
- `obi`: top-book notional imbalance `(bidDepth-askDepth)/(bidDepth+askDepth)`.
- `micro_bps`: microprice displacement relative to midpoint.
- `spread_bps`: best-ask/best-bid spread.
- `bid_depth`, `ask_depth`: notional depth from the sampled top levels.
- `bid_depth_change_1s`, `ask_depth_change_1s`: depth ratio change versus ~1 second earlier.
- `liquidity_pressure`: positive when bid depth builds and/or ask depth disappears.
- `liq_signed_3s`, `liq_abs_3s`, `liq_flow_3s`: signed and absolute liquidation notional over ~3 seconds.
- `score`: venue composite in `[-1,+1]`.

## Cross-venue fields

- `ext_score`: weighted average of fresh venue scores.
- `up_votes`: fresh venues with score >= `EXT_VOTE_THRESHOLD`.
- `down_votes`: fresh venues with score <= `-EXT_VOTE_THRESHOLD`.
- `fresh_venues`: count of fresh sources.
- `fresh_names`: names of fresh sources.
- `median_external_price`: median fresh exchange price.
- `external_open_proxy`: first usable median external price around the market start; NOT Chainlink.
- `external_gap_bps`: current median external price versus that proxy.

## Jump labels

Default jump:

```text
Polymarket outcome ASK increase >= 0.08 within 5 seconds
```

`jump_events.csv` contains feature context from 1/3/5/10/20 seconds before the detected jump. A cooldown prevents the same continuous move from producing an excessive number of duplicate jump labels.

## Strategy labels

### BASE
Current SAFE67 benchmark without DCA.

### EXT_CONFIRM
Tests whether requiring external alignment raises quality.

### EXT_VETO
Tests whether external information works better as a rejection filter than as a positive trigger.

### PRE_JUMP
Tests whether a strong cross-exchange microstructure event can justify entering while the Polymarket contract is still below the normal SAFE67 zone.

## Why all strategies are ENTRY-only

DCA is intentionally removed from this experiment. The objective is to isolate the predictive value of the signal. Adding DCA would mix entry quality with position-management quality.

## Later offline search

After sufficient data, do not optimize only on total PnL. Compare at least:

- number of trades;
- settlement win rate;
- mean/median NET PnL;
- worst loss;
- TP hit rate 3/5/10/20 sec;
- maximum executable NET PnL distribution;
- score and venue-vote distributions;
- performance by token;
- performance by time of day;
- performance when Binance and Bybit agree/disagree;
- performance with and without Coinbase confirmation;
- PRE_JUMP price bands such as 0.52–0.56, 0.57–0.61, 0.62–0.66;
- external-score thresholds such as 0.40/0.50/0.60/0.70;
- one/two/three venue votes;
- 5/10/20-second pre-jump context.

Use an earlier period to select candidate thresholds and a later untouched period to validate them. A threshold that looks exceptional on the same data used to choose it is not enough evidence of a durable edge.

## Source caveats

- Exchange feeds can disconnect or become stale. Strategies that depend on external confirmation should not be judged without checking `source_health.csv`.
- Coinbase does not cover BNB/HYPE in the default mapping, so their composite is normally Binance + Bybit only.
- Cross-exchange clocks and network latency are not perfectly synchronized. Event timestamps are preserved where available, but this lab is not a colocated HFT system.
- Direct Chainlink Data Streams is not included in v1; `external_open_proxy` must never be interpreted as the actual Polymarket settlement source.
- 100% win rate is not an assumption or target guarantee. The lab is designed to measure whether a statistically useful improvement exists.
