MULTI7 PRE-JUMP LAB v1.3 — 4-WAY FORWARD TEST

Active PAPER strategies per token:
1) PRE_JUMP       : score >= 0.40, elapsed 1..160 sec
2) PRE_JUMP42     : score >= 0.42, elapsed 1..160 sec
3) PRE_JUMP10     : score >= 0.40, elapsed 10..120 sec
4) PRE_JUMP42_10  : score >= 0.42, elapsed 10..120 sec

Why keep 1..160 for the first two?
- PRE_JUMP and PRE_JUMP42 remain unchanged controls.
- Their SQLite history stays comparable across redeploys.
- The new 10..120 branches test the timing hypothesis independently.

All four use:
- PM ask 0.52..0.66
- >= 2 same-side venues
- PM 1s momentum -0.01..+0.05
- 5 shares
- +$0.60 NET take-profit
- no stop loss

Disabled from active strategy execution:
BASE, EXT_CONFIRM, EXT_VETO, PRE_JUMP35.
The base strategy task is not scheduled.

External Binance/Bybit/Coinbase collection, source health, jump-event labels,
3/5/10/20s trial labels, and hourly ZIP reporting remain enabled.

New environment variables:
PREJUMP_TEST_MIN_ELAPSED=10
PREJUMP_TEST_MAX_ELAPSED=120
