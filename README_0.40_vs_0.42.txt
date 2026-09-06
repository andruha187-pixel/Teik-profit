MULTI7 PRE-JUMP LAB v1.2 — 0.40 vs 0.42

Active PAPER strategies only:
- PRE_JUMP   : directional score >= 0.40
- PRE_JUMP42 : directional score >= 0.42

Both use:
- PM ask 0.52..0.66
- >= 2 same-side venues
- elapsed 1..160 sec
- PM 1s momentum -0.01..+0.05
- 5 shares
- +$0.60 NET take-profit
- no stop loss

Disabled from active strategy execution:
BASE, EXT_CONFIRM, EXT_VETO, PRE_JUMP35.
The base strategy task is not scheduled at all.

External Binance/Bybit/Coinbase data collection, jump-event labeling,
source health, hourly ZIP reporting and trial 3/5/10/20s labels remain enabled.

Compatibility note:
PRE_JUMP keeps the previous variant name, so the existing 0.40 cash/statistics
stored in SQLite continue after redeploy. PRE_JUMP42 starts as a new variant.
The old PREJUMP_CONTROL_SCORE environment variable is ignored by v1.2.
Use PREJUMP_HIGH_SCORE=0.42.
