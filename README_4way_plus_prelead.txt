MULTI7 PRE-JUMP LAB v1.4 — 4 controls + PRE_LEAD
=================================================

PAPER ONLY. No wallet/private key and no real orders.

The existing four branches are preserved unchanged:
  PRE_JUMP      score >= 0.40, elapsed 1..160s
  PRE_JUMP42    score >= 0.42, elapsed 1..160s
  PRE_JUMP10    score >= 0.40, elapsed 10..120s
  PRE_JUMP42_10 score >= 0.42, elapsed 10..120s

New experimental branch:
  PRE_LEAD

Goal
----
Test whether the external Binance/Bybit/Coinbase score can forecast the normal
PRE_JUMP 0.40 crossing roughly 250-350ms earlier, which is the scale relevant
to the Polymarket taker hold discussed in the order-lifecycle docs.

Default PRE_LEAD signal
-----------------------
- independent loop: 100ms
- current directional score >= 0.34 but still < 0.40
- same direction was already present in the lookback sample
- score increased by at least +0.015 over ~300ms
- linear 300ms projection reaches >= 0.40
- >= 2 same-side venue votes
- PM ask 0.52..0.66
- PM 1s momentum -0.01..+0.05
- elapsed 1..160s

Important execution model
-------------------------
PRE_LEAD does NOT give itself an unrealistic instant PAPER fill.
After a lead signal it waits PRELEAD_SIM_DELAY_MS=250ms, then tries to buy
5 shares from the then-current PM book with a hard price cap:

  min(0.66, signal_ask + 0.05)

If price has already moved beyond the cap or full size is unavailable, the
PRE_LEAD signal is recorded but the PAPER entry is missed. This is intended to
be a more realistic proxy for LIVE FAK behavior than an instant paper fill.

The original four branches do not use this artificial delay and are intentionally
unchanged so historical comparison remains valid.

Hourly ZIP additions
--------------------
prelead_execution.csv
  Delayed-execution result for every PRE_LEAD signal: FILLED, MISSED_PRICE,
  MISSED_NO_BOOK, etc., including actual delay, signal ask, delayed ask and cap.

prelead_alignment.csv
  For every PRE_LEAD signal:
  - score_now / score_prev / delta / projection
  - lead_to_prejump_ms: time until the normal PRE_JUMP branch fires same direction
  - lead_to_jump_ms: time until first recorded same-direction PM +0.08 jump
  - delayed execution status and price

These two files are the main ones to send back for deciding whether the lead
logic actually compensates the delay or is only predicting noise.

Suggested env values
--------------------
LEAD_INTERVAL=0.10
PRELEAD_MIN_SCORE=0.34
PRELEAD_TARGET_SCORE=0.40
PRELEAD_LOOKBACK_MS=300
PRELEAD_HORIZON_MS=300
PRELEAD_MIN_DELTA=0.015
PRELEAD_HISTORY_TOLERANCE_MS=180
PRELEAD_MIN_VENUES=2
PRELEAD_MIN_ELAPSED=1
PRELEAD_MAX_ELAPSED=160
PRELEAD_PRICE_MIN=0.52
PRELEAD_PRICE_MAX=0.66
PRELEAD_PM_MOM_MIN=-0.01
PRELEAD_PM_MOM_MAX=0.05
PRELEAD_SIM_DELAY_MS=250
PRELEAD_SIM_MAX_SLIPPAGE=0.05

Do not change the four existing PRE_JUMP variables while this forward test is
running if you want clean comparison with the prior report history.
