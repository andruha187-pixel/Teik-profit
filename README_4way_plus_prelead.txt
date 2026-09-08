MULTI7 PRE-JUMP LAB v1.5 — 4 controls + PRE_LEAD + PRE_LEAD_SAFE
=================================================================

PAPER ONLY. No wallet/private key and no real orders.

The existing five branches are preserved:
  PRE_JUMP      score >= 0.40, elapsed 1..160s
  PRE_JUMP42    score >= 0.42, elapsed 1..160s
  PRE_JUMP10    score >= 0.40, elapsed 10..120s
  PRE_JUMP42_10 score >= 0.42, elapsed 10..120s
  PRE_LEAD      original early branch, unchanged

New sixth branch:
  PRE_LEAD_SAFE

PRE_LEAD_SAFE default signal
----------------------------
It must first satisfy the exact same PRE_LEAD candidate rules:
- independent loop: 100ms
- current directional score >= 0.34 but still < 0.40
- same direction already present in the ~300ms lookback sample
- score increased by at least +0.015
- linear 300ms projection reaches >= 0.40
- >= 2 same-side venue votes
- PM ask 0.52..0.66
- PM 1s momentum -0.01..+0.05
- elapsed 1..160s

Then PRE_LEAD_SAFE adds ONLY the two frozen forward-test filters:
- projected_score >= 0.55
- signal ask <= 0.56

These values were selected from the first 8h PRE_LEAD sample and should now be
left unchanged while collecting the next forward sample. The earlier 18/18 is
in-sample and is NOT a guarantee of future performance.

Execution model
---------------
Both PRE_LEAD branches deliberately wait PRELEAD_SIM_DELAY_MS=250ms before
PAPER execution, then attempt full-size execution from the then-current book at:

  max buy price = min(0.66, signal_ask + 0.05)

So PRE_LEAD_SAFE does NOT get an instant virtual fill. Example: signal ask 0.56
allows delayed execution only up to 0.61. If the market has moved beyond that,
the signal is recorded as a miss.

Hourly ZIP files
----------------
Original PRE_LEAD stays in:
  prelead_execution.csv
  prelead_alignment.csv

PRE_LEAD_SAFE has separate files:
  prelead_safe_execution.csv
  prelead_safe_alignment.csv

strategy_summary.csv / signal_events.csv / paper_trades.csv / market_results.csv
also contain PRE_LEAD_SAFE as a normal independent strategy variant.

Forward-test env additions
--------------------------
PRELEAD_SAFE_PROJECTED_SCORE=0.55
PRELEAD_SAFE_PRICE_MAX=0.56

Keep the existing PRE_LEAD and four PRE_JUMP settings unchanged for a clean
forward comparison.
