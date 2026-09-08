MULTI7 PRE-JUMP LAB v1.6 — 4 controls + PRE_LEAD + PRE_LEAD_SAFE + PRE_LEAD_CONFIRM
================================================================================

PAPER ONLY. No private key and no LIVE order placement.

Existing branches are kept unchanged:
  PRE_JUMP       score >= 0.40, elapsed 1..160s
  PRE_JUMP42     score >= 0.42, elapsed 1..160s
  PRE_JUMP10     score >= 0.40, elapsed 10..120s
  PRE_JUMP42_10  score >= 0.42, elapsed 10..120s
  PRE_LEAD       original early projection branch
  PRE_LEAD_SAFE  first PRE_LEAD candidate + projected >= 0.55 + ask <= 0.56

NEW: PRE_LEAD_CONFIRM
---------------------
The first candidate that would qualify for PRE_LEAD_SAFE is binding for this
branch. It does NOT wait for a nicer later candidate in the same 5-minute market.

Default sequence:
  1) first PRE_LEAD_SAFE candidate appears
  2) wait PRELEAD_CONFIRM_MS=125ms
  3) original direction must still be supported by >=2 fresh venues/votes
  4) directional external score may fade by at most 0.01 from the candidate
  5) confirm ask must still be <=0.56 and PM momentum must remain in the normal band
  6) only then the branch records a confirmed signal / simulated submission
  7) after submission it STILL waits the full PRELEAD_SIM_DELAY_MS=250ms
  8) PAPER fill is capped at confirm ask +0.05 and hard PRELEAD_PRICE_MAX

So PRE_LEAD_CONFIRM pays both costs honestly: ~125ms confirmation + ~250ms
Polymarket taker-delay simulation. This is deliberate.

Frozen forward-test defaults:
  PRELEAD_CONFIRM_MS=125
  PRELEAD_CONFIRM_MAX_SCORE_FADE=0.01
  PRELEAD_CONFIRM_PRICE_MAX=0.56
  PRELEAD_CONFIRM_MIN_VENUES=2

Do not tune these during the first forward sample.

Hourly ZIP adds:
  prelead_confirm_checks.csv      every first SAFE candidate and PASS/REJECT
  prelead_confirm_execution.csv   delayed execution after confirmed signals
  prelead_confirm_alignment.csv   lead/lag vs ordinary PRE_JUMP and PM jumps

The previous PRE_LEAD and PRE_LEAD_SAFE CSVs remain separate and unchanged.

Deployment
----------
Dockerfile and requirements.txt are in the ZIP root. Put all files directly in
the GitHub repository root used by Coolify, then redeploy.
