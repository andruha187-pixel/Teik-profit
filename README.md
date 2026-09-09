# PRE_LEAD_SAFE LIVE-SIM v1.7

PAPER-only research bot. No wallet/private key and no real Polymarket orders.

## What changed

Only `PRE_LEAD_SAFE` remains active. The four PRE_JUMP controls, raw PRE_LEAD and PRE_LEAD_CONFIRM are disabled.

ENTRY keeps the existing forward-tested PRE_LEAD_SAFE signal and simulates LIVE execution:
- first early candidate is binding;
- projected score >= 0.55;
- signal ask <= 0.56;
- wait 250 ms;
- FAK-style full-size PAPER fill, capped at signal ask + 0.05 and hard market band.

TP is now much closer to the current LIVE bot:
- Telegram-adjustable whole-position NET target;
- default +$0.90 for this new forward run;
- minimum 2 s hold after entry (balance/allowance propagation analogue);
- trigger from fresh BID depth;
- freeze the worst visible full-size SELL limit at trigger;
- wait 250 ms;
- after the delay the full position must still be executable at/above that frozen limit, otherwise it is recorded as `NO_MATCH` and the position remains open for a later attempt.

The bot uses a new DB file (`prejump_lab_live_sim_v17.db`) so the new TP experiment does not mix with old v1.5/v1.6 statistics.

## Telegram TP control

Buttons:
- `➖ TP` = -$0.10
- `🎯 TAKE PROFIT` = show current TP
- `➕ TP` = +$0.10

Exact value:
- `TP 0.90`
- `TP 0,90` also works

A TP change applies immediately to currently open PAPER positions.

## Hourly ZIP

Important files:
- `prelead_safe_execution.csv` — delayed ENTRY simulation
- `tp_live_sim_execution.csv` — TP trigger, frozen limit, 250ms delay, FILLED/NO_MATCH and realized PnL
- `paper_trades.csv`
- `paper_exits.csv`
- `market_results.csv`
- `external_features.csv`

## Coolify

Keep `Dockerfile`, `requirements.txt`, and `main.py` in the repository root. Existing Telegram/source variables can be reused. See `.env.example` for the new TP variables.
