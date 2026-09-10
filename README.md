# Polymarket UltraFast Wallet CopyBot v1.1

## What changed in v1.1

Telegram now controls BUY sizing with four modes:

- **FIXED USD** — every copied BUY uses the selected fixed USD budget.
- **SAME USD 1:1** — mirrors the source trade's USD notional. The source amount is treated as the maximum spend at our slippage limit; because the bot deliberately skips a REST book lookup on the hot path, a better fill may spend slightly less.
- **SAME SHARES 1:1** — requests exactly the same number of shares as the source trade, subject to `MAX COPY USD` and exchange/minimum-size constraints.
- **SCALE %** — source USD notional multiplied by a selected percentage (25%, 50%, 100%, 200%, custom, etc.).

Every BUY is protected by **MAX COPY USD**. Default is `$100` per copied BUY. This cap remains active in all four sizing modes. `SLIPPAGE` remains an absolute probability-price limit and is independent of sizing mode.

When Data API provides `usdcSize`, v1.1 uses that exact source notional. RTDS documents `price` + `size`, so the live fast path otherwise calculates source USD as `price × shares` without adding a REST round trip.

SELL behavior remains independent via **SELL MODE**:

- **PROPORTIONAL** — mirror the fraction of the source wallet position sold using the bot's tracked source inventory.
- **FULL** — any source SELL closes that wallet's entire copied allocation for the asset.
- **OFF** — do not copy SELLs.

## Speed path

Primary detection:

`Polymarket RTDS activity/orders_matched + activity/trades -> in-memory wallet filter/dedupe -> sizing math in memory -> signed FAK`

The first LIVE order deliberately does **not** wait for a REST orderbook request. Sizing adds only in-memory arithmetic. REST `/trades` remains a reconnect/missed-event fallback.

## Telegram controls

- START / STOP
- **COPY SIZE** — FIXED / SAME USD / SAME SHARES / SCALE
- **AMOUNT** — fixed USD value used only in FIXED mode
- **SCALE %** — 25 / 50 / 75 / 100 / 150 / 200 / custom
- **MAX COPY** — hard USD cap per BUY
- SLIPPAGE — selectable/custom absolute price slippage
- WALLETS / ADD WALLET / REMOVE WALLET
- BALANCE
- REPORT overview + per-wallet report buttons
- MODE — PAPER / LIVE; LIVE requires confirmation
- SELL MODE — PROPORTIONAL / FULL / OFF
- NOTIFY

## Examples

Source wallet buys `40 shares @ 0.62`, source notional `$24.80`, our slippage is `0.05`, therefore BUY limit is `0.67`.

**SAME USD 1:1:** the bot requests up to `$24.80 / 0.67 = 37.0149 shares`. This guarantees the worst-case spend at the slippage cap does not exceed about `$24.80`; if execution is better than `0.67`, actual spend can be lower.

**SAME SHARES 1:1:** the bot requests `40 shares`. At a `0.67` worst-case fill this could cost `$26.80`, unless MAX COPY trims it.

**SCALE 50%:** the bot uses a `$12.40` maximum-spend budget, then converts it to shares at the same slippage cap.

If `MAX COPY = $20`, each of those modes is clipped so the BUY cannot be sized above roughly `$20` at the configured limit price.

## Important safety behavior

- Fresh process startup is always **STOP**.
- Fresh database defaults to **PAPER**.
- LIVE additionally requires `LIVE_MASTER_ENABLE=1`, Telegram LIVE confirmation, and START.
- Existing positions of a newly added source wallet are **not copied**; they are only used to seed proportional SELL tracking.
- Multiple watched wallets keep separate virtual allocations even if they trade the same outcome token.
- Unknown errors after POST begins are fail-closed (`AMBIGUOUS`) and are not blindly retried.
- Deterministic FAK NO_MATCH may receive one retry at the exact same slippage cap.
- SELL balance/allowance rejection may receive one delayed retry.
- Per-wallet reports show bot-tracked gross realized PnL and latency. v1.1 also tracks whether BUYs were clipped by MAX COPY.

## Coolify

Use the Dockerfile. Port `8080`; health path `/health`. Mount persistent storage at `/var/data`.

Minimal variables:

```text
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
PORT=8080
DATA_DIR=/var/data
POLYMARKET_PRIVATE_KEY=...
POLYMARKET_WALLET_ADDRESS=...
LIVE_MASTER_ENABLE=0

COPY_SIZE_MODE=FIXED
COPY_USDC=5
COPY_SCALE_PCT=100
MAX_COPY_USDC=100
COPY_SLIPPAGE=0.05
COPY_SELL_MODE=PROPORTIONAL
PAPER_START_BALANCE=500
```

After the first launch these BUY sizing values can be changed with Telegram buttons and are persisted in SQLite.

For real copying: set `LIVE_MASTER_ENABLE=1`, redeploy, use MODE -> LIVE -> CONFIRM LIVE, then START.
