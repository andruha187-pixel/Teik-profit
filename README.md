# UltraFast CopyBot v2.2 — wallet-feed audit fix

**Important v2.2 fix:** v2.0 could silently miss a wallet trade when RTDS did not deliver it first and the Data API exposed the trade more than 4 seconds after the trade timestamp. The old REST fallback used timestamp age as a discovery gate. v2.2 primes existing history once, then treats any newly appearing event key as a new event. Timestamp age now only controls whether a late REST event is still safe to COPY. It is always audited/notified within the audit lookback.

Additional feed hardening:
- resolves any entered user/profile address through `/public-profile` to the canonical `proxyWallet`;
- polls `/trades` every 0.5s by default;
- independently audits `/activity?type=TRADE` every 1s;
- late REST events are reported with an explicit `REST_LATE` reason instead of disappearing;
- STATUS shows REST poll/error/late counters.


## What v2.0 adds

This version keeps all v1.2 wallet-copy controls and audit messages, but adds a dedicated **BTC 15-minute acceleration lane**. It does **not** pre-copy or predict the watched wallet. No order is posted until a watched-wallet BUY/SELL event is actually detected.

Before that event arrives, the bot continuously prepares the deterministic `btc-updown-15m-<slot>` markets:

- discovers the **current and next** 15-minute BTC windows;
- keeps both current `UP` and `DOWN` CLOB token books live over the Polymarket market WebSocket (and preloads the next window for rollover);
- keeps the authenticated client transport warm;
- runs `create_limit_order()` locally once for each discovered outcome token to warm token/market metadata and the signer path, then discards the signed object **without posting it**;
- keeps all sizing, watched-wallet filtering and slippage math in memory;
- on the real watched-wallet event, goes directly to build/sign -> FAK submit with **no REST book/discovery request in front of the LIVE order**.

The exact copy order itself cannot be safely pre-signed ahead of the watched-wallet event because its side, source price and copied size/slippage limit are not known yet. v2.0 therefore warms the expensive/cold dependencies but creates a fresh signed FAK only after the real source event is seen.

### Latency audit

For BTC15 source events, Telegram result messages now include:

`🚀 BTC15 FAST LANE | book fresh ...ms | signer WARM/COLD`

LIVE results also measure `detect→submit` with a high-resolution monotonic clock, so sub-millisecond/millisecond differences are visible rather than rounded away by the old integer millisecond timer. SQLite stores `detect_to_submit_us`, `build_sign_us`, fast-lane book age/price and signer-warm status for later reports.

`STATUS` shows BTC15 market-WS connectivity, number of prepared assets, warmed signer assets and fast-lane hits. `/health` exposes the same diagnostics.

### Important limitation

The fast lane removes **our own** avoidable work after detection; it does not create a guaranteed 250 ms advance notice of another wallet's taker order. If Polymarket only exposes the wallet identity at/after the match event, v2.0 reacts at the first event it can see. There is deliberately **no PRE-COPY** mode in this build.

### Recommended variables for the accelerator

```text
BTC15_FASTLANE_ENABLE=1
BTC15_SLUG_PREFIX=btc-updown-15m
BTC15_DISCOVERY_INTERVAL_SEC=2
BTC15_BOOK_MAX_AGE_MS=1500
BTC15_SIGNER_PREWARM_ENABLE=1
BTC15_SIGNER_PREWARM_SIZE=5
BTC15_SIGNER_PREWARM_PRICE=0.50
BTC15_WS_MAX_AGE_SEC=240
```

All existing v1.2 Telegram controls remain unchanged: wallet add/remove, START/STOP, FIXED/SAME USD/SAME SHARES/SCALE, MAX COPY, slippage, SELL mode, PAPER/LIVE, balance and per-wallet reports.

---

# Polymarket UltraFast Wallet CopyBot v1.2


## What changed in v1.2 — source visibility / missed-trade audit

v1.2 adds a two-stage Telegram audit for every **unique watched-wallet trade that the bot actually sees**:

1. `🆕 НОВАЯ ПОЗИЦИЯ НАЙДЕНА` / `➕ ДОБОР ПОЗИЦИИ НАЙДЕН` / `➖ SELL ПОЗИЦИИ НАЙДЕН` is queued immediately when the source event is accepted. Telegram is intentionally kept off the order hot path.
2. A second message reports the copy result: `✅ ПОЗИЦИЯ ИСПОЛНЕНА`, `⚠️ ЧАСТИЧНО ИСПОЛНЕНА`, `❌ ПОЗИЦИЯ НЕ ИСПОЛНЕНА`, or `⚠️ РЕЗУЛЬТАТ ОРДЕРА НЕОДНОЗНАЧЕН`.

A failed/skipped copy includes a human-readable reason such as FAK `NO_MATCH`, no visible PAPER liquidity, order below minimum size, bot STOP, LIVE wallet not ready, SELL copy disabled, no copied position, balance/allowance failure, or an ambiguous API/transport result.

If the primary RTDS stream did not win the race and the REST fallback discovers the trade, the FOUND message explicitly says:

`⚠️ Найдено через REST fallback — RTDS не был первым источником этой сделки.`

STATUS now shows `unique / raw / dup` watched-wallet event counters. Per-wallet REPORT shows how many decisions came from RTDS versus REST fallback and how many source trades were seen while the bot was STOPPED. Source trades seen while STOP are now persisted as `SKIPPED / BOT_STOPPED` instead of disappearing silently.

The FOUND and RESULT notices are sent through a single FIFO notification queue. This preserves their order without waiting on Telegram before signing/submitting the copy order.

The original cross-feed dedupe protection is intentionally retained unchanged so two RTDS representations of the same transaction cannot create duplicate real orders.

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
- Per-wallet reports show bot-tracked gross realized PnL and latency. v1.2 also tracks whether BUYs were clipped by MAX COPY and adds RTDS/REST feed-audit counters.

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


## v2.2 shadow inventory fix

- Current-position seeding now counts only positive non-redeemable sizes.
- A successful refresh clears stale target-shadow rows before reseeding.
- Closed/zero-size positions are no longer reported as open assets.

## v2.3 — durable delivery recovery

v2.3 keeps the first-copy hot path unchanged, but a failed or uncertain LIVE
copy is no longer abandoned after the first FAK.

- `REJECTED_NO_MATCH`, a partial fill, or a transient server/rate-limit failure
  creates a durable SQLite recovery job for the unfilled remainder.
- `AMBIGUOUS` is **not proof of zero fill**. The POST may have reached the CLOB
  even though the response was lost. Therefore v2.3 first reconciles the bot's
  own wallet trade history before it is allowed to send another order.
- Recovery always uses the original event's slippage limit. It does not silently
  chase a worse price.
- For BTC15, the warmed WebSocket book is used to wait for marketable liquidity
  before another FAK is sent. This avoids hammering guaranteed `NO_MATCH` orders.
- Recovery state is persisted in SQLite and survives redeploy/restart. Startup
  remains STOP; queued jobs resume only after explicit START.
- BTC15 recovery expires at the end of that market's trading window (or the
  configured maximum age, whichever comes first). A fill can never be literally
  guaranteed if price/liquidity never returns inside the user's slippage cap.

The found/result notices now also print `source→detect ≈...ms` when the public
source timestamp is available. This is an estimate because Data API activity
timestamps are normally second-resolution. `detect→submit` remains the precise
local hot-path measurement.

Feed counters were also corrected: RTDS counters now contain RTDS-only events;
REST detections have separate `REST detected unique/raw` counters. A `📡 STATUS`
button is included on the Telegram keyboard.

Default recovery settings:

```text
RECOVERY_ENABLE=1
RECOVERY_BOOK_POLL_MS=50
RECOVERY_FAK_RETRY_MS=250
RECOVERY_AMBIGUOUS_GRACE_MS=2500
RECOVERY_RECONCILE_INTERVAL_MS=250
RECOVERY_MAX_AGE_SEC=900
RECOVERY_OWN_TRADES_LIMIT=200
```
