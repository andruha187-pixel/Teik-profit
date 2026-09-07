# v20.11 PRESIGN PREWARM — first FAK cold-start fix

Эта версия построена поверх v20.10 ULTRA LOW LATENCY. **Стратегия и SAFE-фильтры не менялись.**
Изменён только путь подготовки LIVE-ордера после того, как реальные логи показали:

- SOL FIRST `build/sign = 256ms`, RETRY `5ms`;
- XRP FIRST `build/sign = 272ms`, RETRY `6ms`.

## Что изменено

- За ~12 секунд до следующего 5-минутного слота бот находит уже обнаруженные рынки токенов, которые стоят в `LIVE`.
- Для **Up и Down** каждого такого рынка выполняется настоящий `create_limit_order()` и локальная подпись.
- Подписанный объект сразу выбрасывается. **`post_order()` из presign-prewarm не вызывается никогда**, поэтому prewarm не выставляет реальный ордер и не двигает средства.
- Прогревается именно тот путь SDK, который был холодным на первом FAK: token/market metadata + signer/build.
- Prewarm запускается после окончания обычного PRE_JUMP entry-window предыдущего рынка и дополнительно не запускается, если сейчас активен реальный LIVE order lock.
- При переключении токена в LIVE бот также делает best-effort background prewarm, но не конкурирует с активным рынком, если `START` уже включён.
- Старый read-only transport prewarm через `get_balance_allowance(COLLATERAL)` сохранён.
- Immediate WS retry из v20.10 сохранён: `0ms`, без REST по умолчанию.
- Telegram telemetry теперь показывает `warm yes/<N>ms` или `warm no` в FIRST/RETRY.
- Presign bookkeeping очищается вместе со старыми 5-минутными рынками, чтобы token ids не копились в памяти.

Цель: первый реальный `build/sign` должен стать ближе к уже наблюдавшимся `5–6ms` на RETRY вместо `250–270ms`. Это **не гарантируется заранее** — следующий LIVE лог покажет фактический эффект на сервере. API RTT Polymarket (около 250–370ms в последних логах) этим изменением не устраняется.

## Рекомендуемые execution env

```env
LIVE_ENTRY_MAX_SLIPPAGE=0.05
LIVE_ENTRY_NO_MATCH_RETRIES=1
LIVE_ENTRY_RETRY_DELAY_MS=0
LIVE_ENTRY_RETRY_FORCE_REST=0
LIVE_ENTRY_FORCE_REST_BOOK=0

LIVE_PREWARM_ENABLE=1
LIVE_PREWARM_INTERVAL_SEC=30
LIVE_PREWARM_LEAD_SEC=3

LIVE_PRESIGN_PREWARM_ENABLE=1
LIVE_PRESIGN_PREWARM_LEAD_SEC=12
LIVE_PRESIGN_PREWARM_SIZE=5

FAST_INTERVAL=0.10
EVENT_DRIVEN_LIVE_ENTRY=1
EVENT_DRIVEN_MIN_INTERVAL_MS=5
```

Все BTC/SOL/XRP/BNB/DOGE/ETH SAFE-фильтры из v20.9/v20.10 сохранены как есть. HYPE не менялся.

## Как проверить, что fix реально сработал

Перед новым слотом в Coolify log должны появляться строки вида:

```text
LIVE PRESIGN WARM XRP Up ... build/sign=...ms | LOCAL ONLY / NOT POSTED
LIVE PRESIGN WARM XRP Down ... build/sign=...ms | LOCAL ONLY / NOT POSTED
```

А следующий `LIVE BUY` / `LIVE ENTRY MISSED` покажет:

```text
FIRST[warm yes/260ms, sig→book 2ms, build/sign 6ms, sig→submit 8ms, API ...]
```

`warm yes/260ms` означает, что 260ms были потрачены **заранее**, до начала 5-минутного рынка, а не во время сигнала. Важная цифра — новый FIRST `build/sign`.

---

# MULTI7 PRE-JUMP — PAPER + LIVE

Отдельный торговый бот для BTC/XRP/BNB/SOL/ETH/DOGE/HYPE на логике PRE_JUMP.
Исследовательский 4-way PAPER-бот не нужен этому сервису и может работать отдельно.

## Стратегия по умолчанию

**v20.9** сохраняет базовый PRE_JUMP 0.40 и поверх первого иначе валидного сигнала
применяет отдельные SAFE-фильтры для BTC/SOL/XRP/BNB/DOGE/ETH. HYPE остаётся
на базовой логике без дополнительного SAFE-фильтра.


- Polymarket 5m Up/Down.
- Вход один раз на рынок.
- PM ask: `0.52..0.66`.
- Внешний directional score: `>= 0.40` по умолчанию.
- Минимум 2 свежих same-side venue votes.
- PM ask momentum за ~1 сек: `-0.01..+0.05`.
- Время входа: `1..160` секунд после старта 5m рынка.
- ENTRY: 5 shares по умолчанию.
- DCA: нет.
- Stop-loss: нет.
- Take-profit: `+$0.60 NET` на всю оставшуюся позицию по умолчанию.

Score можно менять из Telegram в реальном времени. Жёсткий минимум в коде — `0.40`; ниже бот не разрешит поставить.

## Telegram

Клавиатура:

```text
START                 STOP
MODES                 SIZES
BALANCE               POSITIONS
STATISTICS            TRADES
TAKE PROFIT            WALLET
-SCORE      SCORE      +SCORE
EMERGENCY STOP
```

### SCORE

```text
SCORE 0.42
SCORE +0.01
SCORE -0.01
SCORE ENV
```

Кнопки `+ SCORE` / `- SCORE` меняют порог на 0.01. `SCORE ENV` возвращает значение `PREJUMP_SCORE` из Environment.
Изменение применяется только к будущим входам; открытая позиция не меняется.

### Размер

```text
SIZE BTC 5
SIZE ETH 3
SIZE ALL 5
```

Размер нельзя менять для токена, пока у него есть открытая bot-tracked позиция.

### Режимы PAPER / LIVE / OFF

```text
MODE BTC PAPER
MODE BTC OFF
MODE BTC LIVE
CONFIRM LIVE BTC
```

Для LIVE требуется второе подтверждение в течение 60 секунд.

## LIVE safety

Реальный BUY/SELL невозможен, пока одновременно не выполнены все условия:

1. `LIVE_MASTER_ENABLE=1` в Environment;
2. wallet SDK и credentials готовы;
3. токен переведён через `MODE <TOKEN> LIVE`;
4. затем отправлено `CONFIRM LIVE <TOKEN>` в течение 60 секунд;
5. глобальный `START` включён;
6. текущий PRE_JUMP сигнал проходит все фильтры.

LIVE execution сохраняет защищённый wrapper предыдущего торгового бота:

- fresh Polymarket book перед execution;
- signed LIMIT order, преобразованный в FAK;
- FAK не оставляет намеренно незаполненный остаток висеть в стакане;
- при неоднозначной ошибке submission используется fail-closed: повтор возможного дублирующего реального ордера автоматически не отправляется;
- TP продолжает контролироваться после STOP;
- EMERGENCY STOP блокирует новые входы и очищает ожидающие LIVE confirmations, но не делает аварийный market-sell открытых позиций.

Winning LIVE shares, оставшиеся до settlement, бот автоматически не redeem-ит.

## TAKE PROFIT

```text
TP 0.60
TP 0.90
TP OFF
TP ENV
```

TP — NET цель для всей оставшейся позиции после учёта entry fee и projected exit fee.

## Первый запуск в Coolify

1. Загрузить файлы репозитория.
2. Persistent storage: `/var/data`.
3. Port: `8080`; health endpoint: `/health`.
4. Добавить Telegram variables и wallet secrets только через Environment/Secrets.
5. Первый deploy оставить с `LIVE_MASTER_ENABLE=0`.
6. Нажать `WALLET` и проверить `SDK: READY` и collateral.
7. Для реальной торговли поставить `LIVE_MASTER_ENABLE=1` и redeploy.
8. Перед `START` перевести нужные токены в LIVE: `MODE BTC LIVE` -> `CONFIRM LIVE BTC`.

По умолчанию все токены после новой базы находятся в PAPER и глобальные ENTRY выключены.

## Важное про ключ

`POLYMARKET_PRIVATE_KEY` не класть в GitHub, Telegram, скриншоты или сообщения. Только Environment/Secret store на сервере.

## Database

```text
/var/data/prejump_multi7_paper_live.db
```

В ней сохраняются PAPER/LIVE fills, сигналы, score threshold на момент сигнала, позиции, TP exits и settlement statistics.

## Tests

```text
python test_prejump_live.py
```

Ожидается:

```text
PRE-JUMP PAPER/LIVE regression: OK
```

## LIVE FAK NO_MATCH handling (v20.1)

PRE-JUMP can move faster than the Polymarket book snapshot. A deterministic
`no orders found to match with FAK order` response is now stored as
`REJECTED_NO_MATCH`, **not** `AMBIGUOUS`. It represents a zero-fill killed FAK.

For LIVE ENTRY the bot now:

1. forces a fresh REST book immediately before submission;
2. uses the accepted signal ask plus `LIVE_ENTRY_MAX_SLIPPAGE` as a hard BUY cap;
3. on deterministic NO_MATCH only, waits `LIVE_ENTRY_RETRY_DELAY_MS`;
4. rechecks PRE-JUMP direction/score/venue votes and performs at most
   `LIVE_ENTRY_NO_MATCH_RETRIES` additional attempt(s);
5. never retries timeouts, transport failures or unknown submission errors. Those
   remain `AMBIGUOUS` and fail-closed.

Defaults:

```text
LIVE_ENTRY_MAX_SLIPPAGE=0.01
LIVE_ENTRY_NO_MATCH_RETRIES=1
LIVE_ENTRY_RETRY_DELAY_MS=150
LIVE_ENTRY_FORCE_REST_BOOK=0
```

The slippage cap is also bounded by `PREJUMP_PRICE_MAX`, so an accepted signal
at 0.66 cannot be chased above 0.66.



## v20.2 tick-safe LIVE price

LIVE limit prices are aligned to the Polymarket outcome token tick before signing.
The bot preserves `tick_size` from fresh order-book snapshots when available and
uses `LIVE_PRICE_TICK_FALLBACK=0.01` otherwise. BUY limits are rounded DOWN to
the tick so normalization never exceeds the configured slippage cap.

Errors raised while building/signing the order, before any `post_order` call, are
recorded as `REJECTED_LOCAL` and are not treated as ambiguous submissions.
Unknown failures after submission remain fail-closed.


## v20.3 LIVE TP balance propagation

A freshly matched LIVE BUY can briefly be visible in bot/CLOB execution history before
the acquired outcome-token balance is available to a subsequent SELL. To avoid a
false fail-closed TP in that short window:

- the first LIVE TP SELL is delayed by `LIVE_TP_MIN_HOLD_MS` after the newest BUY;
- explicit `not enough balance / allowance` TAKE_PROFIT rejections are stored as
  `REJECTED_BALANCE_ALLOWANCE`, not `AMBIGUOUS`;
- no unknown fill is assumed for that deterministic rejection;
- the tracked position remains open and TP retries on later cycles with
  `LIVE_TP_BALANCE_RETRY_DELAY_MS` backoff;
- unknown timeouts/transport failures after submission still remain AMBIGUOUS and
  fail-closed.

Defaults:

```text
LIVE_TP_MIN_HOLD_MS=2000
LIVE_TP_BALANCE_RETRY_DELAY_MS=1000
```

At startup v20.3 also repairs old v20.2 `AMBIGUOUS` TP rows whose stored error
explicitly says `not enough balance`, so an already affected open position can
resume TP attempts after redeploy.


## v20.4 LIVE ENTRY diagnostics — ENTRY FROZEN

This build does **not** change PRE-JUMP signal thresholds, score calculation,
entry window, book/slippage guards, FAK submission, retry rules, or TP logic from
v20.3. It only adds a Telegram diagnostic **after** a LIVE ENTRY attempt has
already failed.

A failed accepted signal now reports:

```text
LIVE ENTRY MISSED BTC
Signal SEEN: Up | score ... | mom ... | votes ... | fresh ... | t=...s
signal ask ... | live ask ... | cap ...
EXECUTION: <exact reason>
```

Interpretation:
- If the research bot enters and LIVE shows `LIVE ENTRY MISSED`, the PRE-JUMP
  signal was seen by LIVE and the failure happened during real execution.
- If the research bot enters but LIVE shows neither `LIVE BUY` nor
  `LIVE ENTRY MISSED`, LIVE never accepted that PRE-JUMP signal; compare the
  signal-stage book/momentum feeds.

No additional REST call or Telegram send is performed **before** the LIVE order
attempt; diagnostics are emitted only after a failed attempt, so they do not add
latency to successful ENTRY execution.


## v20.5 low-latency first LIVE ENTRY

PRE-JUMP signal rules and thresholds are unchanged. The execution path after an
already-accepted LIVE signal is faster:

- `FAST_INTERVAL` default is now `0.10` seconds.
- The first LIVE PRE-JUMP FAK no longer performs a mandatory REST book refresh.
- It immediately uses the current fresh WebSocket book and the existing hard
  `LIVE_ENTRY_MAX_SLIPPAGE` cap.
- If the WS book is stale/missing/crossed, the existing book-safety logic can
  refresh it before execution.
- A deterministic FAK `NO_MATCH` retry still uses a fresh REST snapshot and
  revalidates direction/score/votes before retrying.
- Unknown post-submission failures remain fail-closed.

Recommended execution env for this build:

```text
FAST_INTERVAL=0.10
LIVE_ENTRY_MAX_SLIPPAGE=0.03
LIVE_ENTRY_NO_MATCH_RETRIES=1
LIVE_ENTRY_RETRY_DELAY_MS=150
LIVE_ENTRY_FORCE_REST_BOOK=0
```

`LIVE_ENTRY_FORCE_REST_BOOK` is retained only for configuration compatibility;
v20.5 does not allow an old value of `1` to force a REST round-trip on the first
accepted PRE-JUMP BUY.


## v20.6 event-driven LIVE ENTRY + latency telemetry

PRE-JUMP **rules are unchanged**: score threshold, venue votes, Polymarket price
band, 1-second momentum gate and 1..160 second window are the same. The change is
only *when* a LIVE token evaluates those same rules and how quickly an accepted
signal reaches the CLOB.

- External Binance/Bybit/Coinbase WS updates wake a per-symbol evaluator instead
  of waiting for the next 100ms fallback tick.
- The event path is used only for tokens currently in `LIVE`; PAPER remains on
  the 100ms timer path for clean comparison with the research bot.
- Bursts are coalesced with `EVENT_DRIVEN_MIN_INTERVAL_MS` (default 5ms) so feed
  readers stay non-blocking.
- A per-market/strategy asyncio lock prevents the event path and timer fallback
  from creating duplicate signals/orders.
- First FAK still uses the current fresh Polymarket WS book; no mandatory REST
  RTT is added before submission.
- Requested LIVE slippage default is now `0.05`, still hard-bounded by
  `PREJUMP_PRICE_MAX=0.66`.
- LIVE BUY and `LIVE ENTRY MISSED` Telegram messages include latency telemetry:
  `event→signal`, `signal→book`, `signal→submit`, API response and total
  `signal→response` milliseconds.

Recommended env:

```text
FAST_INTERVAL=0.10
EVENT_DRIVEN_LIVE_ENTRY=1
EVENT_DRIVEN_MIN_INTERVAL_MS=5
LIVE_ENTRY_MAX_SLIPPAGE=0.05
LIVE_ENTRY_NO_MATCH_RETRIES=1
LIVE_ENTRY_RETRY_DELAY_MS=150
LIVE_ENTRY_FORCE_REST_BOOK=0
```

If `signal→submit` is already only a few/tens of milliseconds but Polymarket ask
still moves from, for example, 0.57 to 0.75, the remaining gap is market repricing
rather than the bot's timer/REST latency.

## v20.7 BTC SAFE43 — configurable first-signal filter

This build keeps the v20.6 event-driven/low-latency execution and all non-BTC
PRE-JUMP rules unchanged. It adds two BTC-only environment variables:

```text
BTC_PREJUMP_SCORE=0.43
BTC_PREJUMP_PRICE_MAX=0.55
```

The semantics are deliberately **first normal PRE-JUMP candidate**, not simply a
higher BTC threshold. The bot first evaluates the existing normal PRE-JUMP family
(global score, external direction/votes, PM ask 0.52..0.66, 1s PM momentum, time
window). On the **first otherwise-valid BTC candidate**:

- if directional score >= `BTC_PREJUMP_SCORE` AND signal ask <=
  `BTC_PREJUMP_PRICE_MAX`, the candidate is accepted and proceeds to the same LIVE
  execution path as v20.6;
- otherwise that BTC 5-minute market is permanently skipped. It does **not** wait
  for a later stronger signal.

This distinction matters because the historical SAFE43 screen was measured on the
first normal 0.40 PRE-JUMP signal. Merely setting the global score to 0.43 would
produce a different trade set.

The BTC max above is the **signal ask filter**, not the LIVE fill ceiling. With
`LIVE_ENTRY_MAX_SLIPPAGE=0.05`, an accepted BTC signal at 0.55 can still submit a
limit up to 0.60 (subject to the existing global 0.66 safety ceiling).

Recommended values for the currently tested BTC profile:

```text
PREJUMP_SCORE=0.40
BTC_PREJUMP_SCORE=0.43
BTC_PREJUMP_PRICE_MAX=0.55
LIVE_ENTRY_MAX_SLIPPAGE=0.05
```

Set `BTC_PREJUMP_SCORE=0.40` and `BTC_PREJUMP_PRICE_MAX=0.66` to effectively return
BTC to the normal global PRE-JUMP filter.



## v20.8 MULTI SAFE — first-signal filters for LIVE forward test

Этот build сохраняет v20.6/v20.7 event-driven, FAK, slippage, retry, TP и
fail-closed execution. Изменена только дополнительная проверка **первого иначе
валидного базового PRE-JUMP сигнала** для выбранных токенов. Если фильтр не
пройден, entry gate этого 5-minute рынка закрывается навсегда; более поздний
сигнал его не возобновляет.

Рекомендуемые значения из текущего 30h PAPER-анализа:

```text
PREJUMP_SCORE=0.40
BTC_PREJUMP_SCORE=0.43
BTC_PREJUMP_PRICE_MAX=0.55
SOL_PREJUMP_PM_MOM_MAX=0.02
XRP_PREJUMP_SCORE=0.41
BNB_PREJUMP_PM_MOM_MIN=0.01
DOGE_PREJUMP_MAX_SPREAD=0.02
LIVE_ENTRY_MAX_SLIPPAGE=0.05
```

Логика:

- BTC: score >= 0.43 AND signal ask <= 0.55;
- SOL: PM 1s momentum <= +0.02;
- XRP: score >= 0.41;
- BNB: PM 1s momentum >= +0.01;
- DOGE: свежий non-crossed spread `ask-bid` <= 0.02; отсутствующий bid тоже fail;
- ETH/HYPE: дополнительных SAFE-фильтров нет.

Это параметры для **forward LIVE проверки**, а не гарантия будущего winrate.
Исторический 100% результат был получен на ограниченной выборке; реальные fills,
slippage и смена рыночного режима могут дать другой результат. Для первого LIVE
прогона разумно сохранить текущий маленький размер 5 shares и не повышать его до
накопления новой forward-выборки.

Regression suite:

```text
python test_multi_safe_filters.py
python test_event_driven_entry.py
python test_low_latency_entry.py
python test_nomatch_retry.py
python test_prejump_live.py
python test_tick_alignment.py
python test_tp_balance_sync.py
```


## v20.9 ETH SAFE — HYPE unchanged

Добавлен только ETH-specific FIRST-base-signal фильтр. Остальные SAFE-профили,
event-driven/low-latency execution, FAK, slippage, retry, TP и fail-closed логика
остаются такими же, как в v20.8. HYPE намеренно не фильтруется дополнительно.

Рекомендуемые ETH значения из текущего накопленного анализа и следующего 6h batch:

```text
ETH_PREJUMP_SCORE=0.455
ETH_PREJUMP_PM_MOM_MAX=0.02
```

Семантика такая же, как у остальных SAFE-профилей: на **первом иначе валидном
базовом PRE-JUMP кандидате** ETH должен одновременно иметь directional score
`>= 0.455` и PM 1s momentum `<= +0.02`. Если хотя бы одно условие не проходит,
этот 5-minute ETH market пропускается навсегда; бот не ждёт более красивый
поздний сигнал.

HYPE продолжает использовать только базовые глобальные PRE-JUMP условия и может
оставаться PAPER/OFF для дальнейшего сбора forward-данных.

Исторический 12/12 ETH SAFE результат не является гарантией будущего результата;
для LIVE сохраняйте малый размер до накопления новой forward-выборки.
