# Electra pending-state queues

Starting with Electra, the beacon state exposes three post-validation queues
that this indexer scrapes at daily cadence alongside `validators`:

- `pending_consolidations` — EIP-7251 consolidation queue.
- `pending_deposits` — EIP-7002 deposit queue (churn-bounded activation).
- `pending_partial_withdrawals` — EIP-7002 scheduled partial withdrawals.

## Why these matter

The beacon chain spec runs a stack of validity checks on each submitted
`ConsolidationRequest` / `DepositRequest` / `WithdrawalRequest` (event stream
in `execution_requests`) **before** accepting the request into these state
queues. For consolidations alone, `process_consolidation_request` drops
requests when any of the following hold:

- `source == target` (can't be used as an exit);
- pending consolidations queue is full;
- insufficient churn limit;
- source or target pubkey doesn't exist;
- source lacks the correct withdrawal credentials or address mismatch;
- target lacks compounding (0x02) withdrawal credentials;
- either validator is inactive;
- either validator already has an exit initiated;
- source hasn't been active long enough;
- source has pending withdrawals queued.

An entry that shows up in `pending_consolidations` is guaranteed to apply.
An entry that shows up in `execution_requests` is merely a submission —
it may never apply (about 4× of the raw consolidation events on Gnosis are
operator resubmissions).

Similarly, `PendingDeposit.amount` is the exact amount the beacon chain will
credit when the queue drains. The corresponding `execution_requests` entry
carries the *requested* amount, which on post-Pectra networks can be up to
2048 GNO per request while the actual per-day credit is a few GNO through
the activation churn queue.

## Scrape cadence

**Per-slot** — same as `blocks`, `rewards`, and `data_column_sidecars`. Every
slot produces one snapshot of each queue (stored with `payload_hash` dedup so
consecutive slots with identical queue state collapse at the `ReplacingMergeTree`
merge layer).

This is a deliberate symmetry with existing slot-based loaders: pending-queue
entries can have short on-chain lifetimes (an entry added to
`pending_consolidations` in epoch N can be processed at the N+1 epoch boundary,
a span of as few as ~80s on Gnosis). Sampling less frequently than per-slot
risks missing entries that enter and leave the queue between two samples.

Chunking in `src/services/loader.py` treats these loaders the same as
`blocks`/`rewards`: `CHUNK_SIZE`-wide slot ranges, processed in parallel by
`BACKFILL_WORKERS`.

Rows written per slot (typical production):

| Queue | Entries per snapshot | Notes |
|---|---|---|
| `pending_consolidations` | 0–10k in migration peaks, ~0 idle | Mass-migration operators stage batches of consolidations that drain gradually over days. |
| `pending_deposits` | 0–few thousand | Activation churn queue — turns over relatively quickly. |
| `pending_partial_withdrawals` | Low hundreds steady | Each entry sits in the queue for at least 256 epochs via `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`. |

## Fork gate

Loaders are gated by `config.ELECTRA_START_SLOT`. Slots below that value
never hit the beacon node — the endpoint returns HTTP 400 pre-Electra and
we don't want 400s flooding the error rate metrics during backfill of
historical ranges.

Deployment note: set `ELECTRA_START_SLOT` to the network's activation slot.
`0` disables the gate (useful for forward-only live scraping after Electra
is long past).

## Storage shape

### Raw tables (`raw_pending_*`)

One JSON payload per scraped slot. Same shape as `raw_validators` — a
`slot`, `payload` (string), `payload_hash` for dedup, `retrieved_at`. See
`migrations/003_raw_tables.sql`.

### Structured tables (`pending_*`)

One row per queue entry per snapshot. See `migrations/012_pending_queues.sql`.

- `pending_consolidations(slot, source_index, target_index)`
- `pending_deposits(slot, pubkey, withdrawal_credentials, amount, signature, deposit_slot)`
- `pending_partial_withdrawals(slot, validator_index, amount, withdrawable_epoch)`

## Enabling

```bash
# In .env
ENABLED_LOADERS=blocks,validators,specs,genesis,rewards,pending_consolidations,pending_deposits,pending_partial_withdrawals
ELECTRA_START_SLOT=21254400    # Gnosis mainnet Electra activation (example)
```

Run:

```bash
make migration   # creates raw + structured tables
make backfill    # loads historical range; skips pre-Electra slots silently
make transform   # runs parser to populate structured tables
```

## Downstream: dbt-cerebro consolidation model simplification

Once these queues are available, `int_consensus_validators_consolidations_daily`
can be replaced with a diff-based model:

```sql
-- Applied consolidations on day D = entries in D-1 snapshot but absent in D.
WITH yesterday AS (
  SELECT source_index, target_index
  FROM consensus.pending_consolidations FINAL
  WHERE slot = eod_slot(D - INTERVAL 1 DAY)
),
today AS (
  SELECT source_index, target_index
  FROM consensus.pending_consolidations FINAL
  WHERE slot = eod_slot(D)
)
SELECT source_index, target_index FROM yesterday
EXCEPT
SELECT source_index, target_index FROM today
```

Every heuristic in the current dbt-cerebro income model (dedup per
source_pubkey, self+cross split, target-active check, source↔target pairing,
spec-cap inference) becomes unnecessary — the beacon chain already did that
work and exposed the result in `pending_consolidations`.

## Testing

See `tests/test_pending_queues.py` for parser + loader coverage:
- Parser correctly unpacks each queue's payload shape.
- Parser handles empty queues and malformed JSON gracefully.
- Loader fork-gate correctly refuses pre-Electra slots.
- Loader pre-Electra `load_single` is a no-op (no API call, no write).

Run:

```bash
python -m pytest tests/test_pending_queues.py -q
```
