-- Electra pending-state queues — structured tables populated by ElectraParser.
--
-- Source: BeaconState fields introduced in EIP-7251 (pending_consolidations) and
-- EIP-7002 (pending_deposits, pending_partial_withdrawals). These are the
-- POST-VALIDATION, PRE-APPLICATION queues. An entry existing here is guaranteed to
-- apply — the consensus spec's `process_consolidation_request` /
-- `process_deposit_request` / `process_withdrawal_request` functions drop invalid
-- requests (source==target, inactive source/target, already exiting, insufficient
-- churn, credential mismatch, …) BEFORE they enter the queue.
--
-- Cadence: scraped at the last slot of each UTC day by PendingConsolidationsLoader
-- and siblings. Downstream dbt models derive "what applied on day D" by diffing
-- day-D against day-(D-1) snapshots — entries in D-1 but absent in D = applied on D.
--
-- Separate from 010_fork_electra.sql so this migration can be applied/rolled back
-- independently of the broader Electra schema additions.

-- PendingConsolidation: one (source_index, target_index) pair waiting to apply.
CREATE TABLE IF NOT EXISTS pending_consolidations (
    slot UInt64,
    source_index UInt64,
    target_index UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, source_index, target_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- PendingDeposit: one queued deposit with its EXACT credit amount.
-- `deposit_slot` is the `slot` field of the PendingDeposit container itself —
-- preserved so the deposit entity can be tracked across snapshots as it
-- progresses through the activation churn queue.
CREATE TABLE IF NOT EXISTS pending_deposits (
    slot UInt64,
    pubkey String,
    withdrawal_credentials String,
    amount UInt64,
    signature String,
    deposit_slot UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, pubkey, deposit_slot)
PARTITION BY toStartOfMonth(slot_timestamp);

-- PendingPartialWithdrawal: one scheduled partial withdrawal.
-- `withdrawable_epoch` is when the withdrawal becomes eligible for processing.
CREATE TABLE IF NOT EXISTS pending_partial_withdrawals (
    slot UInt64,
    validator_index UInt64,
    amount UInt64,
    withdrawable_epoch UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, validator_index, withdrawable_epoch)
PARTITION BY toStartOfMonth(slot_timestamp);
