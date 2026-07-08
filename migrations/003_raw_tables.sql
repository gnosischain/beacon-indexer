-- Raw beacon blocks
CREATE TABLE IF NOT EXISTS raw_blocks (
    slot UInt64,
    block_root String,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree()
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Raw validators data
CREATE TABLE IF NOT EXISTS raw_validators (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)  
ORDER BY (slot, payload_hash) 
PARTITION BY toStartOfMonth(slot_timestamp);

-- Raw specs data
CREATE TABLE IF NOT EXISTS raw_specs (
    payload JSON,
    retrieved_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY retrieved_at;

-- Raw genesis data
CREATE TABLE IF NOT EXISTS raw_genesis (
    payload JSON,
    retrieved_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY retrieved_at;


-- Raw rewards data
CREATE TABLE IF NOT EXISTS raw_rewards (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Raw Electra pending_consolidations snapshots. One JSON payload per scraped slot.
-- Same shape as raw_validators — a JSON `data` array of {source_index, target_index}
-- at the given beacon state. Scraped at the last slot of each UTC day by default
-- (VALIDATOR_MODE=daily). Fork-gated: skipped pre-Electra by PendingConsolidationsLoader.
CREATE TABLE IF NOT EXISTS raw_pending_consolidations (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Raw Electra pending_deposits snapshots. JSON `data` array of PendingDeposit entries.
-- Each entry is the EXACT credit amount the beacon chain will apply to the validator
-- — not the reported request amount. Using this removes the post-Pectra "1767 GNO
-- request landed as 0.01 GNO today" accounting ambiguity.
CREATE TABLE IF NOT EXISTS raw_pending_deposits (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Raw Electra pending_partial_withdrawals snapshots. JSON `data` array of
-- PendingPartialWithdrawal entries. Needed to distinguish EIP-7002 scheduled
-- partial withdrawals from full exits and consolidation-driven sweeps.
CREATE TABLE IF NOT EXISTS raw_pending_partial_withdrawals (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);