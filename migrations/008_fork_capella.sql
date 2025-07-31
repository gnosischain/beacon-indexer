-- Capella fork schema additions (Shanghai - Epoch 194048 on mainnet)

-- Add withdrawal fields to blocks table
ALTER TABLE blocks 
ADD COLUMN IF NOT EXISTS withdrawals_root String DEFAULT '',
ADD COLUMN IF NOT EXISTS withdrawals_count UInt32 DEFAULT 0;

-- Withdrawals table matching ERA parser structure
CREATE TABLE IF NOT EXISTS withdrawals (
    slot UInt64,
    block_number UInt64 DEFAULT 0,
    block_hash String DEFAULT '',
    withdrawal_index UInt64,
    validator_index UInt64,
    address String,
    amount UInt64,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, withdrawal_index, validator_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- BLS changes table matching ERA parser structure
CREATE TABLE IF NOT EXISTS bls_changes (
    slot UInt64,
    change_index UInt64,
    signature String DEFAULT '',
    validator_index UInt64 DEFAULT 0,
    from_bls_pubkey String DEFAULT '',
    to_execution_address String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, change_index, validator_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Update schema version
INSERT INTO sync_progress (process_name, last_processed_slot) 
VALUES ('schema_version', 4);