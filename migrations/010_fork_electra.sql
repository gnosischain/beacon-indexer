-- Electra fork schema additions (Epoch 364032 on mainnet)
-- Adds execution requests support

-- Add execution requests fields to blocks table
ALTER TABLE blocks 
ADD COLUMN IF NOT EXISTS execution_requests_count UInt32 DEFAULT 0;

-- Execution requests table with JSON payload (as requested)
CREATE TABLE IF NOT EXISTS execution_requests (
    slot UInt64,
    payload String DEFAULT '',
    deposits_count UInt32 DEFAULT 0,
    withdrawals_count UInt32 DEFAULT 0,
    consolidations_count UInt32 DEFAULT 0,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot
PARTITION BY toStartOfMonth(slot_timestamp);
