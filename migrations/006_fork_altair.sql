-- Altair fork schema additions (Epoch 74240 on mainnet)

-- Add sync committee support to blocks table
ALTER TABLE blocks 
ADD COLUMN IF NOT EXISTS sync_aggregate_participation UInt64 DEFAULT 0,
ADD COLUMN IF NOT EXISTS sync_aggregate_signature String DEFAULT '';

-- Sync aggregates table
CREATE TABLE IF NOT EXISTS sync_aggregates (
    slot UInt64,
    sync_committee_bits String DEFAULT '',
    sync_committee_signature String DEFAULT '',
    participation_count UInt32 DEFAULT 0,
    participating_validators UInt32 DEFAULT 0,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot
PARTITION BY toStartOfMonth(slot_timestamp);

-- Sync committees table
CREATE TABLE IF NOT EXISTS sync_committees (
    period UInt64,
    pubkeys Array(String),
    aggregate_pubkey String,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY period
PARTITION BY intDiv(period, 100);

-- Update schema version
INSERT INTO sync_progress (process_name, last_processed_slot) 
VALUES ('schema_version', 2);