-- Deneb fork schema additions (Cancun - Epoch 269568 on mainnet)

-- Add blob fields to blocks table
ALTER TABLE blocks 
ADD COLUMN IF NOT EXISTS blob_kzg_commitments_count UInt32 DEFAULT 0,
ADD COLUMN IF NOT EXISTS blob_kzg_commitments_root String DEFAULT '';

-- Blob commitments table matching ERA parser structure
CREATE TABLE IF NOT EXISTS blob_commitments (
    slot UInt64,
    commitment_index UInt64,
    commitment String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, commitment_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Blob sidecars table (for actual blob data when available)
CREATE TABLE IF NOT EXISTS blob_sidecars (
    slot UInt64,
    blob_index UInt64,
    kzg_commitment String DEFAULT '',
    kzg_proof String DEFAULT '',
    blob_size UInt64 DEFAULT 0,
    blob_hash String DEFAULT '',
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, blob_index)
PARTITION BY toStartOfMonth(slot_timestamp);
