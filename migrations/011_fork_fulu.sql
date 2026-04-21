-- Fulu fork schema additions (PeerDAS data column sidecars)

CREATE TABLE IF NOT EXISTS raw_data_column_sidecars (
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

CREATE TABLE IF NOT EXISTS data_column_sidecars (
    slot UInt64,
    column_index UInt64,
    column_cells UInt64 DEFAULT 0,
    column_bytes UInt64 DEFAULT 0,
    kzg_commitments_count UInt32 DEFAULT 0,
    kzg_proofs_count UInt32 DEFAULT 0,
    signed_block_slot UInt64 DEFAULT 0,
    proposer_index UInt64 DEFAULT 0,
    body_root String DEFAULT '',
    column_hash String DEFAULT '',
    commitments_hash String DEFAULT '',
    proofs_hash String DEFAULT '',
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, column_index)
PARTITION BY toStartOfMonth(slot_timestamp);
