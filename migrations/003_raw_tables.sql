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