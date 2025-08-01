-- Chunk management for backfill workers
CREATE TABLE IF NOT EXISTS load_state_chunks (
    chunk_id String,
    start_slot UInt64,
    end_slot UInt64,
    loader_name String,
    status Enum8('pending' = 1, 'claimed' = 2, 'completed' = 3, 'failed' = 4),
    worker_id String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chunk_id)
PRIMARY KEY (chunk_id);

-- Enhanced transformer progress tracking with ranges  
CREATE TABLE IF NOT EXISTS transformer_progress (
    raw_table_name String,
    start_slot UInt64,
    end_slot UInt64,
    status Enum8('processing' = 1, 'completed' = 2, 'failed' = 3),
    processed_count UInt64 DEFAULT 0,
    failed_count UInt64 DEFAULT 0,
    error_message String DEFAULT '',
    processed_at DateTime DEFAULT now(),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (raw_table_name, start_slot)
PARTITION BY raw_table_name;