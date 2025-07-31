-- Chunk management for backfill workers
CREATE TABLE load_state_chunks (
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

-- Sync progress tracking
CREATE TABLE IF NOT EXISTS sync_progress (
    process_name String,
    last_processed_slot UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY process_name;