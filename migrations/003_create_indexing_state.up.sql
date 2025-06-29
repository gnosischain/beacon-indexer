CREATE TABLE IF NOT EXISTS indexing_state (
    scraper_id String,
    table_name String,
    start_slot UInt64,
    end_slot UInt64,
    status String,  -- pending, processing, completed, failed
    worker_id String DEFAULT '',
    batch_id String DEFAULT '',
    attempt_count UInt32 DEFAULT 0,
    rows_indexed UInt64 DEFAULT 0,
    error_message String DEFAULT '',
    started_at DateTime DEFAULT now(),
    completed_at Nullable(DateTime),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (scraper_id, table_name, start_slot, end_slot)
SETTINGS index_granularity = 8192;

-- Add indices for performance
ALTER TABLE indexing_state ADD INDEX idx_status (status) TYPE minmax;
ALTER TABLE indexing_state ADD INDEX idx_worker (worker_id) TYPE minmax;

-- Create a view to get the latest state (handles deduplication)
CREATE VIEW IF NOT EXISTS indexing_progress AS
SELECT 
    scraper_id,
    table_name,
    countIf(status = 'completed') as completed,
    countIf(status = 'processing') as processing,
    countIf(status = 'failed') as failed,
    countIf(status = 'pending') as pending,
    max(end_slot) as max_slot,
    sum(rows_indexed) as total_rows
FROM (
    SELECT 
        scraper_id,
        table_name,
        start_slot,
        end_slot,
        argMax(status, updated_at) as status,
        argMax(rows_indexed, updated_at) as rows_indexed
    FROM indexing_state
    GROUP BY scraper_id, table_name, start_slot, end_slot
)
GROUP BY scraper_id, table_name
ORDER BY scraper_id, table_name;

-- Table to track sync position for realtime mode
CREATE TABLE IF NOT EXISTS sync_position (
    scraper_id String,
    table_name String,
    last_synced_slot UInt64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (scraper_id, table_name)
SETTINGS index_granularity = 8192;

-- Add index for faster lookups
ALTER TABLE sync_position ADD INDEX sync_position_updated (updated_at) TYPE minmax;