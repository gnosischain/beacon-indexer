-- Enhanced indexing state table with operation mode support
CREATE TABLE IF NOT EXISTS indexing_state (
    -- Core fields
    mode String DEFAULT 'historical',
    dataset String DEFAULT '',
    start_slot UInt64,
    end_slot UInt64,
    status Enum('pending' = 1, 'processing' = 2, 'completed' = 3, 'failed' = 4, 'skipped' = 5, 'validating' = 6) DEFAULT 'pending',
    
    -- Tracking fields
    worker_id String DEFAULT '',
    batch_id String DEFAULT '',
    attempt_count UInt32 DEFAULT 0,
    rows_indexed UInt64 DEFAULT 0,
    error_message String DEFAULT '',
    
    -- Timestamps
    created_at DateTime DEFAULT now(),
    started_at Nullable(DateTime),
    completed_at Nullable(DateTime),
    updated_at DateTime DEFAULT now(),
    version DateTime DEFAULT now(),
    
    -- Indices
    INDEX idx_mode_dataset (mode, dataset) TYPE minmax GRANULARITY 1,
    INDEX idx_status (status) TYPE minmax GRANULARITY 1,
    INDEX idx_slots (start_slot, end_slot) TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(version)
ORDER BY (mode, dataset, start_slot, end_slot)
PARTITION BY toYYYYMM(created_at)
SETTINGS index_granularity = 8192;

-- Create sync position table for tracking continuous sync
CREATE TABLE IF NOT EXISTS sync_position (
    mode String DEFAULT 'continuous',
    dataset String DEFAULT '',
    last_synced_slot UInt64,
    updated_at DateTime DEFAULT now(),
    
    INDEX idx_mode_dataset (mode, dataset) TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (mode, dataset)
SETTINGS index_granularity = 8192;

-- Backwards compatibility view for existing code using scraper_id/table_name
CREATE VIEW IF NOT EXISTS indexing_state_compat AS
SELECT 
    mode,
    dataset,
    start_slot,
    end_slot,
    status,
    worker_id,
    batch_id,
    attempt_count,
    rows_indexed,
    error_message,
    created_at,
    started_at,
    completed_at,
    updated_at,
    version,
    -- Extract scraper_id and table_name from dataset for compatibility
    splitByChar('_', dataset)[1] AS scraper_id,
    arrayStringConcat(arraySlice(splitByChar('_', dataset), 2), '_') AS table_name
FROM indexing_state;