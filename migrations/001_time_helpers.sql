-- Time helpers table for materialized column calculations (single row)
CREATE TABLE IF NOT EXISTS time_helpers (
    genesis_time_unix UInt64,
    seconds_per_slot UInt64,
    slots_per_epoch UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY (genesis_time_unix, seconds_per_slot, slots_per_epoch)
SETTINGS index_granularity = 1;