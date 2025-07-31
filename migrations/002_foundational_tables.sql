-- Genesis table (needed for timestamp calculations)
CREATE TABLE IF NOT EXISTS genesis (
    genesis_time DateTime64(0, 'UTC'),
    genesis_validators_root String,
    genesis_fork_version String
) ENGINE = ReplacingMergeTree()
ORDER BY genesis_time;

-- Specs table (needed for slot timing)
CREATE TABLE IF NOT EXISTS specs (
    parameter_name String,
    parameter_value String,
    updated_at DateTime64(0, 'UTC') DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY parameter_name;