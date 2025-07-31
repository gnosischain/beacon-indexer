-- Blocks table
CREATE TABLE IF NOT EXISTS blocks (
    slot UInt64,
    proposer_index UInt32,
    parent_root String,
    state_root String,
    body_root String,
    signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot
PARTITION BY toStartOfMonth(slot_timestamp);

-- Attestations table
CREATE TABLE IF NOT EXISTS attestations (
    slot UInt64,
    committee_index UInt32,
    beacon_block_root String,
    source_epoch UInt64,
    source_root String,
    target_epoch UInt64,
    target_root String,
    aggregation_bits String,
    signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, committee_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Validators table
CREATE TABLE IF NOT EXISTS validators (
    slot UInt64,
    validator_index UInt32,
    balance UInt64,
    status String,
    pubkey String,
    withdrawal_credentials String,
    effective_balance UInt64,
    slashed UInt8,
    activation_eligibility_epoch UInt64,
    activation_epoch UInt64,
    exit_epoch UInt64,
    withdrawable_epoch UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, validator_index)
PARTITION BY toStartOfMonth(slot_timestamp);