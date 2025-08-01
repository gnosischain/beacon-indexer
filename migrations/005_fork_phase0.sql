-- Phase 0 (Genesis) fork tables 

-- Blocks table with all Phase 0 fields
CREATE TABLE IF NOT EXISTS blocks (
    slot UInt64,
    proposer_index UInt64,
    parent_root String DEFAULT '',
    state_root String DEFAULT '',
    signature String DEFAULT '',
    version String DEFAULT '',
    randao_reveal String DEFAULT '',
    graffiti String DEFAULT '',
    eth1_deposit_root String DEFAULT '',
    eth1_deposit_count UInt64 DEFAULT 0,
    eth1_block_hash String DEFAULT '',
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot
PARTITION BY toStartOfMonth(slot_timestamp);

-- Attestations table with all fields
CREATE TABLE IF NOT EXISTS attestations (
    slot UInt64,
    attestation_index UInt64,
    aggregation_bits String DEFAULT '',
    signature String DEFAULT '',
    attestation_slot UInt64 DEFAULT 0,
    committee_index UInt64 DEFAULT 0,
    beacon_block_root String DEFAULT '',
    source_epoch UInt64 DEFAULT 0,
    source_root String DEFAULT '',
    target_epoch UInt64 DEFAULT 0,
    target_root String DEFAULT '',
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, attestation_index, committee_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Deposits table (beacon block deposits)
CREATE TABLE IF NOT EXISTS deposits (
    slot UInt64,
    deposit_index UInt64,
    pubkey String DEFAULT '',
    withdrawal_credentials String DEFAULT '',
    amount UInt64 DEFAULT 0,
    signature String DEFAULT '',
    proof Array(String) DEFAULT [],
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, deposit_index, pubkey)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Voluntary exits table
CREATE TABLE IF NOT EXISTS voluntary_exits (
    slot UInt64,
    exit_index UInt64,
    signature String DEFAULT '',
    epoch UInt64 DEFAULT 0,
    validator_index UInt64 DEFAULT 0,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, validator_index, epoch)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Proposer slashings table
CREATE TABLE IF NOT EXISTS proposer_slashings (
    slot UInt64,
    slashing_index UInt64,
    header_1_slot UInt64 DEFAULT 0,
    header_1_proposer_index UInt64 DEFAULT 0,
    header_1_parent_root String DEFAULT '',
    header_1_state_root String DEFAULT '',
    header_1_body_root String DEFAULT '',
    header_1_signature String DEFAULT '',
    header_2_slot UInt64 DEFAULT 0,
    header_2_proposer_index UInt64 DEFAULT 0,
    header_2_parent_root String DEFAULT '',
    header_2_state_root String DEFAULT '',
    header_2_body_root String DEFAULT '',
    header_2_signature String DEFAULT '',
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, slashing_index, header_1_proposer_index)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Attester slashings table
CREATE TABLE IF NOT EXISTS attester_slashings (
    slot UInt64,
    slashing_index UInt64,
    att_1_slot UInt64 DEFAULT 0,
    att_1_committee_index UInt64 DEFAULT 0,
    att_1_beacon_block_root String DEFAULT '',
    att_1_source_epoch UInt64 DEFAULT 0,
    att_1_source_root String DEFAULT '',
    att_1_target_epoch UInt64 DEFAULT 0,
    att_1_target_root String DEFAULT '',
    att_1_signature String DEFAULT '',
    att_1_attesting_indices Array(UInt64) DEFAULT [],
    att_1_validator_count UInt32 DEFAULT 0,
    att_2_slot UInt64 DEFAULT 0,
    att_2_committee_index UInt64 DEFAULT 0,
    att_2_beacon_block_root String DEFAULT '',
    att_2_source_epoch UInt64 DEFAULT 0,
    att_2_source_root String DEFAULT '',
    att_2_target_epoch UInt64 DEFAULT 0,
    att_2_target_root String DEFAULT '',
    att_2_signature String DEFAULT '',
    att_2_attesting_indices Array(UInt64) DEFAULT [],
    att_2_validator_count UInt32 DEFAULT 0,
    total_slashed_validators UInt32 DEFAULT 0,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, slashing_index, att_1_committee_index)
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


CREATE TABLE IF NOT EXISTS rewards (
    slot UInt64,
    proposer_index UInt64,
    total UInt64,
    attestations UInt64,
    sync_aggregate UInt64,
    proposer_slashings UInt64,
    attester_slashings UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot)
PARTITION BY toStartOfMonth(slot_timestamp);