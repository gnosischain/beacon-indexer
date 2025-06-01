-- Genesis table to store beacon chain genesis information
CREATE TABLE IF NOT EXISTS genesis (
    genesis_time DateTime64(0, 'UTC'),
    genesis_validators_root String,
    genesis_fork_version String,
    created_at DateTime64(0, 'UTC') DEFAULT now('UTC'),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (genesis_time);

-- Specs table to store chain configuration parameters
CREATE TABLE IF NOT EXISTS specs (
    parameter_name String,
    parameter_value String,
    updated_at DateTime64(0, 'UTC') DEFAULT now('UTC'),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),
    PRIMARY KEY (parameter_name, updated_at)
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (parameter_name, updated_at);

-- Instead of functions, we'll use expressions directly in table definitions
-- Let's create a helpers table for frequently used values
CREATE TABLE IF NOT EXISTS time_helpers (
    genesis_time_unix UInt64,
    seconds_per_slot UInt64,
    slots_per_epoch UInt64,
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY tuple();

-- Insert initial placeholder values (will be updated after genesis and specs are populated)
INSERT INTO time_helpers (genesis_time_unix, seconds_per_slot, slots_per_epoch) VALUES (0, 12, 32);

-- Beacon blocks table
CREATE TABLE IF NOT EXISTS blocks (
    slot UInt64,
    block_root String,
    parent_root String,
    state_root String,
    proposer_index UInt64,
    eth1_block_hash String,
    eth1_deposit_root String,
    eth1_deposit_count UInt64,
    graffiti String,
    signature String,
    randao_reveal String,
    timestamp DateTime64(0, 'UTC'),
    is_canonical UInt8,
    fork_version String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, block_root);

-- Raw blocks table for storing complete block data
CREATE TABLE IF NOT EXISTS raw_blocks (
    slot UInt64,
    block_root String,
    version String,
    block_data String, -- JSON representation of the full block data
    is_canonical UInt8 DEFAULT 1,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, block_root);

-- Attestations table
CREATE TABLE IF NOT EXISTS attestations (
    slot UInt64,
    block_root String,
    attestation_slot UInt64,
    attestation_index UInt64,
    aggregation_bits String,
    committee_bits String, -- Added for Electra+
    beacon_block_root String,
    source_epoch UInt64,
    source_root String,
    target_epoch UInt64,
    target_root String,
    signature String,
    validators Array(UInt64),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, attestation_index);

-- Committees table
CREATE TABLE IF NOT EXISTS committees (
    slot UInt64,
    epoch UInt64,
    committee_slot UInt64,
    committee_index UInt64,
    validators Array(UInt64),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, committee_index);

-- Sync committees table
CREATE TABLE IF NOT EXISTS sync_committees (
    slot UInt64,
    epoch UInt64,
    validators Array(UInt64),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, epoch);

-- Sync aggregates table (Altair+)
CREATE TABLE IF NOT EXISTS sync_aggregates (
    slot UInt64,
    block_root String,
    sync_committee_bits String,
    sync_committee_signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, block_root);

-- Validators table
CREATE TABLE IF NOT EXISTS validators (
    slot UInt64,
    validator_index UInt64,
    pubkey String,
    raw_pubkey String, -- Added for direct API mapping
    withdrawal_credentials String,
    raw_withdrawal_credentials String, -- Added for direct API mapping
    effective_balance UInt64,
    slashed UInt8,
    activation_eligibility_epoch UInt64,
    activation_epoch UInt64,
    exit_epoch UInt64,
    withdrawable_epoch UInt64,
    status String,
    balance UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, validator_index);

-- Execution payloads table
CREATE TABLE IF NOT EXISTS execution_payloads (
    slot UInt64,
    block_root String,
    parent_hash String,
    fee_recipient String,
    state_root String,
    receipts_root String,
    logs_bloom String,
    prev_randao String,
    block_number UInt64,
    gas_limit UInt64,
    gas_used UInt64,
    timestamp DateTime64(0, 'UTC'),
    extra_data String,
    base_fee_per_gas UInt64,
    block_hash String,
    excess_blob_gas Nullable(UInt64),
    blob_gas_used Nullable(UInt64), -- Added for Deneb+
    transactions_count UInt64,
    withdrawals_count UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, block_root);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    slot UInt64,
    block_root String,
    tx_index UInt64,
    transaction_data String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, tx_index);

-- Withdrawals table
CREATE TABLE IF NOT EXISTS withdrawals (
    slot UInt64,
    block_root String,
    withdrawal_index UInt64,
    validator_index UInt64,
    address String,
    amount UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, withdrawal_index);

-- BLS to execution changes table (Capella+)
CREATE TABLE IF NOT EXISTS bls_to_execution_changes (
    slot UInt64,
    block_root String,
    change_index UInt64,
    validator_index UInt64,
    from_bls_pubkey String,
    to_execution_address String,
    signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, change_index);

-- Deposits table
CREATE TABLE IF NOT EXISTS deposits (
    slot UInt64,
    block_root String,
    deposit_index UInt64,
    pubkey String,
    withdrawal_credentials String,
    amount UInt64,
    signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, deposit_index);

-- Blob sidecars table
CREATE TABLE IF NOT EXISTS blob_sidecars (
    slot UInt64,
    block_root String,
    blob_index UInt64,
    kzg_commitment String,
    kzg_proof String,
    blob_data String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, blob_index);

-- KZG commitments table (Deneb+)
CREATE TABLE IF NOT EXISTS kzg_commitments (
    slot UInt64,
    block_root String,
    commitment_index UInt64,
    commitment String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, commitment_index);

-- Block rewards table
CREATE TABLE IF NOT EXISTS block_rewards (
    slot UInt64,
    block_root String,
    proposer_index UInt64,
    total UInt64,
    attestations UInt64,
    sync_aggregate UInt64,
    proposer_slashings UInt64,
    attester_slashings UInt64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, proposer_index);

-- Attestation rewards table
CREATE TABLE IF NOT EXISTS attestation_rewards (
    epoch UInt64,
    validator_index UInt64,
    head Int64,
    target Int64,
    source Int64,
    inclusion_delay Nullable(UInt64),
    inactivity Int64,
    epoch_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        epoch * (SELECT slots_per_epoch FROM time_helpers LIMIT 1) * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(epoch_timestamp)
ORDER BY (epoch, validator_index);

-- Sync committee rewards table
CREATE TABLE IF NOT EXISTS sync_committee_rewards (
    slot UInt64,
    block_root String,
    validator_index UInt64,
    reward Int64,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, validator_index);

-- Voluntary exits table
CREATE TABLE IF NOT EXISTS voluntary_exits (
    slot UInt64,
    block_root String,
    validator_index UInt64,
    epoch UInt64,
    signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, validator_index);

-- Proposer slashings table
CREATE TABLE IF NOT EXISTS proposer_slashings (
    slot UInt64,
    block_root String,
    proposer_index UInt64,
    header_1_slot UInt64,
    header_1_proposer UInt64,
    header_1_root String,
    header_1_signature String,
    header_2_slot UInt64,
    header_2_proposer UInt64,
    header_2_root String,
    header_2_signature String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, proposer_index);

-- Attester slashings table
CREATE TABLE IF NOT EXISTS attester_slashings (
    slot UInt64,
    block_root String,
    slashing_index UInt64,
    attestation_1_indices Array(UInt64),
    attestation_1_slot UInt64,
    attestation_1_index UInt64,
    attestation_1_root String,
    attestation_1_sig String,
    attestation_2_indices Array(UInt64),
    attestation_2_slot UInt64,
    attestation_2_index UInt64,
    attestation_2_root String,
    attestation_2_sig String,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time, 'UTC') FROM genesis LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(slot_timestamp)
ORDER BY (slot, slashing_index);

-- Scraper state table to track progress
CREATE TABLE IF NOT EXISTS scraper_state (
    scraper_id String,
    last_processed_slot UInt64,
    mode String,
    updated_at DateTime64(0, 'UTC') DEFAULT now(),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (scraper_id, updated_at);