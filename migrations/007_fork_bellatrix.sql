-- Bellatrix fork schema additions (The Merge - Epoch 144896 on mainnet)

-- Execution payloads table matching ERA parser structure
CREATE TABLE IF NOT EXISTS execution_payloads (
    slot UInt64,
    parent_hash String DEFAULT '',
    fee_recipient String DEFAULT '',
    state_root String DEFAULT '',
    receipts_root String DEFAULT '',
    logs_bloom String DEFAULT '',
    prev_randao String DEFAULT '',
    block_number UInt64 DEFAULT 0,
    gas_limit UInt64 DEFAULT 0,
    gas_used UInt64 DEFAULT 0,
    timestamp UInt64 DEFAULT 0,
    base_fee_per_gas String DEFAULT '',
    block_hash String DEFAULT '',
    extra_data String DEFAULT '',
    transactions_count UInt64 DEFAULT 0,
    blob_gas_used UInt64 DEFAULT 0,      -- Deneb+
    excess_blob_gas UInt64 DEFAULT 0,    -- Deneb+
    withdrawals_count UInt64 DEFAULT 0,  -- Capella+
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, block_number)
PARTITION BY toStartOfMonth(slot_timestamp);

-- Transactions table matching ERA parser structure
CREATE TABLE IF NOT EXISTS transactions (
    slot UInt64,
    block_number UInt64 DEFAULT 0,
    block_hash String DEFAULT '',
    transaction_index UInt64,
    transaction_hash String,
    fee_recipient String DEFAULT '',
    gas_limit UInt64 DEFAULT 0,
    gas_used UInt64 DEFAULT 0,
    base_fee_per_gas String DEFAULT '',
    from_address String DEFAULT '',
    to_address String DEFAULT '',
    value String DEFAULT '',
    gas_price UInt64 DEFAULT 0,
    nonce UInt64 DEFAULT 0,
    input String DEFAULT '',
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, transaction_index, transaction_hash)
PARTITION BY toStartOfMonth(slot_timestamp);
