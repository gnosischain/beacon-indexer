-- Remove indices in reverse order


-- Scraper state table indices
ALTER TABLE scraper_state DROP INDEX IF EXISTS scraper_state_mode;

-- Attester slashings table indices
ALTER TABLE attester_slashings DROP INDEX IF EXISTS attester_slashings_attestation_2_slot;
ALTER TABLE attester_slashings DROP INDEX IF EXISTS attester_slashings_attestation_1_slot;

-- Proposer slashings table indices
ALTER TABLE proposer_slashings DROP INDEX IF EXISTS proposer_slashings_proposer_index;

-- Voluntary exits table indices
ALTER TABLE voluntary_exits DROP INDEX IF EXISTS voluntary_exits_epoch;
ALTER TABLE voluntary_exits DROP INDEX IF EXISTS voluntary_exits_validator_index;

-- Sync committee rewards table indices
ALTER TABLE sync_committee_rewards DROP INDEX IF EXISTS sync_committee_rewards_reward;
ALTER TABLE sync_committee_rewards DROP INDEX IF EXISTS sync_committee_rewards_validator_index;

-- Attestation rewards table indices
ALTER TABLE attestation_rewards DROP INDEX IF EXISTS attestation_rewards_inactivity;
ALTER TABLE attestation_rewards DROP INDEX IF EXISTS attestation_rewards_validator_index;

-- Block rewards table indices
ALTER TABLE block_rewards DROP INDEX IF EXISTS block_rewards_total;
ALTER TABLE block_rewards DROP INDEX IF EXISTS block_rewards_proposer_index;

-- KZG commitments table indices
ALTER TABLE kzg_commitments DROP INDEX IF EXISTS kzg_commitments_commitment;

-- Blob sidecars table indices
ALTER TABLE blob_sidecars DROP INDEX IF EXISTS blob_sidecars_kzg_commitment;

-- Deposits table indices
ALTER TABLE deposits DROP INDEX IF EXISTS deposits_withdrawal_credentials;
ALTER TABLE deposits DROP INDEX IF EXISTS deposits_pubkey;

-- BLS to execution changes table indices
ALTER TABLE bls_to_execution_changes DROP INDEX IF EXISTS bls_changes_to_address;
ALTER TABLE bls_to_execution_changes DROP INDEX IF EXISTS bls_changes_validator_index;

-- Withdrawals table indices
ALTER TABLE withdrawals DROP INDEX IF EXISTS withdrawals_address;
ALTER TABLE withdrawals DROP INDEX IF EXISTS withdrawals_validator_index;

ALTER TABLE consolidations DROP INDEX IF EXISTS consolidations_source_address;
ALTER TABLE consolidations DROP INDEX IF EXISTS consolidations_target_pubkey;

-- Transactions table indices
ALTER TABLE transactions DROP INDEX IF EXISTS transactions_tx_index;

-- Execution payloads table indices
ALTER TABLE execution_payloads DROP INDEX IF EXISTS execution_payloads_timestamp;
ALTER TABLE execution_payloads DROP INDEX IF EXISTS execution_payloads_block_hash;
ALTER TABLE execution_payloads DROP INDEX IF EXISTS execution_payloads_block_number;

-- Validators table indices
ALTER TABLE validators DROP INDEX IF EXISTS validators_withdrawal_credentials;
ALTER TABLE validators DROP INDEX IF EXISTS validators_status;
ALTER TABLE validators DROP INDEX IF EXISTS validators_pubkey;

-- Sync aggregates table indices
ALTER TABLE sync_aggregates DROP INDEX IF EXISTS sync_aggregates_bits;

-- Sync committees table indices
ALTER TABLE sync_committees DROP INDEX IF EXISTS sync_committees_epoch;

-- Committees table indices
ALTER TABLE committees DROP INDEX IF EXISTS committees_committee_slot;
ALTER TABLE committees DROP INDEX IF EXISTS committees_epoch;

-- Attestations table indices
ALTER TABLE attestations DROP INDEX IF EXISTS attestations_beacon_block_root;
ALTER TABLE attestations DROP INDEX IF EXISTS attestations_target_epoch;
ALTER TABLE attestations DROP INDEX IF EXISTS attestations_source_epoch;
ALTER TABLE attestations DROP INDEX IF EXISTS attestations_attestation_slot;

-- Blocks table indices
ALTER TABLE blocks DROP INDEX IF EXISTS blocks_fork_version;
ALTER TABLE blocks DROP INDEX IF EXISTS blocks_timestamp;
ALTER TABLE blocks DROP INDEX IF EXISTS blocks_parent_root;
ALTER TABLE blocks DROP INDEX IF EXISTS blocks_proposer_index;
