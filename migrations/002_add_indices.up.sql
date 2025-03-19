-- Add indices to improve query performance

-- Raw blocks table indices
ALTER TABLE raw_blocks ADD INDEX raw_blocks_version (version) TYPE minmax;
ALTER TABLE raw_blocks ADD INDEX raw_blocks_is_canonical (is_canonical) TYPE minmax;

-- Blocks table indices
ALTER TABLE blocks ADD INDEX blocks_proposer_index (proposer_index) TYPE minmax;
ALTER TABLE blocks ADD INDEX blocks_parent_root (parent_root) TYPE minmax;
ALTER TABLE blocks ADD INDEX blocks_timestamp (timestamp) TYPE minmax;
ALTER TABLE blocks ADD INDEX blocks_fork_version (fork_version) TYPE minmax;

-- Attestations table indices
ALTER TABLE attestations ADD INDEX attestations_attestation_slot (attestation_slot) TYPE minmax;
ALTER TABLE attestations ADD INDEX attestations_source_epoch (source_epoch) TYPE minmax;
ALTER TABLE attestations ADD INDEX attestations_target_epoch (target_epoch) TYPE minmax;
ALTER TABLE attestations ADD INDEX attestations_beacon_block_root (beacon_block_root) TYPE minmax;

-- Committees table indices
ALTER TABLE committees ADD INDEX committees_epoch (epoch) TYPE minmax;
ALTER TABLE committees ADD INDEX committees_committee_slot (committee_slot) TYPE minmax;

-- Sync committees table indices
ALTER TABLE sync_committees ADD INDEX sync_committees_epoch (epoch) TYPE minmax;

-- Sync aggregates table indices
ALTER TABLE sync_aggregates ADD INDEX sync_aggregates_bits (sync_committee_bits) TYPE minmax;

-- Validators table indices
ALTER TABLE validators ADD INDEX validators_pubkey (pubkey) TYPE minmax;
ALTER TABLE validators ADD INDEX validators_status (status) TYPE minmax;
ALTER TABLE validators ADD INDEX validators_withdrawal_credentials (withdrawal_credentials) TYPE minmax;


-- Execution payloads table indices
ALTER TABLE execution_payloads ADD INDEX execution_payloads_block_number (block_number) TYPE minmax;
ALTER TABLE execution_payloads ADD INDEX execution_payloads_block_hash (block_hash) TYPE minmax;
ALTER TABLE execution_payloads ADD INDEX execution_payloads_timestamp (timestamp) TYPE minmax;

-- Transactions table indices
ALTER TABLE transactions ADD INDEX transactions_tx_index (tx_index) TYPE minmax;

-- Withdrawals table indices
ALTER TABLE withdrawals ADD INDEX withdrawals_validator_index (validator_index) TYPE minmax;
ALTER TABLE withdrawals ADD INDEX withdrawals_address (address) TYPE minmax;

-- BLS to execution changes table indices
ALTER TABLE bls_to_execution_changes ADD INDEX bls_changes_validator_index (validator_index) TYPE minmax;
ALTER TABLE bls_to_execution_changes ADD INDEX bls_changes_to_address (to_execution_address) TYPE minmax;

-- Deposits table indices
ALTER TABLE deposits ADD INDEX deposits_pubkey (pubkey) TYPE minmax;
ALTER TABLE deposits ADD INDEX deposits_withdrawal_credentials (withdrawal_credentials) TYPE minmax;

-- Blob sidecars table indices
ALTER TABLE blob_sidecars ADD INDEX blob_sidecars_kzg_commitment (kzg_commitment) TYPE minmax;

-- KZG commitments table indices
ALTER TABLE kzg_commitments ADD INDEX kzg_commitments_commitment (commitment) TYPE minmax;

-- Block rewards table indices
ALTER TABLE block_rewards ADD INDEX block_rewards_proposer_index (proposer_index) TYPE minmax;
ALTER TABLE block_rewards ADD INDEX block_rewards_total (total) TYPE minmax;

-- Attestation rewards table indices
ALTER TABLE attestation_rewards ADD INDEX attestation_rewards_validator_index (validator_index) TYPE minmax;
ALTER TABLE attestation_rewards ADD INDEX attestation_rewards_inactivity (inactivity) TYPE minmax;

-- Sync committee rewards table indices
ALTER TABLE sync_committee_rewards ADD INDEX sync_committee_rewards_validator_index (validator_index) TYPE minmax;
ALTER TABLE sync_committee_rewards ADD INDEX sync_committee_rewards_reward (reward) TYPE minmax;

-- Voluntary exits table indices
ALTER TABLE voluntary_exits ADD INDEX voluntary_exits_validator_index (validator_index) TYPE minmax;
ALTER TABLE voluntary_exits ADD INDEX voluntary_exits_epoch (epoch) TYPE minmax;

-- Proposer slashings table indices
ALTER TABLE proposer_slashings ADD INDEX proposer_slashings_proposer_index (proposer_index) TYPE minmax;

-- Attester slashings table indices
ALTER TABLE attester_slashings ADD INDEX attester_slashings_attestation_1_slot (attestation_1_slot) TYPE minmax;
ALTER TABLE attester_slashings ADD INDEX attester_slashings_attestation_2_slot (attestation_2_slot) TYPE minmax;

-- Scraper state table indices
ALTER TABLE scraper_state ADD INDEX scraper_state_mode (mode) TYPE minmax;