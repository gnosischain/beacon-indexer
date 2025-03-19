-- Remove indices in reverse order

-- Scraper state table indices
ALTER TABLE scraper_state DROP INDEX scraper_state_mode;

-- Attester slashings table indices
ALTER TABLE attester_slashings DROP INDEX attester_slashings_attestation_2_slot;
ALTER TABLE attester_slashings DROP INDEX attester_slashings_attestation_1_slot;

-- Proposer slashings table indices
ALTER TABLE proposer_slashings DROP INDEX proposer_slashings_proposer_index;

-- Voluntary exits table indices
ALTER TABLE voluntary_exits DROP INDEX voluntary_exits_epoch;
ALTER TABLE voluntary_exits DROP INDEX voluntary_exits_validator_index;

-- Sync committee rewards table indices
ALTER TABLE sync_committee_rewards DROP INDEX sync_committee_rewards_reward;
ALTER TABLE sync_committee_rewards DROP INDEX sync_committee_rewards_validator_index;

-- Attestation rewards table indices
ALTER TABLE attestation_rewards DROP INDEX attestation_rewards_inactivity;
ALTER TABLE attestation_rewards DROP INDEX attestation_rewards_validator_index;

-- Block rewards table indices
ALTER TABLE block_rewards DROP INDEX block_rewards_total;
ALTER TABLE block_rewards DROP INDEX block_rewards_proposer_index;

-- KZG commitments table indices
ALTER TABLE kzg_commitments DROP INDEX kzg_commitments_commitment;

-- Blob sidecars table indices
ALTER TABLE blob_sidecars DROP INDEX blob_sidecars_kzg_commitment;

-- Deposits table indices
ALTER TABLE deposits DROP INDEX deposits_withdrawal_credentials;
ALTER TABLE deposits DROP INDEX deposits_pubkey;

-- BLS to execution changes table indices
ALTER TABLE bls_to_execution_changes DROP INDEX bls_changes_to_address;
ALTER TABLE bls_to_execution_changes DROP INDEX bls_changes_validator_index;

-- Withdrawals table indices
ALTER TABLE withdrawals DROP INDEX withdrawals_address;
ALTER TABLE withdrawals DROP INDEX withdrawals_validator_index;

-- Transactions table indices
ALTER TABLE transactions DROP INDEX transactions_tx_index;

-- Execution payloads table indices
ALTER TABLE execution_payloads DROP INDEX execution_payloads_timestamp;
ALTER TABLE execution_payloads DROP INDEX execution_payloads_block_hash;
ALTER TABLE execution_payloads DROP INDEX execution_payloads_block_number;


-- Validators table indices
ALTER TABLE validators DROP INDEX validators_withdrawal_credentials;
ALTER TABLE validators DROP INDEX validators_status;
ALTER TABLE validators DROP INDEX validators_pubkey;

-- Sync aggregates table indices
ALTER TABLE sync_aggregates DROP INDEX sync_aggregates_bits;

-- Sync committees table indices
ALTER TABLE sync_committees DROP INDEX sync_committees_epoch;

-- Committees table indices
ALTER TABLE committees DROP INDEX committees_committee_slot;
ALTER TABLE committees DROP INDEX committees_epoch;

-- Attestations table indices
ALTER TABLE attestations DROP INDEX attestations_beacon_block_root;
ALTER TABLE attestations DROP INDEX attestations_target_epoch;
ALTER TABLE attestations DROP INDEX attestations_source_epoch;
ALTER TABLE attestations DROP INDEX attestations_attestation_slot;

-- Blocks table indices
ALTER TABLE blocks DROP INDEX blocks_fork_version;
ALTER TABLE blocks DROP INDEX blocks_timestamp;
ALTER TABLE blocks DROP INDEX blocks_parent_root;
ALTER TABLE blocks DROP INDEX blocks_proposer_index;

-- Raw blocks table indices
ALTER TABLE raw_blocks DROP INDEX raw_blocks_is_canonical;
ALTER TABLE raw_blocks DROP INDEX raw_blocks_version;