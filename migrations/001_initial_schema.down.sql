-- Drop all tables in reverse order of their dependencies

-- Drop scraper state table
DROP TABLE IF EXISTS scraper_state;

-- Drop slashing tables
DROP TABLE IF EXISTS attester_slashings;
DROP TABLE IF EXISTS proposer_slashings;

-- Drop exit tables
DROP TABLE IF EXISTS voluntary_exits;

-- Drop reward tables
DROP TABLE IF EXISTS sync_committee_rewards;
DROP TABLE IF EXISTS attestation_rewards;
DROP TABLE IF EXISTS block_rewards;

-- Drop KZG commitments
DROP TABLE IF EXISTS kzg_commitments;

-- Drop blob sidecars table
DROP TABLE IF EXISTS blob_sidecars;

-- Drop BLS to execution changes
DROP TABLE IF EXISTS bls_to_execution_changes;

-- Drop deposit table
DROP TABLE IF EXISTS deposits;

-- Drop execution layer tables
DROP TABLE IF EXISTS withdrawals;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS execution_payloads;


-- Drop validator table
DROP TABLE IF EXISTS validators;

-- Drop committee tables
DROP TABLE IF EXISTS sync_aggregates;
DROP TABLE IF EXISTS sync_committees;
DROP TABLE IF EXISTS committees;

-- Drop attestation table
DROP TABLE IF EXISTS attestations;

-- Drop block tables
DROP TABLE IF EXISTS blocks;

DROP TABLE IF EXISTS consolidations;

-- Drop time helpers table
DROP TABLE IF EXISTS time_helpers;

-- Drop specs table
DROP TABLE IF EXISTS specs;

-- Drop genesis table
DROP TABLE IF EXISTS genesis;