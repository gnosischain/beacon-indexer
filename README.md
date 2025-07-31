# Beacon Chain Indexer

![Beacon Indexer](img/header-beacon_indexer.png)

A simple, minimalistic beacon chain indexer using the ELT (Extract, Load, Transform) pattern with **fork-aware parsing**. This indexer loads raw data from the beacon API first, then transforms it into structured tables with automatic fork detection and appropriate parsing for each Ethereum consensus layer upgrade.

## Features

- **Fork-Aware Architecture**: Automatic detection and parsing of all Ethereum consensus forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- **ELT Architecture**: Raw data loading and transformation are completely separated
- **Modular Design**: Easy to add new loaders, parsers, and support for future forks
- **Multiple Processing Modes**: Realtime sync, backfill with multiple workers
- **Resilient**: Raw data is never lost, transformations can be rerun with different fork parsers
- **Network Agnostic**: Supports mainnet, Gnosis Chain, Holesky, Sepolia with configurable fork schedules
- **Simple Configuration**: Minimal environment variables with external fork configuration

## Architecture

### Load Stage
- **LoaderService**: Fetches raw JSON data from beacon API
- **Loaders**: Modular components for different endpoints (blocks, validators, specs, genesis)
- **Raw Tables**: Store unprocessed JSON data (fork-agnostic)

### Transform Stage (Fork-Aware)
- **ForkDetectionService**: Automatically detects active fork based on slot/epoch
- **ParserFactory**: Creates appropriate parser for each fork
- **Fork-Specific Parsers**: Extract structured data according to fork specifications
- **Structured Tables**: Final destination for processed data with fork-specific fields

### Supported Forks

| Fork | Mainnet Epoch | Gnosis Epoch | Key Features | New Tables |
|------|---------------|---------------|--------------|------------|
| **Phase 0** | 0 | 0 | Basic blocks, attestations | `blocks`, `attestations` |
| **Altair** | 74240 | 512 | Sync committees | `sync_aggregates`, `sync_committees` |
| **Bellatrix** | 144896 | 385536 | Execution payloads (The Merge) | `execution_payloads`, `execution_transactions` |
| **Capella** | 194048 | 648704 | Withdrawals (Shanghai) | `withdrawals`, `bls_to_execution_changes` |
| **Deneb** | 269568 | 889856 | Blob transactions (Cancun) | `blob_sidecars`, `kzg_commitments` |
| **Electra** | 364032 | 1337856 | Execution requests | `deposit_requests`, `withdrawal_requests`, `consolidation_requests` |

## Quick Start

### 1. Setup Environment

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 2. Run Database Migration

```bash
make migration
# or locally: make dev-migration
```

### 3. Load Historical Data (Backfill)

```bash
make backfill
# or locally: make dev-backfill
```

### 4. Start Realtime Data Loading

```bash
make realtime
# or locally: make dev-realtime
```

### 5. Start Fork-Aware Data Transformation

```bash
make transform
# or locally: make dev-transform
```

## The 4 Essential Modes

### 1. Migration
**Purpose**: Set up database schema and tables for all forks
```bash
# Docker
make migration

# Local
make dev-migration
python scripts/migrate.py
```

### 2. Backfill
**Purpose**: Load historical raw data from beacon chain
```bash
# Docker (uses START_SLOT and END_SLOT from .env)
make backfill

# Local with custom range
python -m src.main load backfill --start-slot 0 --end-slot 1000000
```

### 3. Realtime
**Purpose**: Load new raw data continuously as blocks are produced
```bash
# Docker
make realtime

# Local
make dev-realtime
python -m src.main load realtime
```

### 4. Transform
**Purpose**: Process raw data into structured tables with fork-aware parsing
```bash
# Docker
make transform

# Local
make dev-transform
python -m src.main transform run
```

**What Fork-Aware Transform Does**:
- Automatically detects the fork for each slot based on network configuration
- Uses appropriate parser (Phase0Parser, AltairParser, BellatrixParser, etc.)
- Extracts fork-specific fields (sync aggregates, execution payloads, blobs, etc.)
- Inserts data into correct tables with proper schema versioning
- Handles fork transitions seamlessly within batches
- Can reprocess historical data: `python -m src.main transform reprocess --start-slot 0 --end-slot 1000`

## Fork Management

### Fork Information Commands

```bash
# List all configured forks for current network
python -m src.main fork list

# Get fork information for specific slot
python -m src.main fork info --slot 5000000

# Get fork information for specific epoch
python -m src.main fork info --epoch 150000
```

### Network Configuration

The indexer supports multiple networks with different fork schedules and timing parameters:

```bash
# Mainnet (default)
export NETWORK_NAME=mainnet

# Gnosis Chain
export NETWORK_NAME=gnosis

# Holesky testnet  
export NETWORK_NAME=holesky

# Sepolia testnet
export NETWORK_NAME=sepolia
```

Each network has its own configuration in `config/forks.yaml` including:
- **Genesis time**: Network start timestamp
- **Slot timing**: Seconds per slot (12s for Ethereum, 5s for Gnosis)
- **Epoch size**: Slots per epoch (32 for Ethereum, 16 for Gnosis)
- **Fork schedule**: Exact activation epochs for each fork

## Usage

### Load Commands

```bash
# Realtime loading
python -m src.main load realtime

# Backfill historical data
python -m src.main load backfill --start-slot 0 --end-slot 1000000

# Load one-time data (specs, genesis)
python -m src.main load onetime
```

### Transform Commands

```bash
# Run fork-aware transformer continuously
python -m src.main transform run --batch-size 100

# Reprocess specific range with current fork parsers
python -m src.main transform reprocess --start-slot 0 --end-slot 1000 --batch-size 100
```

### Fork Commands

```bash
# List all forks and their activation epochs
python -m src.main fork list

# Check which fork is active for a specific slot
python -m src.main fork info --slot 22000000

# Check fork for specific epoch
python -m src.main fork info --epoch 269568
```

## Docker Usage

### Using Docker Compose

```bash
# Build images
docker compose build

# Run migrations (includes all fork tables)
docker compose --profile migration up

# Start realtime loader
docker compose --profile realtime up -d

# Start fork-aware transformer
docker compose --profile transform up -d

# Run backfill (edit end-slot in docker-compose.yml)
docker compose --profile backfill up
```

## Configuration

Environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `BEACON_NODE_URL` | Beacon node API endpoint | `http://localhost:5052` |
| `CLICKHOUSE_HOST` | ClickHouse host | `localhost` |
| `CLICKHOUSE_PORT` | ClickHouse port | `9000` |
| `CLICKHOUSE_USER` | ClickHouse username | `default` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | `` |
| `CLICKHOUSE_DATABASE` | ClickHouse database | `beacon_chain` |
| `CLICKHOUSE_SECURE` | Use secure connection | `false` |
| `ENABLED_LOADERS` | Comma-separated loader names | `blocks,validators,specs,genesis` |
| `VALIDATOR_MODE` | Validator processing mode | `daily` |
| `START_SLOT` | Default start slot for backfill | `0` |
| `END_SLOT` | Default end slot for backfill | `` (current head) |
| `BACKFILL_WORKERS` | Number of backfill workers | `4` |
| `CHUNK_SIZE` | Slots per chunk in backfill | `1000` |
| `NETWORK_NAME` | Network for fork schedule | `mainnet` (mainnet, gnosis, holesky, sepolia) |
| `LOG_LEVEL` | Logging level | `INFO` |

### Validator Processing Modes

| Mode | Description | Use Case | Resource Usage |
|------|-------------|----------|----------------|
| `daily` | Process validators only on the last slot of each day | Production efficiency | Low |
| `all_slots` | Process validators for every slot | Complete historical data | High |

## Adding Support for New Forks

### 1. Update Fork Configuration

```yaml
# config/forks.yaml
networks:
  mainnet:
    forks:
      new_fork:
        version: "0x06000000"
        epoch: 500000
        schema_version: 7
```

### 2. Create Database Migration

```sql
-- migrations/011_fork_new_fork.sql
ALTER TABLE blocks ADD COLUMN IF NOT EXISTS new_fork_field String DEFAULT '';

CREATE TABLE IF NOT EXISTS new_fork_table (
    slot UInt64,
    new_field String,
    -- ... other fields
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot;
```

### 3. Create Fork Parser

```python
# src/parsers/new_fork.py
from .previous_fork import PreviousForkParser

class NewForkParser(PreviousForkParser):
    def __init__(self):
        super().__init__()
        self.fork_name = "new_fork"
    
    def get_supported_tables(self):
        return super().get_supported_tables() + ["new_fork_table"]
    
    def parse_fork_specific(self, slot, data):
        # Parse new fork-specific data
        pass
```

### 4. Register Parser

```python
# src/parsers/factory.py
from .new_fork import NewForkParser

# Add to _init_parsers()
self.parser_cache["new_fork"] = NewForkParser()
```

## Database Schema

### Raw Data Tables (Fork-Agnostic)
- `raw_blocks`: Raw beacon block data
- `raw_validators`: Raw validator data  
- `raw_specs`: Raw chain specifications
- `raw_genesis`: Raw genesis information

### Structured Tables (Fork-Aware)

#### Base Tables (Phase 0)
- `blocks`: Processed beacon blocks with fork-specific fields
- `attestations`: Block attestations
- `validators`: Validator information

#### Fork-Specific Tables
- `sync_aggregates`, `sync_committees` (Altair+)
- `execution_payloads`, `execution_transactions` (Bellatrix+)
- `withdrawals`, `bls_to_execution_changes` (Capella+)
- `blob_sidecars`, `kzg_commitments` (Deneb+)
- `deposit_requests`, `withdrawal_requests`, `consolidation_requests` (Electra+)

### State Management
- `load_state_chunks`: Tracks backfill progress
- `sync_progress`: Tracks processing progress and schema version

## Development

### Project Structure

```
beacon-indexer/
├── config/
│   └── forks.yaml                # Fork configuration for all networks
├── src/
│   ├── config.py                 # Configuration
│   ├── cli.py                    # Command line interface with fork commands
│   ├── main.py                   # Entry point
│   ├── utils/
│   │   └── logger.py             # Logging setup
│   ├── services/
│   │   ├── beacon_api.py         # Beacon API client
│   │   ├── clickhouse.py         # ClickHouse client
│   │   ├── fork.py               # Fork detection service
│   │   ├── loader.py             # Load service (fork-agnostic)
│   │   └── transformer.py        # Transform service (fork-aware)
│   ├── loaders/
│   │   ├── __init__.py           # Loader registry
│   │   ├── base.py               # Base loader class
│   │   ├── blocks.py             # Blocks loader
│   │   ├── validators.py         # Validators loader
│   │   ├── specs.py              # Specs loader
│   │   └── genesis.py            # Genesis loader
│   └── parsers/
│       ├── __init__.py           # Parser registry
│       ├── factory.py            # Parser factory
│       ├── fork_base.py          # Base fork parser
│       ├── phase0.py             # Phase 0 parser
│       ├── altair.py             # Altair parser
│       ├── bellatrix.py          # Bellatrix parser
│       ├── capella.py            # Capella parser
│       ├── deneb.py              # Deneb parser
│       └── electra.py            # Electra parser
├── migrations/
│   ├── 001_initial_schema.sql    # Base schema
│   ├── 006_fork_altair.sql       # Altair fork tables
│   ├── 007_fork_bellatrix.sql    # Bellatrix fork tables
│   ├── 008_fork_capella.sql      # Capella fork tables
│   ├── 009_fork_deneb.sql        # Deneb fork tables
│   └── 010_fork_electra.sql      # Electra fork tables
├── scripts/
│   └── migrate.py                # Migration script
├── docker-compose.yml            # Docker composition
├── Dockerfile                    # Docker image
└── README.md                     # This file
```

### Running Tests

```bash
# Install development dependencies
pip install -r requirements.txt

# Test fork detection
python -m src.main fork list

# Test specific fork parsing
python -m src.main fork info --slot 22000000
```

## Monitoring

### Fork-Aware Monitoring

The indexer provides detailed logging for fork detection and parsing:

```bash
# View fork detection logs
docker compose logs transform | grep fork

# Monitor chunk progress
python scripts/chunks.py overview

# Check schema version
python -c "
from src.services.clickhouse import ClickHouse
ch = ClickHouse()
result = ch.execute('SELECT last_processed_slot FROM sync_progress WHERE process_name = \"schema_version\"')
print(f'Schema version: {result[0][\"last_processed_slot\"] if result else \"unknown\"}')
"
```

Key log fields for fork-aware operations:
- `fork`: Active fork name for the slot being processed
- `parser`: Parser class being used
- `schema_version`: Database schema version
- `tables`: Number of tables populated by fork parser

## Troubleshooting

### Common Issues

1. **Fork Detection Failed**
   - Check `config/forks.yaml` exists and is valid
   - Verify `NETWORK_NAME` environment variable
   - Ensure genesis and specs data are loaded

2. **Parser Not Found for Fork**
   - Verify parser is registered in `ParserFactory`
   - Check fork name spelling in configuration
   - Ensure parser class is imported

3. **Schema Version Mismatch**
   - Run migrations: `make migration`
   - Check `sync_progress` table for schema version
   - Verify all fork migrations have been applied

4. **Fork Transition Issues**
   - Fork transitions are handled automatically
   - Check logs for parser switching messages
   - Verify fork activation epochs in configuration

### Performance Tuning

- **Fork Detection**: Fork lookups are cached per service instance
- **Parser Selection**: Parsers are instantiated once and reused
- **Batch Processing**: Fork grouping minimizes parser switching overhead
- **Database**: Fork-specific indexes on new tables improve query performance

### Adding New Networks

1. Add network configuration to `config/forks.yaml`:
```yaml
networks:
  my_network:
    genesis_time: 1234567890
    seconds_per_slot: 12
    slots_per_epoch: 32
    slots_per_historical_root: 8192
    genesis_fork_version: "0x00000000"
    forks:
      phase0: { version: "0x00000000", epoch: 0, schema_version: 1 }
      # ... other forks
```

2. Set `NETWORK_NAME` environment variable
3. Run migrations to create tables
4. Start indexing with network-specific fork schedule

## Examples

### Mainnet Usage
```bash
export NETWORK_NAME=mainnet
make migration && make backfill && make realtime && make transform
```

### Testnet Usage
```bash
export NETWORK_NAME=holesky
export START_SLOT=0
export END_SLOT=1000000
make migration && make backfill
```

### Gnosis Chain Usage
```bash
export NETWORK_NAME=gnosis
export BEACON_NODE_URL=https://beacon-chain.gnosis.io
make migration && make backfill && make realtime && make transform
```

### Fork Analysis Examples
```bash
# Check current mainnet forks
python -m src.main fork list

# Check Gnosis Chain forks (different timing)
export NETWORK_NAME=gnosis
python -m src.main fork list

# Analyze Deneb activation on mainnet
python -m src.main fork info --epoch 269568

# Analyze Deneb activation on Gnosis Chain
export NETWORK_NAME=gnosis  
python -m src.main fork info --epoch 889856

# Reprocess Merge transition on mainnet
python -m src.main transform reprocess --start-slot 4700000 --end-slot 4800000
```

## License

This project is licensed under the [MIT License](LICENSE)