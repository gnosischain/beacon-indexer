# Beacon Chain Indexer

![Beacon Indexer](img/header-beacon_indexer.png)

A production-ready beacon chain indexer using the ELT (Extract, Load, Transform) pattern with **fork-aware parsing**. This indexer loads raw data from the beacon API first, then transforms it into structured tables with automatic fork detection and appropriate parsing for each Ethereum consensus layer upgrade.

## Features

- **Fork-Aware Architecture**: Automatic detection and parsing of all Ethereum consensus forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- **ELT Architecture**: Raw data loading and transformation are completely separated for reliability
- **Multi-Network Support**: Mainnet, Gnosis Chain, Holesky, Sepolia with auto-detected fork schedules
- **Parallel Processing**: Multi-worker backfill with intelligent chunk distribution
- **Resilient Design**: Raw data is never lost, transformations can be rerun with different parsers
- **Range-Based Progress**: Handles gaps and failures gracefully with detailed progress tracking

## Quick Start

### 1. Configuration

```bash
cp .env.example .env
# Edit .env with your settings:
# - BEACON_NODE_URL: Your beacon node API endpoint
# - CLICKHOUSE_HOST: ClickHouse database host
# - START_SLOT/END_SLOT: Range for backfill
```

### 2. Build and Initialize

```bash
make build
make migration
```

### 3. Load Data

```bash
# Load historical data
make backfill

# Start real-time loading
make realtime

# Process into structured tables
make transform
```

### 4. Monitor Progress

```bash
make status
make logs
```

## Architecture Overview

### ELT Pipeline

1. **Extract & Load**: Raw JSON data from beacon API → `raw_blocks`, `raw_validators` tables
2. **Transform**: Fork-aware parsing → structured tables (`blocks`, `attestations`, `execution_payloads`, etc.)

### Fork-Aware Processing

The indexer automatically detects and handles all Ethereum consensus forks:

| Fork | Key Features | New Tables |
|------|--------------|------------|
| **Phase 0** | Basic blocks, attestations | `blocks`, `attestations`, `deposits` |
| **Altair** | Sync committees | `sync_aggregates`, `sync_committees` |
| **Bellatrix** | Execution payloads (The Merge) | `execution_payloads`, `transactions` |
| **Capella** | Withdrawals (Shanghai) | `withdrawals`, `bls_changes` |
| **Deneb** | Blob transactions (Cancun) | `blob_sidecars`, `blob_commitments` |
| **Electra** | Execution requests | `execution_requests` |

### Network Support

- **Mainnet**: Full production support
- **Gnosis Chain**: Different slot timing (5s vs 12s) and fork schedule
- **Testnets**: Holesky, Sepolia with appropriate configurations

## Essential Commands

```bash
# Build Docker images
make build

# Set up database schema
make migration

# Load historical data (set START_SLOT/END_SLOT in .env)
make backfill

# Start continuous data loading
make realtime

# Process raw data into structured tables
make transform

# Check progress
make status

# View logs
make logs

# Clean up
make clean
```

## Configuration

Key environment variables in `.env`:

```bash
# Beacon Node
BEACON_NODE_URL=http://localhost:5052

# ClickHouse Database
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=beacon_chain

# Data Loading
START_SLOT=0
END_SLOT=1000000
BACKFILL_WORKERS=4
CHUNK_SIZE=1000

# Validator Processing
VALIDATOR_MODE=daily  # or all_slots

# Network (auto-detected from beacon node)
# NETWORK_NAME=mainnet  # optional override
```

### Validator Processing Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `daily` | Process validators only on the last slot of each day | Production efficiency |
| `all_slots` | Process validators for every slot | Complete data requirements |

## Database Schema

### Raw Tables (Fork-Agnostic)
- `raw_blocks`: Raw beacon block JSON data
- `raw_validators`: Raw validator data  
- `raw_specs`: Chain specifications
- `raw_genesis`: Genesis information

### Structured Tables (Fork-Aware)

**Core Tables**
- `blocks`: Processed beacon blocks with fork-specific fields
- `attestations`: Block attestations
- `validators`: Validator information and states

**Fork-Specific Tables**
- `sync_aggregates`, `sync_committees` (Altair+)
- `execution_payloads`, `transactions` (Bellatrix+)
- `withdrawals`, `bls_changes` (Capella+)
- `blob_sidecars`, `blob_commitments` (Deneb+)
- `execution_requests` (Electra+)

**System Tables**
- `genesis`: Genesis time and configuration
- `specs`: Chain timing parameters
- `load_state_chunks`: Backfill progress tracking
- `transformer_progress`: Processing progress

## Network Examples

### Mainnet
```bash
# Uses default settings
make migration && make backfill && make realtime && make transform
```

### Gnosis Chain
```bash
export BEACON_NODE_URL=https://beacon-chain.gnosis.io
make migration && make backfill && make realtime && make transform
```

### Holesky Testnet
```bash
export BEACON_NODE_URL=https://ethereum-holesky-beacon-api.publicnode.com
export START_SLOT=0
export END_SLOT=100000
make migration && make backfill
```

## CLI Usage

The indexer also supports direct CLI usage:

```bash
# Fork information
python -m src.main fork list
python -m src.main fork info --slot 5000000

# Load operations
python -m src.main load realtime
python -m src.main load backfill --start-slot 0 --end-slot 1000000

# Transform operations
python -m src.main transform run --continuous
python -m src.main transform reprocess --start-slot 0 --end-slot 1000
```

## Monitoring

### Progress Tracking
```bash
# Overall progress
make status

# View logs
make logs

# Detailed monitoring
docker run --rm --env-file .env beacon-indexer:latest python scripts/transformer_status.py
```

### Key Metrics
- **Chunk Progress**: Backfill completion by loader
- **Transform Status**: Raw data processing progress  
- **Fork Detection**: Active fork per slot/epoch
- **Error Tracking**: Failed ranges and retry status

## Development

### Project Structure
```
beacon-indexer/
├── src/
│   ├── services/          # Core services
│   │   ├── beacon_api.py  # Beacon node client
│   │   ├── clickhouse.py  # Database client
│   │   ├── fork.py        # Fork detection
│   │   ├── loader.py      # Data loading service
│   │   └── transformer.py # Data transformation
│   ├── loaders/           # Data loaders
│   │   ├── blocks.py      # Block data loader
│   │   ├── validators.py  # Validator data loader
│   │   ├── specs.py       # Chain specs loader
│   │   └── genesis.py     # Genesis loader
│   └── parsers/           # Fork-aware parsers
│       ├── factory.py     # Parser factory
│       ├── phase0.py      # Phase 0 parser
│       ├── altair.py      # Altair parser
│       ├── bellatrix.py   # Bellatrix parser
│       ├── capella.py     # Capella parser
│       ├── deneb.py       # Deneb parser
│       └── electra.py     # Electra parser
├── migrations/            # Database migrations
├── config/               # Fork configurations
├── scripts/              # Utility scripts
└── docker-compose.yml    # Docker services
```

### Adding New Forks

1. **Update Fork Config** (`config/forks.yaml`)
2. **Create Migration** (`migrations/XXX_fork_name.sql`)
3. **Create Parser** (`src/parsers/fork_name.py`)
4. **Register Parser** (update `factory.py`)

## Troubleshooting

### Common Issues

**Fork Detection Failed**
- Ensure genesis and specs data are loaded
- Check network configuration

**Processing Stuck**
- Check `make status` for failed chunks
- Failed chunks are automatically retried

**Schema Errors**
- Run `make migration` to update schema
- Reprocess data: `python -m src.main transform reprocess`

### Performance

- **Backfill**: Adjust `BACKFILL_WORKERS` and `CHUNK_SIZE`
- **Transform**: Increase batch size for better throughput
- **Database**: Fork-specific indexes improve query performance

## Docker Services

The indexer runs as separate Docker services:

- **migration**: Database schema setup
- **backfill**: Historical data loading  
- **realtime**: Continuous data loading
- **transform**: Data processing

Each service can be scaled and monitored independently.

## License

This project is licensed under the [MIT License](LICENSE)