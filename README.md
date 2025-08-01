# Beacon Chain Indexer

![Beacon Indexer](img/header-beacon_indexer.png)

A simple, minimalistic beacon chain indexer using the ELT (Extract, Load, Transform) pattern with **fork-aware parsing** and **automatic network detection**. This indexer loads raw data from the beacon API first, then transforms it into structured tables with automatic fork detection and appropriate parsing for each Ethereum consensus layer upgrade.

## Features

- **Dynamic Network Detection**: Automatically detects network type (mainnet, Gnosis Chain, Holesky, Sepolia) from beacon chain data
- **Fork-Aware Architecture**: Automatic detection and parsing of all Ethereum consensus forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- **ELT Architecture**: Raw data loading and transformation are completely separated
- **Range-Based Processing**: Enhanced transformer with gap-aware processing and progress tracking
- **Modular Design**: Easy to add new loaders, parsers, and support for future forks
- **Multiple Processing Modes**: Realtime sync, backfill with multiple workers
- **Resilient**: Raw data is never lost, transformations can be rerun with different fork parsers
- **Network Agnostic**: Supports mainnet, Gnosis Chain, Holesky, Sepolia with auto-detected fork schedules
- **Minimal Configuration**: Auto-detection reduces configuration requirements

## Architecture

### Load Stage
- **LoaderService**: Fetches raw JSON data from beacon API
- **Loaders**: Modular components for different endpoints (blocks, validators, rewards, specs, genesis)
- **Raw Tables**: Store unprocessed JSON data (fork-agnostic)

### Transform Stage (Fork-Aware)
- **ForkDetectionService**: Automatically detects network and active fork based on beacon chain data
- **ParserFactory**: Creates appropriate parser for each fork with network-aware fork versions
- **Fork-Specific Parsers**: Extract structured data according to fork specifications
- **Structured Tables**: Final destination for processed data with fork-specific fields
- **Range-Based Processing**: Gap-aware transformation with progress tracking

### Supported Forks

| Fork | Mainnet Epoch | Gnosis Epoch | Key Features | New Tables |
|------|---------------|---------------|--------------|------------|
| **Phase 0** | 0 | 0 | Basic blocks, attestations | `blocks`, `attestations` |
| **Altair** | 74240 | 512 | Sync committees | `sync_aggregates`, `sync_committees` |
| **Bellatrix** | 144896 | 385536 | Execution payloads (The Merge) | `execution_payloads`, `transactions` |
| **Capella** | 194048 | 648704 | Withdrawals (Shanghai) | `withdrawals`, `bls_changes` |
| **Deneb** | 269568 | 889856 | Blob transactions (Cancun) | `blob_sidecars`, `blob_commitments` |
| **Electra** | 364032 | 1337856 | Execution requests | `execution_requests` |

### Network Auto-Detection

The indexer automatically detects your network by analyzing:
- `CONFIG_NAME` parameter from beacon chain specs
- Genesis time and timing parameters
- Fork version patterns in historical blocks
- Network-specific fork activation epochs

Supported networks:
- **Mainnet**: 12s slots, 32 slots/epoch
- **Gnosis Chain**: 5s slots, 16 slots/epoch  
- **Holesky**: Testnet with mainnet timing
- **Sepolia**: Testnet with mainnet timing

## Quick Start

### 1. Setup Environment

```bash
cp .env.example .env
# Edit .env with your beacon node URL and database settings
# Network detection is automatic - no network configuration needed!
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
make transform-continuous
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
# Docker - Continuous processing
make transform-continuous

# Docker - Batch mode (process all and exit)
make transform

# Local
make dev-transform
python -m src.main transform run --continuous
```

**What Fork-Aware Transform Does**:
- Automatically detects the network and fork for each slot
- Uses appropriate parser (Phase0Parser, AltairParser, BellatrixParser, etc.)
- Extracts fork-specific fields (sync aggregates, execution payloads, blobs, etc.)
- Inserts data into correct tables with proper schema versioning
- Handles fork transitions seamlessly within batches
- Processes data with gap-aware range tracking
- Can reprocess historical data: `python -m src.main transform reprocess --start-slot 0 --end-slot 1000`

## Adding New API Endpoints

The indexer is designed to be easily extensible. Here's a step-by-step guide to add a new beacon chain API endpoint with raw data loading and fork-aware transformation.

### Step 1: Add API Method to BeaconAPI

Add the new endpoint method to `src/services/beacon_api.py`:

```python
async def get_committees(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
    """Get committees for a given state."""
    return await self.get(f"/eth/v1/beacon/states/{state_id}/committees")
```

### Step 2: Create Raw Table Migration

Create a migration file (e.g., `migrations/012_raw_committees.sql`):

```sql
-- Raw committees data
CREATE TABLE IF NOT EXISTS raw_committees (
    slot UInt64,
    payload String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)  
ORDER BY slot 
PARTITION BY toStartOfMonth(slot_timestamp);
```

### Step 3: Create Structured Table Migration

Add structured tables in the same migration or a new one:

```sql
-- Committees table
CREATE TABLE IF NOT EXISTS committees (
    slot UInt64,
    committee_index UInt64,
    validators Array(UInt64),
    validator_count UInt32,
    timestamp_utc DateTime DEFAULT toDateTime(0),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, committee_index)
PARTITION BY toStartOfMonth(slot_timestamp);
```

### Step 4: Create Loader

Create `src/loaders/committees.py`:

```python
import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class CommitteesLoader(BaseLoader):
    """Loader for committee data with String payload storage."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("committees", beacon_api, clickhouse)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch committee data from beacon API using slot as state_id."""
        return await self.beacon_api.get_committees(str(slot))
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare committee data for database insertion with String payload."""
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "retrieved_at": datetime.now()
        }
```

### Step 5: Register Loader

Add the loader to `src/loaders/__init__.py`:

```python
from .committees import CommitteesLoader

LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader,
    "rewards": RewardsLoader,
    "committees": CommitteesLoader  # Add new loader
}
```

### Step 6: Create Parser

Create `src/parsers/committees.py`:

```python
import json
from typing import Dict, List, Any
from .base import BaseParser

class CommitteesParser(BaseParser):
    """Parser for committee data stored as String payloads."""
    
    def __init__(self):
        super().__init__("committees")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse committee data from String payload into structured format."""
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")
        
        try:
            # Parse JSON string back to dict
            if isinstance(payload_str, str):
                payload = json.loads(payload_str)
            else:
                payload = payload_str
        except (json.JSONDecodeError, TypeError):
            return {}
        
        if "data" not in payload:
            return {}
        
        committees_data = payload["data"]
        committee_rows = []
        
        for committee in committees_data:
            committee_rows.append({
                "slot": slot,
                "committee_index": int(committee.get("index", 0)),
                "validators": [int(v) for v in committee.get("validators", [])],
                "validator_count": len(committee.get("validators", []))
            })
        
        return {"committees": committee_rows}
```

### Step 7: Add Fork-Aware Support (Optional)

If the endpoint changes behavior across forks, extend the fork parsers. For example, in `src/parsers/phase0.py`:

```python
def get_supported_tables(self) -> List[str]:
    """Phase 0 supports committees along with other tables."""
    return [
        "blocks", 
        "attestations", 
        "deposits", 
        "voluntary_exits", 
        "proposer_slashings", 
        "attester_slashings",
        "committees"  # Add committees support
    ]

def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Parse Phase 0 operations including committees if available."""
    result = super().parse_fork_specific(slot, data)
    
    # Add committees parsing if needed
    # (Usually committees are fetched separately, not from block data)
    
    return result
```

### Step 8: Update Loader Service for Chunk Generation

For slot-based loaders (like blocks or rewards), you need to update the chunk generation logic in `src/services/loader.py`:

**Find the `_generate_smart_chunks` method and update the condition:**

```python
# BEFORE (only handles blocks)
if loader_name == "blocks":
    # Blocks: process all slots

# AFTER (handles blocks, rewards, and other slot-based loaders)
if loader_name in ["blocks", "rewards", "committees"]:  # Add your new loader here
    # Blocks, Rewards, Committees: process all slots (slot-based loaders)
```

**⚠️ Important**: If you skip this step, your loader will not generate any chunks and workers will have no work to do, resulting in empty `status_counts={}` in the logs.

### Step 9: Update Transformer Service

If your new endpoint requires special handling in the transformer, modify `src/services/transformer.py`:

```python
async def run(self, batch_size: int = 100, continuous: bool = False):
    """Run the transformer with per-table range tracking."""
    # ... existing code ...
    
    while consecutive_empty_rounds < max_empty_rounds:
        try:
            processed_any = False
            
            # Process blocks (fork-aware)
            if await self._process_table_batch("raw_blocks", "blocks", batch_size):
                processed_any = True
            
            # Process validators (NOT fork-aware)
            if await self._process_table_batch("raw_validators", "validators", batch_size):
                processed_any = True
            
            # Process committees (choose fork-aware or not)
            if await self._process_table_batch("raw_committees", "committees", batch_size):
                processed_any = True
```

### Step 10: Update Configuration

Add the new loader to your environment configuration. Update `.env`:

```bash
# Add committees to enabled loaders
ENABLED_LOADERS=blocks,validators,specs,genesis,rewards,committees
```

### Step 11: Test and Deploy

1. **Run Migration**:
   ```bash
   make migration
   ```

2. **Test Loading**:
   ```bash
   # Test loading a few slots
   python -m src.main load backfill --start-slot 9000000 --end-slot 9000010
   ```

3. **Test Transformation**:
   ```bash
   # Test transforming the loaded data
   python -m src.main transform batch
   ```

4. **Verify Data**:
   ```sql
   -- Check raw data
   SELECT COUNT(*) FROM raw_committees;
   
   -- Check transformed data
   SELECT COUNT(*) FROM committees;
   
   -- Sample data
   SELECT * FROM committees ORDER BY slot DESC LIMIT 5;
   ```

This modular approach ensures that new endpoints integrate seamlessly with the existing fork-aware architecture, worker-based backfilling, and real-time processing systems.

## Fork Management

### Fork Information Commands

```bash
# List all auto-detected forks for current network
python -m src.main fork list

# Get fork information for specific slot
python -m src.main fork info --slot 5000000

# Get fork information for specific epoch
python -m src.main fork info --epoch 150000
```

### Network Auto-Detection

The indexer automatically detects your network from beacon chain data:

```bash
# Check detected network and fork schedule
python -m src.main fork list
```

**Detection Process:**
1. **Network Type**: Reads `CONFIG_NAME` from beacon chain specs
2. **Timing Parameters**: Extracts `SECONDS_PER_SLOT`, `SLOTS_PER_EPOCH` from specs
3. **Genesis Time**: Gets network start time from genesis data
4. **Fork Epochs**: Analyzes version changes in historical blocks
5. **Fork Versions**: Maps to network-specific fork versions

**Supported Networks:**
- **mainnet**: Auto-detected from `CONFIG_NAME = "mainnet"`
- **gnosis**: Auto-detected from `CONFIG_NAME = "gnosis"`
- **holesky**: Auto-detected from beacon chain characteristics
- **sepolia**: Auto-detected from beacon chain characteristics

Each network has its own automatically detected configuration:
- **Genesis time**: Network start timestamp (from genesis data)
- **Slot timing**: Seconds per slot (from specs: 12s for Ethereum, 5s for Gnosis)
- **Epoch size**: Slots per epoch (from specs: 32 for Ethereum, 16 for Gnosis)
- **Fork schedule**: Activation epochs detected from historical blocks

## Usage

### Load Commands

```bash
# Realtime loading
python -m src.main load realtime

# Backfill historical data
python -m src.main load backfill --start-slot 0 --end-slot 1000000
```

### Transform Commands

```bash
# Run fork-aware transformer continuously
python -m src.main transform run --continuous --batch-size 100

# Run batch mode (process all available data and exit)
python -m src.main transform batch --batch-size 100

# Reprocess specific range with current fork parsers
python -m src.main transform reprocess --start-slot 0 --end-slot 1000 --batch-size 100
```

### Fork Commands

```bash
# List all auto-detected forks and their activation epochs
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

# Start fork-aware transformer (continuous mode)
docker compose --profile transform-continuous up -d

# Start fork-aware transformer (batch mode)
docker compose --profile transform up

# Run backfill (edit START_SLOT/END_SLOT in .env)
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
| `ENABLED_LOADERS` | Comma-separated loader names | `blocks,validators,specs,genesis,rewards` |
| `VALIDATOR_MODE` | Validator processing mode | `daily` |
| `START_SLOT` | Default start slot for backfill | `0` |
| `END_SLOT` | Default end slot for backfill | `` (current head) |
| `BACKFILL_WORKERS` | Number of backfill workers | `4` |
| `CHUNK_SIZE` | Slots per chunk in backfill | `1000` |
| `LOG_LEVEL` | Logging level | `INFO` |

**Network Detection**: No manual network configuration required! The indexer automatically detects your network from beacon chain data.

### Validator Processing Modes

| Mode | Description | Use Case | Resource Usage |
|------|-------------|----------|----------------|
| `daily` | Process validators only on the last slot of each day | Production efficiency | Low |
| `all_slots` | Process validators for every slot | Complete historical data | High |

## Adding Support for New Forks

### 1. Update Fork Configuration (Optional)

The indexer auto-detects most fork information, but you can override in `config/forks.yaml`:

```yaml
fork_versions:
  mainnet:
    new_fork: "0x06000000"
  gnosis:
    new_fork: "0x06000064"

fork_order:
  - phase0
  - altair
  - bellatrix
  - capella
  - deneb
  - electra
  - new_fork  # Add to end of list
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

    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for new fork (mainnet)."""
        return "0x06000000"
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
- `raw_rewards`: Raw reward data
- `raw_specs`: Raw chain specifications
- `raw_genesis`: Raw genesis information

### Structured Tables (Fork-Aware)

#### Base Tables (Phase 0)
- `blocks`: Processed beacon blocks with fork-specific fields
- `attestations`: Block attestations
- `validators`: Validator information
- `rewards`: Block reward information

#### Fork-Specific Tables
- `sync_aggregates`, `sync_committees` (Altair+)
- `execution_payloads`, `transactions` (Bellatrix+)
- `withdrawals`, `bls_changes` (Capella+)
- `blob_sidecars`, `blob_commitments` (Deneb+)
- `execution_requests` (Electra+)

### State Management
- `load_state_chunks`: Tracks backfill progress
- `transformer_progress`: Tracks range-based transformation progress with gap handling

## Development

### Project Structure

```
beacon-indexer/
├── config/
│   └── forks.yaml                # Minimal fork configuration (auto-detection fallback)
├── src/
│   ├── config.py                 # Configuration
│   ├── cli.py                    # Command line interface with fork commands
│   ├── main.py                   # Entry point
│   ├── utils/
│   │   └── logger.py             # Logging setup
│   ├── services/
│   │   ├── beacon_api.py         # Beacon API client
│   │   ├── clickhouse.py         # ClickHouse client
│   │   ├── fork.py               # Auto-detecting fork service
│   │   ├── loader.py             # Load service (fork-agnostic)
│   │   └── transformer.py        # Transform service (fork-aware, range-based)
│   ├── loaders/
│   │   ├── __init__.py           # Loader registry
│   │   ├── base.py               # Base loader class
│   │   ├── blocks.py             # Blocks loader
│   │   ├── validators.py         # Validators loader
│   │   ├── rewards.py            # Rewards loader
│   │   ├── specs.py              # Specs loader
│   │   └── genesis.py            # Genesis loader
│   └── parsers/
│       ├── __init__.py           # Parser registry
│       ├── factory.py            # Network-aware parser factory
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
│   ├── migrate.py                # Migration script
│   ├── chunks.py                 # Chunk status checker
│   └── transformer_status.py     # Transformer progress checker
├── docker-compose.yml            # Docker composition
├── Dockerfile                    # Docker image
└── README.md                     # This file
```

### Running Tests

```bash
# Install development dependencies
pip install -r requirements.txt

# Test auto-detection
python -m src.main fork list

# Test specific fork parsing
python -m src.main fork info --slot 22000000
```

## Monitoring

### Fork-Aware Monitoring

The indexer provides detailed logging for fork detection and parsing:

```bash
# View fork detection logs
docker compose logs transform-continuous | grep fork

# Monitor chunk progress
python scripts/chunks.py overview

# Monitor transformer progress
python scripts/transformer_status.py

# Check auto-detected network
python -m src.main fork list
```

Key log fields for fork-aware operations:
- `fork`: Active fork name for the slot being processed
- `parser`: Parser class being used
- `network`: Auto-detected network name
- `tables`: Number of tables populated by fork parser

## Troubleshooting

### Common Issues

1. **Network Auto-Detection Failed**
   - Ensure genesis and specs data are loaded: `make migration`
   - Check that foundation data exists: `SELECT COUNT(*) FROM genesis; SELECT COUNT(*) FROM specs;`
   - Verify beacon node is accessible and returning valid data

2. **Fork Detection Failed**
   - Check that raw blocks contain version information
   - Ensure `CONFIG_NAME` is present in specs table
   - Verify timing parameters in `time_helpers` table

3. **Parser Not Found for Fork**
   - Check that all fork parsers are registered in `ParserFactory`
   - Verify parser class is imported and instantiated correctly
   - Check fork name spelling in auto-detection

4. **Range Processing Issues**
   - Use transformer status script: `python scripts/transformer_status.py`
   - Check for failed ranges in `transformer_progress` table
   - Verify raw data exists before transformation

### Performance Tuning

- **Auto-Detection**: Network and fork detection is cached per service instance
- **Parser Selection**: Parsers are instantiated once and reused
- **Batch Processing**: Fork grouping minimizes parser switching overhead
- **Range Processing**: Gap-aware processing optimizes transformer efficiency
- **Database**: Fork-specific indexes on new tables improve query performance

### Manual Network Override

If auto-detection fails, you can add manual override in `config/forks.yaml`:

```yaml
# Force specific network (bypasses auto-detection)
network_mapping:
  mainnet: "mainnet"
  gnosis: "gnosis"
  
# Override fork versions if needed
fork_versions:
  mainnet:
    phase0: "0x00000000"
    altair: "0x01000000"
    # ... other forks
```

## Examples

### Mainnet Usage (Auto-Detection)
```bash
# No network configuration needed - auto-detected!
make migration && make backfill && make realtime && make transform-continuous
```

### Gnosis Chain Usage (Auto-Detection)
```bash
# Auto-detects Gnosis Chain from beacon data
export BEACON_NODE_URL=https://beacon-chain.gnosis.io
make migration && make backfill && make realtime && make transform-continuous
```

### Fork Analysis Examples
```bash
# Check auto-detected network and forks
python -m src.main fork list

# Analyze specific fork activation (auto-detected network)
python -m src.main fork info --epoch 269568

# Reprocess fork transition with current parsers
python -m src.main transform reprocess --start-slot 4700000 --end-slot 4800000

# Check transformer processing status
python scripts/transformer_status.py

# Monitor chunk progress
python scripts/chunks.py overview
```

### Development Examples
```bash
# Test auto-detection with different beacon nodes
export BEACON_NODE_URL=https://your-beacon-node.com
python -m src.main fork list

# Process small range for testing
python -m src.main load backfill --start-slot 9000000 --end-slot 9000100
python -m src.main transform batch --batch-size 50

# Check range-based processing
python scripts/transformer_status.py
```

## License

This project is licensed under the [MIT License](LICENSE)