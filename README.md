# Beacon Chain Indexer

A simple, minimalistic beacon chain indexer using the ELT (Extract, Load, Transform) pattern. This indexer loads raw data from the beacon API first, then transforms it into structured tables separately.

## Features

- **ELT Architecture**: Raw data loading and transformation are completely separated
- **Modular Design**: Easy to add new loaders and parsers
- **Multiple Processing Modes**: Realtime sync, backfill with multiple workers
- **Resilient**: Raw data is never lost, transformations can be rerun
- **Simple Configuration**: Minimal environment variables

## Architecture

### Load Stage
- **LoaderService**: Fetches raw JSON data from beacon API
- **Loaders**: Modular components for different endpoints (blocks, validators, specs, genesis)
- **Raw Tables**: Store unprocessed JSON data

### Transform Stage
- **TransformerService**: Processes raw data into structured tables
- **Parsers**: Extract structured data from raw JSON
- **Structured Tables**: Final destination for processed data

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

### 5. Start Data Transformation

```bash
make transform
# or locally: make dev-transform
```

## The 4 Essential Modes

### 1. Migration
**Purpose**: Set up database schema and tables
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
**Purpose**: Process raw data into structured tables for analytics
```bash
# Docker
make transform

# Local
make dev-transform
python -m src.main transform run
```

**What Transform Does**:
- Reads raw JSON data from `raw_blocks`, `raw_validators`, etc.
- Parses JSON into structured fields
- Inserts processed data into `blocks`, `attestations`, `validators` tables
- Runs continuously, processing new raw data as it arrives
- Can reprocess historical data: `python -m src.main transform reprocess --start-slot 0 --end-slot 1000`

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
# Run transformer continuously
python -m src.main transform run --batch-size 100

# Reprocess specific range
python -m src.main transform reprocess --start-slot 0 --end-slot 1000 --batch-size 100
```

## Docker Usage

### Using Docker Compose

```bash
# Run migrations
docker compose build
python scripts/migrate.py

# Load one-time data
docker compose --profile onetime up

# Start realtime loader
docker compose --profile loader up -d

# Start transformer
docker compose --profile transformer up -d

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
| `LOG_LEVEL` | Logging level | `INFO` |

### Validator Processing Modes

The indexer supports two validator processing modes:

| Mode | Description | Use Case | Resource Usage |
|------|-------------|----------|----------------|
| `daily` | Process validators only on the last slot of each day | Production efficiency | Low |
| `all_slots` | Process validators for every slot | Complete historical data | High |

**Daily Mode (Recommended)**:
- Captures validator snapshots once per day
- Significantly reduces storage and processing requirements
- Provides sufficient granularity for most analytics
- Follows the same pattern as the original beacon indexer

**All Slots Mode**:
- Captures validator state for every single slot
- Complete historical record but resource-intensive
- Use only when slot-level validator precision is required

## Adding New Endpoints

### 1. Create a New Loader

```python
# src/loaders/new_endpoint.py
from .base import BaseLoader

class NewEndpointLoader(BaseLoader):
    def __init__(self, beacon_api, clickhouse):
        super().__init__("new_endpoint", beacon_api, clickhouse)
    
    async def fetch_data(self, identifier):
        return await self.beacon_api.get(f"/api/endpoint/{identifier}")
    
    def prepare_row(self, identifier, data):
        return {
            "identifier": identifier,
            "payload": json.dumps(data),
            "retrieved_at": datetime.now()
        }
```

### 2. Register the Loader

```python
# src/loaders/__init__.py
from .new_endpoint import NewEndpointLoader

LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader,
    "new_endpoint": NewEndpointLoader  # Add here
}
```

### 3. Create a Parser (Optional)

```python
# src/parsers/new_endpoint.py
from .base import BaseParser

class NewEndpointParser(BaseParser):
    def __init__(self):
        super().__init__("new_endpoint")
    
    def parse(self, raw_data):
        # Extract structured data
        return {"new_table": [processed_rows]}
```

### 4. Add Database Tables

```sql
-- Add to migrations/001_initial_schema.sql

-- Raw table
CREATE TABLE raw_new_endpoint (
    identifier String,
    payload String,
    retrieved_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY identifier;

-- Structured table
CREATE TABLE new_table (
    id UInt64,
    field1 String,
    field2 UInt32
) ENGINE = MergeTree()
ORDER BY id;
```

### 5. Enable the Loader

```bash
# Add to .env
ENABLED_LOADERS=blocks,validators,specs,genesis,new_endpoint
```

## Database Schema

### Raw Data Tables
- `raw_blocks`: Raw beacon block data
- `raw_validators`: Raw validator data  
- `raw_specs`: Raw chain specifications
- `raw_genesis`: Raw genesis information

### Structured Tables
- `blocks`: Processed beacon blocks
- `attestations`: Block attestations
- `validators`: Validator information

### State Management
- `load_state_chunks`: Tracks backfill progress
- `sync_progress`: Tracks processing progress

## Development

### Project Structure

```
beacon-indexer/
├── src/
│   ├── config.py              # Configuration
│   ├── cli.py                 # Command line interface
│   ├── main.py                # Entry point
│   ├── utils/
│   │   └── logger.py          # Logging setup
│   ├── services/
│   │   ├── beacon_api.py      # Beacon API client
│   │   ├── clickhouse.py      # ClickHouse client
│   │   ├── loader.py          # Load service
│   │   └── transformer.py     # Transform service
│   ├── loaders/
│   │   ├── __init__.py        # Loader registry
│   │   ├── base.py            # Base loader class
│   │   ├── blocks.py          # Blocks loader
│   │   ├── validators.py      # Validators loader
│   │   ├── specs.py           # Specs loader
│   │   └── genesis.py         # Genesis loader
│   └── parsers/
│       ├── __init__.py        # Parser registry
│       ├── base.py            # Base parser class
│       ├── blocks.py          # Blocks parser
│       └── validators.py      # Validators parser
├── migrations/
│   └── 001_initial_schema.sql # Database schema
├── scripts/
│   └── migrate.py             # Migration script
├── docker-compose.yml         # Docker composition
├── Dockerfile                 # Docker image
├── docker-entrypoint.sh       # Docker entrypoint
├── requirements.txt           # Python dependencies
├── .env.example               # Environment template
└── README.md                  # Documentation
```

### Running Tests

```bash
# Install development dependencies
pip install -r requirements.txt

# Run the application locally
python -m src.main --help
```

## Monitoring

The indexer uses structured JSON logging. Key log fields:

- `loader`: Which loader is processing
- `slot`: Current slot being processed
- `worker`: Worker ID for backfill
- `error`: Error details if something fails

## Troubleshooting

### Common Issues

1. **ClickHouse Connection Failed**
   - Check `CLICKHOUSE_HOST` and credentials
   - Ensure ClickHouse is running and accessible

2. **Beacon API Timeouts**
   - Check `BEACON_NODE_URL`
   - Verify beacon node is synced and responsive

3. **Workers Not Processing**
   - Check `load_state_chunks` table for failed chunks
   - Restart backfill to retry failed chunks

4. **Transformer Lag**
   - Monitor `sync_progress` table
   - Increase batch size or reduce processing load

### Performance Tuning

- **Backfill**: Increase `BACKFILL_WORKERS` and `CHUNK_SIZE`
- **Realtime**: Reduce beacon node polling if CPU usage is high
- **Transform**: Increase batch size for better throughput
- **Database**: Add indexes for frequently queried columns

## License

MIT License