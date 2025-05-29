![Beacon-Indexer Header](img/header-beacon_indexer.png)

A scraper for Beacon Chain that extracts and stores data in a ClickHouse database for analysis.

## Overview

This project provides a complete solution for scraping data from a beacon chain node and storing it in a ClickHouse database. It supports both historical backfilling and real-time streaming of beacon chain data with multiple processing modes for optimal performance.

Key features:
- Extract data from multiple beacon chain endpoints (blocks, attestations, validators, etc.)
- Store data in an optimized ClickHouse database schema
- Support for all consensus layer forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- Multiple processing modes: realtime, historical, and parallel
- Configurable scrapers - run only what you need
- Intelligent validator processing (daily snapshots for efficiency)
- Bulk insertion optimization for high-throughput scenarios
- Comprehensive data model for all beacon chain components

## Requirements

- Python 3.8+
- ClickHouse database (local or ClickHouse Cloud)
- Access to a beacon chain node (Lighthouse, Prysm, Teku, Nimbus, or others)
- Docker and Docker Compose (for containerized deployment)

## Installation

### Option 1: Local Installation

#### 1. Clone the repository

```bash
git clone https://github.com/yourusername/beacon-indexer.git
cd beacon-indexer
```

#### 2. Set up a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

#### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### Option 2: Docker Installation

#### 1. Clone the repository

```bash
git clone https://github.com/yourusername/beacon-indexer.git
cd beacon-indexer
```

#### 2. Configure environment

Create a `.env` file by copying the example:

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration (see Configuration section below).

#### 3. Build and start the Docker containers

```bash
docker compose build
```

## Configuration

### Local ClickHouse Setup

If you're using a local ClickHouse instance, you can set it up with:

```bash
# Using the provided script
./scripts/setup_clickhouse.sh

# Or manually run migrations
./run_migrations.sh
```

### ClickHouse Cloud Setup

For ClickHouse Cloud, you'll need to:

1. Create a ClickHouse Cloud account and instance
2. Configure your `.env` file with your ClickHouse Cloud credentials
3. Run migrations using the provided script:

```bash
./run_migrations.sh
```

### Environment Configuration

Create a `.env` file by copying the example:

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration:

```ini
# Beacon Node configuration
BEACON_NODE_URL=http://your-beacon-node:5052

# ClickHouse configuration
CLICKHOUSE_HOST=your-instance.cloud.clickhouse.com  # For ClickHouse Cloud
CLICKHOUSE_PORT=443  # Use 443 for ClickHouse Cloud or 9000 for local
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=beacon_chain

# Scraper configuration
SCRAPER_MODE=realtime  # Options: realtime, historical, parallel
HISTORICAL_START_SLOT=0
HISTORICAL_END_SLOT=  # Leave empty to use latest slot
BATCH_SIZE=100
MAX_CONCURRENT_REQUESTS=5
PARALLEL_WORKERS=4  # For parallel mode
LOG_LEVEL=20  # 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
ENABLED_SCRAPERS=block,validator,reward,blob,specs  # Comma-separated list of scrapers to enable
```

## Running the Indexer

### Using Docker (Recommended)

First, run migrations to prepare the database:

```bash
# Run migrations
docker compose run --rm beacon-migrate
```

Then start the indexer in your chosen mode:

```bash
# Start in detached mode
docker compose up -d beacon-scraper

# View logs
docker compose logs -f beacon-scraper
```

### Using Python Directly

The scraper can be run in three different modes with configurable components:

#### Real-time Mode

This mode continuously processes new blocks as they are produced on the chain:

```bash
python -m src.main --mode realtime --scrapers block,validator,reward,blob,specs
```

or using environment variables:

```bash
SCRAPER_MODE=realtime ENABLED_SCRAPERS=block,validator,reward,blob,specs python -m src.main
```

#### Historical Mode

This mode backfills data from a specified slot range:

```bash
python -m src.main --mode historical --scrapers block,validator,reward,blob,specs --start-slot 0 --end-slot 1000000 --batch-size 100
```

or using environment variables:

```bash
SCRAPER_MODE=historical HISTORICAL_START_SLOT=0 HISTORICAL_END_SLOT=1000000 BATCH_SIZE=100 ENABLED_SCRAPERS=block,validator,reward,blob,specs python -m src.main
```

If you don't specify an end slot, it will use the latest slot on the beacon chain.

#### Parallel Mode (NEW!)

This mode uses multiple workers to process historical data in parallel for maximum throughput:

```bash
python -m src.main --mode parallel --scrapers block,validator,reward,blob,specs --start-slot 0 --end-slot 1000000 --parallel-workers 8 --batch-size 1000
```

or using environment variables:

```bash
SCRAPER_MODE=parallel HISTORICAL_START_SLOT=0 HISTORICAL_END_SLOT=1000000 PARALLEL_WORKERS=8 BATCH_SIZE=1000 ENABLED_SCRAPERS=block,validator,reward,blob,specs python -m src.main
```

#### Selective Component Execution

You can choose which scrapers to enable by using the `--scrapers` parameter or the `ENABLED_SCRAPERS` environment variable:

```bash
# Run only block and validator scrapers in realtime mode
python -m src.main --mode realtime --scrapers block,validator

# Run only block data backfill in historical mode
python -m src.main --mode historical --scrapers block --start-slot 1000000 --end-slot 2000000
```

Available scrapers:
- `block`: Basic block data and components (attestations, deposits, etc.)
- `validator`: Validator information (daily snapshots for efficiency)
- `reward`: Block rewards and attestation rewards
- `blob`: Blob sidecars for data availability (Deneb+ only)
- `specs`: Chain specifications (runs once)

### Processing Modes Comparison

| Mode | Use Case | Performance | Resource Usage | Best For |
|------|----------|-------------|----------------|----------|
| **Realtime** | Live monitoring | Low-moderate | Low | Production monitoring |
| **Historical** | Single-threaded backfill | Moderate | Low-moderate | Initial sync, gap filling |
| **Parallel** | Multi-threaded backfill | High | High | Large historical ranges |

### Command Line Arguments

- `--mode`: Scraper mode (realtime, historical, or parallel)
- `--scrapers`: Comma-separated list of scrapers to enable (block,validator,reward,blob,specs)
- `--start-slot`: Starting slot for historical/parallel mode
- `--end-slot`: Ending slot for historical/parallel mode
- `--batch-size`: Number of slots to process in each batch
- `--parallel-workers`: Number of parallel workers (parallel mode only)

## Project Structure

```
beacon-indexer/
├── README.md                   # Project documentation
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile                  # Docker image definition
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Project metadata
├── run_clickhouse_migrations.py # Database migration script
├── run_migrations.sh           # Migration helper script
├── setup.py                    # Package installation script
├── .env.example                # Example environment variables
├── migrations/                 # Database migrations
│   ├── 001_initial_schema.up.sql
│   ├── 001_initial_schema.down.sql
│   ├── 002_add_indices.up.sql
│   └── 002_add_indices.down.sql
├── scripts/                    # Utility scripts
│   ├── docker-entrypoint.sh    # Docker entrypoint
│   └── update_specs.py         # Specs updater script
├── src/                        # Source code
│   ├── __init__.py
│   ├── config.py               # Configuration management
│   ├── main.py                 # Main entry point
│   ├── models/                 # Data models
│   ├── repositories/           # Database access layer
│   ├── scrapers/               # Data extraction components
│   ├── services/               # Core services
│   └── utils/                  # Utility functions
└── tests/                      # Test suite
```

## Components

### Scrapers

The project includes specialized scrapers for each type of beacon chain data:

- `BlockScraper`: Extracts block information and embedded components (attestations, deposits, slashings, etc.)
- `ValidatorScraper`: Extracts validator information with intelligent daily snapshot processing
- `BlobSidecarScraper`: Processes blob sidecars (Deneb+)
- `RewardScraper`: Extracts reward calculations for blocks, attestations, and sync committees
- `SpecsScraper`: Extracts chain specifications (runs once)

### Services

- `BeaconAPIService`: Interface with the beacon chain node API
- `ClickHouseService`: Database interaction layer with bulk insertion optimization
- `HistoricalService`: Coordinates historical data scraping
- `RealtimeService`: Manages real-time data processing
- `ParallelService`: Manages multi-worker parallel processing (NEW!)
- `BulkInsertionService`: Optimizes database insertions for high throughput

### Models

The project uses Pydantic models to ensure data integrity:

- `BeaconBlock`, `Attestation`, `Validator`, etc.
- Models include methods for conversion between API response and database formats

### Repositories

Database access layer with specialized repositories for each data type:

- `BeaconBlockRepository`, `AttestationRepository`, `ValidatorRepository`, etc.
- Provides CRUD operations and specialized queries

## Running Modes

### Realtime Mode

Realtime mode continuously processes new blocks as they are produced on the beacon chain:

- Polls the beacon node at regular intervals
- Processes one block at a time as they are created
- Maintains state to track the last processed slot
- Runs as a daemon process
- Validator scraper only processes target slots (last slot of each day)

This mode is ideal for keeping a database continuously updated with the current state of the chain.

### Historical Mode

Historical mode backfills data by processing past blocks:

- Processes a defined range of slots from a start to end point
- Uses batch processing for efficiency
- Terminates when the specified range is completed
- Intelligent slot targeting for validator scraper

This mode is ideal for initial database population or filling gaps in data.

### Parallel Mode (NEW!)

Parallel mode uses multiple workers to process historical data simultaneously:

- Spawns multiple worker processes to handle different slot ranges
- Optimized for maximum throughput on large historical ranges
- Uses bulk insertion for database efficiency
- Automatically manages worker coordination and range assignment
- Each worker processes its assigned range independently

This mode is ideal for rapidly processing large amounts of historical data.

## Validator Processing Optimization

The validator scraper has been optimized for efficiency:

- **Daily Snapshots**: Only processes the last slot of each day to reduce storage requirements
- **Smart Slot Detection**: Automatically identifies target slots based on UTC day boundaries
- **Batch Processing**: Processes validators in configurable batch sizes
- **Memory Optimization**: Uses streaming processing to handle large validator sets

This approach reduces database size by ~99% while maintaining essential validator state information.

## Database Migration Details

The migration system is designed to work with both local ClickHouse instances and ClickHouse Cloud.

### File Structure for Migrations

```
beacon-indexer/
├── docker-compose.yml           # Configuration for Docker services
├── Dockerfile                   # Instructions to build the Docker image
├── run_clickhouse_migrations.py # The migration script that can be run directly or via Docker
├── run_migrations.sh            # Helper script to run migrations in various environments
└── migrations/                  # Directory containing SQL migration files
    ├── 001_initial_schema.up.sql
    └── 002_add_indices.up.sql
```

### Running Migrations

You can run migrations in several ways:

1. **Using Docker Compose**:
   ```bash
   docker compose run --rm beacon-migrate
   ```

2. **Using the helper script** (which will choose the best method automatically):
   ```bash
   ./run_migrations.sh
   ```

3. **Directly with the Python script** (if running outside Docker):
   ```bash
   ./run_clickhouse_migrations.py \
     host=your-instance.cloud.clickhouse.com \
     port=443 \
     user=default \
     password=your_password \
     db=beacon_chain \
     dir=./migrations \
     direction=up \
     secure=True \
     verify=False
   ```

### Advanced Migration Usage

#### Running Migrations Down

To revert migrations (rarely needed):

```bash
# Using Docker Compose with custom direction
docker compose run --rm -e CH_DIRECTION=down beacon-migrate

# Using the Python script directly
./run_clickhouse_migrations.py \
  host=your-instance.cloud.clickhouse.com \
  port=443 \
  user=default \
  password=your_password \
  db=beacon_chain \
  dir=./migrations \
  direction=down \
  secure=True \
  verify=False
```

#### Custom Migration Directory

If your migrations are in a different location:

```bash
# With Docker Compose
docker compose run --rm -e CH_MIGRATIONS_DIR=/custom/path beacon-migrate

# With Python script
./run_clickhouse_migrations.py dir=/custom/path
```

## Performance Optimization

### Bulk Insertion

The indexer uses sophisticated bulk insertion strategies:

- **Batched Inserts**: Groups multiple records into single database operations
- **Parallel Processing**: Multiple insertion queues for different table types
- **Adaptive Batch Sizing**: Automatically adjusts batch sizes based on table characteristics
- **Memory Management**: Streaming processing to handle large datasets

### Concurrency Control

- **Semaphore-based Rate Limiting**: Prevents overwhelming the beacon node API
- **Worker Coordination**: Intelligent range assignment in parallel mode
- **Database Connection Pooling**: Efficient database resource utilization

### Memory Optimization

- **Streaming Processing**: Processes data without loading entire datasets into memory
- **Cache Management**: LRU-style caching for frequently accessed data
- **Garbage Collection**: Automatic cleanup of processed data

## Troubleshooting

### Checking Migration Status

To check which migrations have been applied:

```sql
SELECT * FROM beacon_chain.migrations ORDER BY executed_at;
```

### Connection Issues

If you have trouble connecting to ClickHouse Cloud:

1. Verify your connection details in `.env`
2. Make sure `secure=True` for ClickHouse Cloud
3. Check for any firewalls blocking outbound connections on port 443
4. Try connecting with the ClickHouse client directly:
   ```bash
   clickhouse-client --host=your-instance.cloud.clickhouse.com \
     --port=443 \
     --user=default \
     --password=your_password \
     --secure
   ```

### Docker Issues

If you encounter issues with Docker:

1. Make sure all containers are properly stopped and removed:
   ```bash
   docker compose down --rmi all --volumes --remove-orphans
   ```

2. Rebuild from scratch:
   ```bash
   docker compose build --no-cache
   ```

3. Check if the changes to your code are being properly included in the built images by examining the container:
   ```bash
   docker compose run --rm --entrypoint bash beacon-scraper
   cat /app/src/main.py | grep "scrapers"
   ```

### Performance Issues

If you're experiencing slow processing:

1. **Increase Parallel Workers**: For historical/parallel mode, increase `PARALLEL_WORKERS`
2. **Adjust Batch Size**: Increase `BATCH_SIZE` for better throughput
3. **Tune Concurrency**: Adjust `MAX_CONCURRENT_REQUESTS` based on your beacon node capacity
4. **Monitor Resources**: Check CPU, memory, and network usage
5. **Database Optimization**: Ensure ClickHouse has sufficient resources

### Log Level Configuration

You can adjust the verbosity of logging by setting the `LOG_LEVEL` environment variable:

```bash
# Set to DEBUG level for maximum verbosity
LOG_LEVEL=10 docker compose up beacon-scraper

# Set to INFO level (default)
LOG_LEVEL=20 docker compose up beacon-scraper

# Set to WARNING level for minimal logs
LOG_LEVEL=30 docker compose up beacon-scraper
```

### Migration Script Errors

If you encounter errors with the migration script:

1. Check the error message for specific SQL issues
2. Verify that your migrations directory contains valid SQL files
3. Run with more detailed logging to see specifics of the error

### Validator Processing Issues

If validator processing is slow or failing:

1. **Reduce Batch Size**: Lower the validator batch size in the scraper configuration
2. **Check Memory Usage**: Validator processing can be memory-intensive
3. **Verify Target Slots**: Ensure the validator scraper is correctly identifying target slots
4. **Monitor API Timeouts**: Increase timeout values if the beacon node is slow

## Example Workflows

### Initial Setup and Full Historical Sync

```bash
# 1. Run migrations
./run_migrations.sh

# 2. Start with parallel mode for fast historical sync
SCRAPER_MODE=parallel \
HISTORICAL_START_SLOT=0 \
PARALLEL_WORKERS=8 \
BATCH_SIZE=1000 \
ENABLED_SCRAPERS=block,validator,reward,blob,specs \
python -m src.main

# 3. Switch to realtime mode for ongoing monitoring
SCRAPER_MODE=realtime \
ENABLED_SCRAPERS=block,validator,reward,blob \
python -m src.main
```

### Gap Filling

```bash
# Fill missing data between specific slots
SCRAPER_MODE=historical \
HISTORICAL_START_SLOT=5000000 \
HISTORICAL_END_SLOT=5100000 \
ENABLED_SCRAPERS=block,reward \
python -m src.main
```

### Validator-Only Processing

```bash
# Process only validator data for a specific period
SCRAPER_MODE=historical \
HISTORICAL_START_SLOT=4000000 \
HISTORICAL_END_SLOT=5000000 \
ENABLED_SCRAPERS=validator \
python -m src.main
```

## License

This project is licensed under the [MIT License](LICENSE).