![Beacon-Indexer Header](img/header-beacon_indexer.png)

A comprehensive scraper for Beacon Chain that extracts and stores data in a ClickHouse database for analysis.

## Overview

This project provides a complete solution for scraping data from a beacon chain node and storing it in a ClickHouse database. It supports both historical backfilling and real-time streaming of beacon chain data.

Key features:
- Extract data from multiple beacon chain endpoints (blocks, attestations, validators, etc.)
- Store data in an optimized ClickHouse database schema
- Support for all consensus layer forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- Realtime and historical processing modes
- Configurable scrapers - run only what you need
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
SCRAPER_MODE=realtime  # Options: realtime, historical
HISTORICAL_START_SLOT=0
HISTORICAL_END_SLOT=  # Leave empty to use latest slot
BATCH_SIZE=100
MAX_CONCURRENT_REQUESTS=5
LOG_LEVEL=20  # 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL
ENABLED_SCRAPERS=block,validator,reward,blob,specs  # Comma-separated list of scrapers to enable
```

## Running the Indexer

### Using Docker (Recommended)

First, run migrations to prepare the database:

```bash
# Run migrations
docker compose run --rm migrate
```

Then start the indexer in realtime mode:

```bash
# Start in detached mode
docker compose up -d beacon-scraper

# View logs
docker compose logs -f beacon-scraper
```

Or run in historical mode to backfill data:

```bash
# Run historical backfill
docker compose up historical-scraper
```

### Using Python Directly

The scraper can be run in two modes with configurable components:

#### Real-time Mode

This mode continuously processes new blocks as they are produced on the chain:

```bash
python -m src.main --mode realtime --scrapers block,validator,reward,blob,specs
```

#### Historical Mode

This mode backfills data from a specified slot range:

```bash
python -m src.main --mode historical --scrapers block,validator,reward,blob,specs --start-slot 0 --end-slot 1000000 --batch-size 100
```

If you don't specify an end slot, it will use the latest slot on the beacon chain.

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
- `validator`: Validator information (daily snapshots)
- `reward`: Block rewards and attestation rewards
- `blob`: Blob sidecars for data availability (Deneb+ only)
- `specs`: Chain specifications

### Command Line Arguments

- `--mode`: Scraper mode (historical or realtime)
- `--scrapers`: Comma-separated list of scrapers to enable (block,validator,reward,blob,specs)
- `--start-slot`: Starting slot for historical mode
- `--end-slot`: Ending slot for historical mode
- `--batch-size`: Number of slots to process in each batch
- `--bulk-insert`: Use bulk insert mode for better performance

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
│   └── docker-entrypoint.sh    # Docker entrypoint
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

- `BlockScraper`: Extracts basic block information and block components
- `ValidatorScraper`: Extracts validator information
- `BlobSidecarScraper`: Processes blob sidecars (Deneb+)
- `RewardScraper`: Extracts reward calculations
- `SpecsScraper`: Extracts chain specifications

### Services

- `BeaconAPIService`: Interface with the beacon chain node API
- `ClickHouseService`: Database interaction layer
- `HistoricalService`: Coordinates historical data scraping
- `RealtimeService`: Manages real-time data processing

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

This mode is ideal for keeping a database continuously updated with the current state of the chain.

### Historical Mode

Historical mode backfills data by processing past blocks in bulk:

- Processes a defined range of slots from a start to end point
- Uses larger batch sizes for efficiency
- May use more concurrent processing to speed up data acquisition
- Terminates when the specified range is completed

This mode is ideal for initial database population or filling gaps in data.

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
   docker compose run --rm migrate
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
docker compose run --rm -e CH_DIRECTION=down migrate

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
docker compose run --rm -e CH_MIGRATIONS_DIR=/custom/path migrate

# With Python script
./run_clickhouse_migrations.py dir=/custom/path
```

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

## License

This project is licensed under the [MIT License](LICENSE).