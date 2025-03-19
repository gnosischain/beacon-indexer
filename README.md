# Beacon Indexer

![Beacon-Indexer Header](img/header-beacon_indexer.png)

A scraper for Beacon Chain that extracts and stores data in a ClickHouse database for analysis.

## Overview

This project provides a complete solution for scraping data from a beacon chain node and storing it in a ClickHouse database. It supports both historical backfilling and real-time streaming of beacon chain data.

Key features:
- Extract data from multiple beacon chain endpoints (blocks, attestations, validators, etc.)
- Store data in an optimized ClickHouse database schema
- Support for all consensus layer forks (Phase 0, Altair, Bellatrix, Capella, Deneb, Electra)
- Realtime and historical processing modes
- Comprehensive data model for all beacon chain components

## Requirements

- Python 3.8+
- ClickHouse database server (version 21.3+)
- Access to an beacon chain node (Lighthouse, Prysm, Teku, Nimbus, or others)

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/beacon-chain-scraper.git
cd beacon-chain-scraper
```

### 2. Set up a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Setup ClickHouse

If you don't have ClickHouse installed, follow the [official ClickHouse installation guide](https://clickhouse.com/docs/en/getting-started/install).

Once installed, create the database and schema:

```bash
# Using the provided script
./scripts/setup_clickhouse.sh

# Or manually run migrations
python scripts/run_migrations.py
```

### 5. Configure environment

Create a `.env` file by copying the example:

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration:

```
# Beacon Node configuration
BEACON_NODE_URL=http://localhost:5052

# ClickHouse configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=beacon_chain

# Scraper configuration
SCRAPER_MODE=realtime  # Options: realtime, historical
HISTORICAL_START_SLOT=0
HISTORICAL_END_SLOT=  # Leave empty to use latest slot
BATCH_SIZE=100
MAX_CONCURRENT_REQUESTS=5
```

## Usage

### Running the Scraper

The scraper can be run in two modes:

#### Real-time Mode

This mode continuously processes new blocks as they are produced on the chain:

```bash
python -m src.main --mode realtime
```

#### Historical Mode

This mode backfills data from a specified slot range:

```bash
python -m src.main --mode historical --start-slot 0 --end-slot 1000000 --batch-size 100
```

If you don't specify an end slot, it will use the latest slot on the beacon chain.

### Command Line Arguments

- `--mode`: Scraper mode (historical or realtime)
- `--start-slot`: Starting slot for historical mode
- `--end-slot`: Ending slot for historical mode
- `--batch-size`: Number of slots to process in each batch
- `--bulk-insert`: Use bulk insert mode for better performance

## Project Structure

```
beacon-chain-scraper/
├── README.md                   # Project documentation
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Project metadata and tool configurations
├── setup.py                    # Package installation script
├── .env.example                # Example environment variables
├── migrations/                 # Database migrations
│   ├── 001_initial_schema.up.sql
│   ├── 001_initial_schema.down.sql
│   ├── 002_add_indices.up.sql
│   └── 002_add_indices.down.sql
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

## Database Schema

The schema is designed for efficient querying of beacon chain data:

- Optimized for time-series analysis
- Support for chain reorgs
- Indices for common query patterns
- Efficient storage using ClickHouse-specific features

Major tables include:
- `blocks`: Beacon chain blocks
- `validators`: Validator information
- `attestations`: Attestation data
- `execution_payloads`: Execution layer payloads
- `blob_sidecars`: Blob sidecar data (Deneb+)
- `rewards`: Block and attestation rewards
- And many more specialized tables

## Configuration and Specs Management

The scraper maintains a database of chain specifications retrieved from the beacon node. These specs are used for various calculations including slot timestamps and epoch boundaries.

### Automatic Specs Updates

The scraper automatically updates specs from the beacon node when starting. It will only insert changed parameters to avoid unnecessary database writes.

## License

This project is licensed under the [MIT License](LICENSE).