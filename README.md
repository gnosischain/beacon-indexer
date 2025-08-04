# Beacon Chain Indexer

![Beacon Indexer](img/header-beacon_indexer.png)

A simple, minimalistic beacon chain indexer using the ELT (Extract, Load, Transform) pattern with **fork-aware parsing**, **automatic network detection**, and **dual storage backends**. This indexer loads raw data from the beacon API first, then transforms it into structured tables with automatic fork detection and appropriate parsing for each Ethereum consensus layer upgrade.

## Storage Backends

Choose between two storage backends based on your needs:

### 🗄️ ClickHouse Backend (Production)
- **Real-time SQL queries** with high performance analytics
- **Concurrent access** and complex joins
- **Fork-aware processing** with chunk-based worker distribution
- **Production-ready** with monitoring and progress tracking
- **Best for**: Production systems, real-time analytics, complex queries

### 📊 Parquet Backend (Analytics)
- **Local file storage** with no database setup required
- **Portable data files** compatible with pandas, DuckDB, Spark
- **Excellent compression** and columnar format for analytics
- **Simple deployment** - just file I/O operations
- **Best for**: Data analysis, ETL pipelines, archival storage, development

```bash
# Choose your backend in .env
STORAGE_BACKEND=clickhouse  # or parquet
```

## Quick Start

### 1. Setup Environment
```bash
cp .env.example .env
# Edit .env with your beacon node URL and storage backend
```

### 2. Choose Storage Backend

#### Option A: ClickHouse (Production)
```bash
# In .env
STORAGE_BACKEND=clickhouse
CLICKHOUSE_HOST=your-clickhouse-host

# Run database migration
make migration
```

#### Option B: Parquet (Analytics/Development)
```bash
# In .env  
STORAGE_BACKEND=parquet
PARQUET_OUTPUT_DIR=./parquet_data

# No migration needed!
```

### 3. Run the Pipeline
```bash
# Build image
make build

# Load historical data
make backfill

# Transform raw data
make transform

# Check progress
make status
```

## The 4 Essential Operations

| Command | Purpose | Description |
|---------|---------|-------------|
| `make migration` | Setup | Database schema (ClickHouse only) |
| `make backfill` | Load | Historical raw data from beacon API |
| `make transform` | Process | Raw data into structured tables |
| `make realtime` | Sync | Continuous loading of new blocks |

## Key Features

- **Dual Storage Backend**: ClickHouse or Parquet with single environment variable
- **Dynamic Network Detection**: Auto-detects mainnet, Gnosis Chain, Holesky, Sepolia
- **Fork-Aware Architecture**: Handles all consensus forks (Phase 0 → Electra)
- **ELT Pattern**: Separate raw loading and transformation stages
- **Modular Design**: Easy to extend with new loaders and parsers
- **Zero Configuration**: Network and fork detection from beacon chain data

## Documentation

| Topic | File | Description |
|-------|------|-------------|
| **Storage Backends** | [docs/storage-backends.md](docs/storage-backends.md) | Detailed comparison and selection guide |
| **ClickHouse Guide** | [docs/clickhouse-guide.md](docs/clickhouse-guide.md) | Production setup, fork-aware processing |
| **Parquet Guide** | [docs/parquet-guide.md](docs/parquet-guide.md) | Analytics setup, querying examples |
| **Architecture** | [docs/architecture.md](docs/architecture.md) | System design, forks, network detection |
| **Configuration** | [docs/configuration.md](docs/configuration.md) | Environment variables, processing modes |
| **Development** | [docs/development.md](docs/development.md) | Adding endpoints, extending functionality |
| **Deployment** | [docs/deployment.md](docs/deployment.md) | Docker, monitoring, troubleshooting |

## Supported Networks & Forks

| Network | Detection | Timing | Forks Supported |
|---------|-----------|---------|-----------------|
| **Mainnet** | Auto | 12s slots, 32/epoch | Phase 0 → Electra |
| **Gnosis Chain** | Auto | 5s slots, 16/epoch | Phase 0 → Electra |
| **Holesky** | Auto | 12s slots, 32/epoch | Phase 0 → Electra |
| **Sepolia** | Auto | 12s slots, 32/epoch | Phase 0 → Electra |

## Quick Examples

### Parquet Analytics Workflow
```python
import pandas as pd
import duckdb

# Load blocks data
blocks = pd.read_parquet('./parquet_data/blocks/')

# Query with DuckDB
con = duckdb.connect()
result = con.execute("""
    SELECT slot, proposer_index 
    FROM './parquet_data/blocks/*.parquet' 
    WHERE slot > 9000000 
    ORDER BY slot DESC LIMIT 10
""").fetchdf()
```

### ClickHouse Production Workflow
```sql
-- Real-time queries
SELECT slot, proposer_index 
FROM blocks 
WHERE slot > 9000000 
ORDER BY slot DESC LIMIT 10;

-- Fork-aware analysis
SELECT fork, COUNT(*) as blocks
FROM blocks 
GROUP BY version as fork;
```

## License

This project is licensed under the [MIT License](LICENSE)

---

📖 **For detailed documentation, see the [docs/](docs/) directory**