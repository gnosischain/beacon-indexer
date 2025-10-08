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


## Maintenance System Usage Guide

The maintenance system provides powerful tools to fix failed chunks, check data integrity, and maintain your beacon chain indexer.

### Quick Start

#### 1. Check System Status
```bash
# Get overall system health
make maintenance-status

# Or directly:
docker compose run --rm backfill python scripts/maintenance.py summary
```

#### 2. Find and Fix Issues
```bash
# Check for issues in a specific range
make maintenance-check
# Enter start slot: 9000000
# Enter end slot: 9100000

# Fix failed chunks
make maintenance-fix  
# Enter start slot: 9000000
# Enter end slot: 9100000
# Enter loaders: blocks,validators (or press enter for all)
# Force reprocess: N
```

#### 3. Quick Maintenance
```bash
# Show recommendations
make maintenance-quick-check

# Fix recent failures (last week)
make maintenance-fix-recent
```

### Detailed Commands

#### Analysis Commands

```bash
# Show failed chunks with details
make maintenance-failed

# Analyze data gaps in raw tables
make maintenance-gaps

# Check transformation status
docker compose run --rm backfill python scripts/maintenance.py status
```

#### Maintenance Commands

```bash
# 1. Integrity Check
python -m src.main maintain check --start-slot 9000000 --end-slot 9100000 --detailed

# 2. Fix Failed Chunks (normal mode - only failed chunks)
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000

# 3. Fix Specific Loaders
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000 --loaders blocks,validators

# 4. Force Mode (reprocess even successful chunks)
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000 --force

# 5. Dry Run (preview what would be fixed)
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000 --dry-run

# 6. Reset Chunk Status
python -m src.main maintain reset --start-slot 9000000 --end-slot 9100000 --status failed
```

#### Docker Usage

```bash
# Using maintenance profile
docker compose --profile maintenance run --rm maintenance python -m src.main maintain check --start-slot 9000000 --end-slot 9100000

# Using backfill service
docker compose run --rm backfill python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000
```

### Common Scenarios

#### Scenario 1: Failed Chunks After Network Issues

**Problem**: Some chunks failed during loading due to network connectivity issues.

**Solution**:
```bash
# 1. Check what failed
make maintenance-failed

# 2. Fix failed chunks in affected range
python -m src.main maintain fix --start-slot 9050000 --end-slot 9060000

# 3. Verify fix
python -m src.main maintain check --start-slot 9050000 --end-slot 9060000
```

#### Scenario 2: Missing Data (Gaps)

**Problem**: Some slots are missing from raw data tables.

**Solution**:
```bash
# 1. Analyze gaps
make maintenance-gaps

# 2. Force reprocess the range with gaps
python -m src.main maintain fix --start-slot 9040000 --end-slot 9050000 --force

# 3. Check if gaps are filled
make maintenance-gaps
```

#### Scenario 3: Stuck Workers

**Problem**: Workers got stuck and chunks are in 'claimed' status for hours.

**Solution**:
```bash
# 1. Check for stuck chunks
docker compose run --rm backfill python scripts/maintenance.py recommendations

# 2. Reset stuck chunks to pending
python -m src.main maintain reset --start-slot 0 --end-slot 999999999 --status claimed

# 3. Restart backfill workers
make backfill
```

#### Scenario 4: Transformation Issues

**Problem**: Raw data loaded but transformation failed.

**Solution**:
```bash
# 1. Check transformation status
docker compose run --rm backfill python scripts/maintenance.py status

# 2. Fix failed ranges (this will re-run transformation)
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000

# 3. Or manually run transformer
python -m src.main transform batch
```

#### Scenario 5: Complete Range Reprocessing

**Problem**: Need to completely reprocess a range due to data corruption.

**Solution**:
```bash
# 1. Dry run to see what will happen
python -m src.main maintain fix --start-slot 9000000 --end-slot 9010000 --force --dry-run

# 2. Force reprocess everything in range
python -m src.main maintain fix --start-slot 9000000 --end-slot 9010000 --force

# This will:
# - Delete all raw and transformed data in range
# - Reset chunks to pending
# - Reload from beacon API
# - Re-transform the data
```

### Command Reference

#### `maintain check`
Checks data integrity without making changes.

**Options**:
- `--start-slot`: Start of slot range
- `--end-slot`: End of slot range  
- `--detailed`: Show detailed gap analysis

**Example**:
```bash
python -m src.main maintain check --start-slot 9000000 --end-slot 9100000 --detailed
```

#### `maintain fix`
Fixes failed chunks by cleaning up data and reprocessing.

**Options**:
- `--start-slot`: Start of slot range
- `--end-slot`: End of slot range
- `--loaders`: Comma-separated list of loaders (blocks,validators,rewards)
- `--force`: Reprocess even if chunks are not failed
- `--dry-run`: Show what would be fixed without doing it

**Example**:
```bash
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000 --loaders blocks --force
```

#### `maintain reset`
Resets chunk status from one state to pending.

**Options**:
- `--start-slot`: Start of slot range
- `--end-slot`: End of slot range
- `--loaders`: Comma-separated list of loaders
- `--status`: Current status to reset (failed, claimed, completed)

**Example**:
```bash
python -m src.main maintain reset --start-slot 9000000 --end-slot 9100000 --status claimed
```

### Maintenance Scripts

#### `scripts/maintenance.py`
Utility script for analysis and monitoring.

**Commands**:
- `failed`: Show failed chunks
- `gaps`: Analyze data gaps
- `status`: Show transformation status
- `summary`: System overview
- `recommendations`: Maintenance recommendations

**Examples**:
```bash
python scripts/maintenance.py summary
python scripts/maintenance.py recommendations
python scripts/maintenance.py failed
```

### Safety Features

#### 1. Dry Run Mode
Always test with `--dry-run` first to see what would happen:
```bash
python -m src.main maintain fix --start-slot 9000000 --end-slot 9100000 --dry-run
```

#### 2. Scope Limiting
- Only operates on specified slot ranges
- Can be limited to specific loaders
- Respects ENABLED_LOADERS configuration

#### 3. Data Integrity
- Uses database transactions where possible
- Logs all operations for audit trail
- Verifies data before and after operations

### Monitoring Integration

#### Continuous Monitoring
Set up regular checks:
```bash
# Daily health check
0 6 * * * cd /path/to/beacon-indexer && make maintenance-quick-check

# Weekly comprehensive check  
0 2 * * 0 cd /path/to/beacon-indexer && docker compose run --rm backfill python -m src.main maintain check --start-slot 8000000 --end-slot 999999999 > weekly_check.log
```

#### Alerting
Monitor the output of maintenance scripts and alert on:
- High number of failed chunks
- Large data gaps
- Untransformed data accumulating

### Troubleshooting

#### Issue: "Maintenance command only works with ClickHouse backend"
**Solution**: Maintenance commands require ClickHouse. Set `STORAGE_BACKEND=clickhouse` in your `.env`.

#### Issue: Permission errors during data cleanup
**Solution**: Ensure the container has proper permissions and the database user has DELETE privileges.

#### Issue: Out of memory during large fixes
**Solution**: Process smaller ranges or reduce CHUNK_SIZE in configuration.

#### Issue: Fix command hangs
**Solution**: Check beacon node connectivity and increase timeouts if needed.

## Best Practices

1. **Always check before fixing**: Use `maintain check` first
2. **Start small**: Test with small slot ranges first
3. **Use dry run**: Always dry run first for large operations
4. **Monitor during maintenance**: Watch logs during fix operations
5. **Backup critical data**: Consider backing up important ranges before major fixes
6. **Schedule maintenance**: Run during low-activity periods
7. **Document issues**: Keep track of what ranges needed fixing and why

## License

This project is licensed under the [MIT License](LICENSE)

---

📖 **For detailed documentation, see the [docs/](docs/) directory**