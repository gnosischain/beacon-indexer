# Parquet Backend Guide

The Parquet backend provides a simple, database-free alternative for beacon chain data analysis and development.

## Overview

The Parquet backend stores all data as local Parquet files, eliminating the need for database setup while providing excellent compression and compatibility with modern analytics tools.

## Features

- **No Database Setup**: Just file I/O operations
- **Portable Data**: Standard Parquet format works with pandas, DuckDB, Spark
- **Excellent Compression**: Efficient columnar storage (50-90% size reduction)
- **Local Development**: Perfect for development and testing
- **Analytics-Ready**: Direct compatibility with data science tools

## File Organization

The Parquet backend organizes data in a structured directory layout:

```
parquet_data/
├── raw_blocks/                    # Raw beacon block data
│   ├── blocks_20241201_143022.parquet
│   └── processed/                 # Processed raw files moved here
│       └── blocks_20241201_143022.parquet
├── raw_validators/                # Raw validator data
│   ├── date=2024-12-01/          # Partitioned by date
│   │   └── validators_20241201_143045.parquet
├── raw_rewards/                   # Raw reward data
├── blocks/                        # Structured block data
│   ├── date=2024-12-01/
│   │   └── blocks_20241201_143100.parquet
├── attestations/                  # Block attestations
├── validators/                    # Validator information
├── rewards/                       # Block rewards
├── genesis/                       # Genesis configuration
├── specs/                         # Chain specifications
└── time_helpers/                  # Timing parameters
```

## Configuration

### Environment Variables

```bash
# Required
STORAGE_BACKEND=parquet
BEACON_NODE_URL=http://your-beacon-node:5052

# Parquet-specific settings
PARQUET_OUTPUT_DIR=./parquet_data        # Output directory
PARQUET_COMPRESSION=snappy               # snappy, gzip, lz4
PARQUET_ROWS_PER_FILE=100000            # Rows per file (controls file size)

# Optional optimization
VALIDATOR_MODE=daily                     # Reduces data volume
START_SLOT=9000000                      # For testing with smaller ranges
END_SLOT=9100000
```

### Compression Options

| Compression | Speed | Size | Use Case |
|-------------|-------|------|----------|
| `snappy` | ⚡⚡⚡ Fast | 📦 Good | Default, balanced performance |
| `gzip` | ⚡ Slow | 📦📦 Excellent | Storage-optimized |
| `lz4` | ⚡⚡ Very Fast | 📦 Fair | Speed-optimized |

## Setup and Usage

### 1. Quick Start
```bash
# Setup environment
cp .env.example .env
echo "STORAGE_BACKEND=parquet" >> .env
echo "BEACON_NODE_URL=http://your-beacon-node:5052" >> .env

# Create directory
mkdir -p ./parquet_data

# Run pipeline
make build
make backfill
make transform
```

### 2. Docker Usage
```bash
# All services automatically create parquet_data volume
docker compose --profile backfill up
docker compose --profile transform up

# Check results
ls -la ./parquet_data/
```

### 3. Local Development
```bash
# Install dependencies
pip install pandas>=2.0.0 pyarrow>=10.0.0

# Set environment
export STORAGE_BACKEND=parquet

# Run locally
python -m src.main load backfill --start-slot 9000000 --end-slot 9001000
python -m src.main transform batch
```

## Processing Workflow

### 1. Raw Data Loading
```
Beacon API → Raw Parquet Files
- raw_blocks/blocks_timestamp.parquet
- raw_validators/validators_timestamp.parquet
- raw_rewards/rewards_timestamp.parquet
```

### 2. Transformation Process
```
Raw Files → Parser → Structured Files → Move to processed/
- Sequential processing (no workers)
- File-based progress tracking
- Simple parsers (no fork awareness)
```

### 3. Data Organization
- **Date Partitioning**: Files partitioned by `date=YYYY-MM-DD`
- **Timestamp Naming**: Files named with creation timestamp
- **Processing Tracking**: Raw files moved to `processed/` subdirectories

## Querying Parquet Data

### Using pandas (Simple Analysis)

```python
import pandas as pd

# Load all blocks
blocks = pd.read_parquet('./parquet_data/blocks/')
print(f"Loaded {len(blocks)} blocks")

# Load specific date partition
blocks_today = pd.read_parquet('./parquet_data/blocks/date=2024-12-01/')

# Basic analysis
recent_blocks = blocks[blocks['slot'] > 9000000]
proposer_stats = blocks.groupby('proposer_index').size().sort_values(ascending=False)

# Memory-efficient loading for large datasets
for chunk in pd.read_parquet('./parquet_data/blocks/', chunksize=10000):
    # Process chunk
    print(f"Processing {len(chunk)} rows")
```

### Using DuckDB (Recommended for Large Data)

```python
import duckdb

# Connect to DuckDB
con = duckdb.connect()

# Query Parquet files directly with SQL
result = con.execute("""
    SELECT 
        slot, 
        proposer_index, 
        slot_timestamp
    FROM './parquet_data/blocks/*.parquet' 
    WHERE slot > 9000000 
    ORDER BY slot DESC 
    LIMIT 10
""").fetchdf()

# Join multiple tables
combined = con.execute("""
    SELECT 
        b.slot,
        b.proposer_index,
        r.total as reward
    FROM './parquet_data/blocks/*.parquet' b
    JOIN './parquet_data/rewards/*.parquet' r ON b.slot = r.slot
    WHERE b.slot > 9000000
    ORDER BY b.slot DESC
    LIMIT 100
""").fetchdf()

# Aggregation queries
stats = con.execute("""
    SELECT 
        proposer_index,
        COUNT(*) as block_count,
        AVG(CASE WHEN r.total > 0 THEN r.total END) as avg_reward
    FROM './parquet_data/blocks/*.parquet' b
    LEFT JOIN './parquet_data/rewards/*.parquet' r ON b.slot = r.slot
    GROUP BY proposer_index
    ORDER BY block_count DESC
    LIMIT 20
""").fetchdf()

# Time-based analysis
daily_stats = con.execute("""
    SELECT 
        DATE(slot_timestamp) as date,
        COUNT(*) as blocks_per_day,
        COUNT(DISTINCT proposer_index) as unique_proposers
    FROM './parquet_data/blocks/*.parquet'
    WHERE slot_timestamp >= '2024-01-01'
    GROUP BY DATE(slot_timestamp)
    ORDER BY date
""").fetchdf()
```

### Using Polars (High Performance)

```python
import polars as pl

# Lazy evaluation for large datasets
blocks = pl.scan_parquet('./parquet_data/blocks/*.parquet')

# Complex query with lazy evaluation
result = (
    blocks
    .filter(pl.col('slot') > 9000000)
    .select(['slot', 'proposer_index', 'slot_timestamp'])
    .with_columns([
        pl.col('slot_timestamp').dt.date().alias('date')
    ])
    .group_by('date')
    .agg([
        pl.count().alias('blocks_per_day'),
        pl.col('proposer_index').n_unique().alias('unique_proposers')
    ])
    .sort('date')
    .head(30)
    .collect()  # Execute the query
)

# Streaming for very large datasets
for batch in pl.scan_parquet('./parquet_data/blocks/*.parquet').iter_slices(n_rows=50000):
    # Process batch
    print(f"Processing batch with {len(batch)} rows")
```

### Using Apache Arrow (Direct Access)

```python
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Direct parquet reading
table = pq.read_table('./parquet_data/blocks/')

# Filter with Arrow compute
filtered = table.filter(pc.greater(table['slot'], 9000000))

# Convert to pandas when needed
df = filtered.to_pandas()
```

## Data Schema

The Parquet files maintain the same schema as the ClickHouse tables:

### Blocks Table
```python
{
    'slot': 'uint64',
    'proposer_index': 'uint64',
    'parent_root': 'string',
    'state_root': 'string', 
    'signature': 'string',
    'version': 'string',
    'randao_reveal': 'string',
    'graffiti': 'string',
    'inserted_at': 'timestamp[ns]',
    'slot_timestamp': 'timestamp[ns]',
    'date': 'date32'  # Partition column
}
```

### Validators Table
```python
{
    'slot': 'uint64',
    'validator_index': 'uint32',
    'balance': 'uint64',
    'status': 'string',
    'pubkey': 'string',
    'withdrawal_credentials': 'string',
    'effective_balance': 'uint64',
    'slashed': 'uint8',
    'activation_eligibility_epoch': 'uint64',
    'activation_epoch': 'uint64',
    'exit_epoch': 'uint64',
    'withdrawable_epoch': 'uint64'
}
```

### Rewards Table
```python
{
    'slot': 'uint64',
    'proposer_index': 'uint64',
    'total': 'uint64',
    'attestations': 'uint64',
    'sync_aggregate': 'uint64',
    'proposer_slashings': 'uint64',
    'attester_slashings': 'uint64'
}
```

## Analytics Examples

### Proposer Performance Analysis

```python
import pandas as pd
import matplotlib.pyplot as plt

# Load data
blocks = pd.read_parquet('./parquet_data/blocks/')
rewards = pd.read_parquet('./parquet_data/rewards/')

# Merge blocks and rewards
df = blocks.merge(rewards, on='slot', how='left')

# Top proposers by block count
top_proposers = df.groupby('proposer_index').agg({
    'slot': 'count',
    'total': ['mean', 'sum']
}).round(2)

top_proposers.columns = ['block_count', 'avg_reward', 'total_reward']
top_proposers = top_proposers.sort_values('block_count', ascending=False).head(20)

print("Top 20 Proposers:")
print(top_proposers)

# Visualize
top_proposers['block_count'].plot(kind='bar', title='Blocks Proposed by Top Validators')
plt.show()
```

### Network Activity Analysis

```python
import duckdb
import pandas as pd

con = duckdb.connect()

# Daily network activity
daily_activity = con.execute("""
    SELECT 
        DATE(slot_timestamp) as date,
        COUNT(*) as total_blocks,
        COUNT(DISTINCT proposer_index) as active_validators,
        AVG(CASE WHEN r.total > 0 THEN r.total END) as avg_reward,
        SUM(CASE WHEN LENGTH(graffiti) > 2 THEN 1 ELSE 0 END) as blocks_with_graffiti
    FROM './parquet_data/blocks/*.parquet' b
    LEFT JOIN './parquet_data/rewards/*.parquet' r ON b.slot = r.slot
    WHERE slot_timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(slot_timestamp)
    ORDER BY date
""").fetchdf()

print(daily_activity.head())

# Plot activity over time
daily_activity.set_index('date')['total_blocks'].plot(
    title='Daily Block Production', 
    ylabel='Blocks per Day'
)
plt.show()
```

### Validator Lifecycle Analysis

```python
# Load validator data
validators = pd.read_parquet('./parquet_data/validators/')

# Get latest state per validator
latest_validators = validators.loc[validators.groupby('validator_index')['slot'].idxmax()]

# Status distribution
status_counts = latest_validators['status'].value_counts()
print("Validator Status Distribution:")
print(status_counts)

# Balance analysis
balance_stats = latest_validators.groupby('status')['balance'].describe()
print("\nBalance Statistics by Status:")
print(balance_stats)

# Activation timeline
activation_timeline = latest_validators.groupby('activation_epoch').size()
print("\nValidators Activated per Epoch (recent):")
print(activation_timeline.tail(20))
```

## Performance Optimization

### File Size Optimization

```bash
# Large files (better compression, slower queries)
PARQUET_ROWS_PER_FILE=1000000

# Small files (faster queries, less compression)
PARQUET_ROWS_PER_FILE=50000

# Balanced (recommended)
PARQUET_ROWS_PER_FILE=100000
```

### Memory Management

```python
# For large datasets, use chunked reading
chunks = []
for chunk in pd.read_parquet('./parquet_data/blocks/', chunksize=100000):
    # Process chunk
    processed_chunk = chunk[chunk['slot'] > 9000000]
    chunks.append(processed_chunk)

# Combine results
result = pd.concat(chunks, ignore_index=True)
```

### Query Performance

```python
# Use DuckDB for complex queries (much faster than pandas)
import duckdb

con = duckdb.connect()

# This is much faster than pandas equivalent
result = con.execute("""
    SELECT proposer_index, COUNT(*) 
    FROM './parquet_data/blocks/*.parquet' 
    WHERE slot > 9000000 
    GROUP BY proposer_index 
    ORDER BY COUNT(*) DESC
""").fetchdf()
```

## Monitoring and Maintenance

### Check Data Status

```bash
# File counts
find ./parquet_data -name "*.parquet" | wc -l

# Directory sizes
du -sh ./parquet_data/*

# Raw vs processed files
echo "Raw files:"
find ./parquet_data/raw_* -name "*.parquet" | wc -l
echo "Processed files:"
find ./parquet_data/raw_* -path "*/processed/*" -name "*.parquet" | wc -l

# Sample recent data
python -c "
import pandas as pd
blocks = pd.read_parquet('./parquet_data/blocks/')
print(f'Latest block: slot {blocks.slot.max()}')
print(f'Total blocks: {len(blocks)}')
print(blocks.tail())
"
```

### Data Validation

```python
import pandas as pd

# Check data integrity
blocks = pd.read_parquet('./parquet_data/blocks/')

# Basic checks
print(f"Blocks loaded: {len(blocks)}")
print(f"Slot range: {blocks.slot.min()} - {blocks.slot.max()}")
print(f"Date range: {blocks.slot_timestamp.min()} - {blocks.slot_timestamp.max()}")
print(f"Unique proposers: {blocks.proposer_index.nunique()}")

# Check for gaps
slots = blocks.slot.sort_values()
gaps = slots.diff()[slots.diff() > 1]
if len(gaps) > 0:
    print(f"Found {len(gaps)} gaps in slot sequence")
else:
    print("No gaps found in slot sequence")
```

### Cleanup and Maintenance

```bash
# Remove old processed files (optional)
find ./parquet_data -path "*/processed/*" -name "*.parquet" -mtime +7 -delete

# Compress old data (if using gzip compression)
find ./parquet_data -name "*.parquet" -mtime +30 -exec gzip {} \;

# Archive old partitions
tar -czf archive_2024_01.tar.gz ./parquet_data/*/date=2024-01-*
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   ```bash
   mkdir -p ./parquet_data
   chmod 755 ./parquet_data
   ```

2. **Memory Issues with Large Files**
   ```bash
   # Reduce file size
   PARQUET_ROWS_PER_FILE=50000
   
   # Use daily mode for validators
   VALIDATOR_MODE=daily
   ```

3. **Import Errors**
   ```bash
   pip install pandas>=2.0.0 pyarrow>=10.0.0
   ```

4. **Slow Queries**
   ```python
   # Use DuckDB instead of pandas
   import duckdb
   con = duckdb.connect()
   # Much faster for complex queries
   ```

### Performance Issues

```bash
# Check file sizes
find ./parquet_data -name "*.parquet" -exec ls -lh {} \; | head -20

# Monitor disk space
df -h ./parquet_data

# Check compression ratio
du -sh ./parquet_data
# Compare with uncompressed CSV size
```

The Parquet backend provides an excellent foundation for beacon chain data analysis with minimal setup requirements and maximum compatibility with modern data science tools.