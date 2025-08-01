# Storage Backends

The Beacon Chain Indexer supports two storage backends, each optimized for different use cases.

## Backend Comparison

| Feature | Parquet | ClickHouse |
|---------|---------|------------|
| **Setup Complexity** | ✅ None (just files) | ❌ Database required |
| **Query Performance** | ⚡ Good (with DuckDB) | ⚡⚡ Excellent |
| **Real-time Queries** | ❌ File-based | ✅ Live SQL queries |
| **Concurrent Access** | ❌ Limited | ✅ Unlimited |
| **Data Portability** | ✅ Standard format | ❌ Database-specific |
| **Analytics Tools** | ✅ pandas, Spark, DuckDB | ✅ Any SQL client |
| **Storage Efficiency** | ✅ Excellent compression | ✅ Good compression |
| **Development** | ✅ Simple file operations | ❌ Database complexity |
| **Production Scale** | ⚠️ Limited | ✅ Unlimited |
| **Fork Awareness** | ❌ Simple parsing only | ✅ Full fork-aware processing |
| **Worker Distribution** | ❌ Sequential processing | ✅ Multi-worker chunks |
| **Progress Tracking** | ❌ File-based only | ✅ Database-tracked progress |

## When to Choose Each Backend

### Choose Parquet When:
- ✅ **Data Analysis**: Working with pandas, Jupyter notebooks
- ✅ **Development**: No database setup required
- ✅ **ETL Pipelines**: Need portable, standard format
- ✅ **Archival Storage**: Long-term data retention
- ✅ **Quick Testing**: Rapid prototyping and experimentation
- ✅ **Offline Analytics**: Processing data without live queries
- ✅ **Small to Medium Scale**: Up to millions of records

### Choose ClickHouse When:
- ✅ **Production Systems**: High-availability requirements
- ✅ **Real-time Analytics**: Live dashboards and monitoring
- ✅ **Complex Queries**: Joins, aggregations, time-series analysis
- ✅ **High Throughput**: Millions of records per day
- ✅ **Concurrent Users**: Multiple analysts accessing data
- ✅ **Fork Analysis**: Need detailed fork-specific processing
- ✅ **Large Scale**: Billions of records, years of data

## Architecture Differences

### Parquet Backend Architecture
```
Beacon API → Raw Parquet Files → Simple Transform → Structured Parquet Files
                ↓
           Sequential Processing
           No Chunk Management
           File-based Progress
```

**Characteristics:**
- **Simple Pipeline**: Linear processing without worker coordination
- **File-based Storage**: Each table stored as partitioned Parquet files
- **No Fork Awareness**: Uses basic parsers for all data
- **Sequential Transform**: Processes files one by one
- **Local Progress**: Tracks processing by moving files to `processed/` directories

### ClickHouse Backend Architecture
```
Beacon API → Raw ClickHouse Tables → Fork-Aware Transform → Structured ClickHouse Tables
                ↓                           ↓
           Chunk Management              Fork Detection
           Multi-worker Processing       Parser Factory
           Database Progress Tracking    Range-based Processing
```

**Characteristics:**
- **Complex Pipeline**: Distributed processing with worker coordination
- **Database Storage**: All data in ClickHouse tables with proper schemas
- **Full Fork Awareness**: Automatic fork detection and appropriate parsing
- **Parallel Transform**: Multiple workers process chunks simultaneously
- **Database Progress**: Detailed progress tracking in dedicated tables

## Configuration

### Parquet Backend
```bash
# .env configuration
STORAGE_BACKEND=parquet
PARQUET_OUTPUT_DIR=./parquet_data
PARQUET_COMPRESSION=snappy         # snappy, gzip, lz4
PARQUET_ROWS_PER_FILE=100000       # Controls file size

# Optional: Reduce memory usage
VALIDATOR_MODE=daily               # Process only daily snapshots
```

### ClickHouse Backend
```bash
# .env configuration
STORAGE_BACKEND=clickhouse
CLICKHOUSE_HOST=your-host
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=false

# Optional: Tune performance
BACKFILL_WORKERS=4                 # Parallel workers
CHUNK_SIZE=1000                    # Slots per chunk
```

## Performance Characteristics

### Parquet Performance
- **Write Speed**: ⚡ Fast (direct file writes)
- **Read Speed**: ⚡ Good (columnar format)
- **Query Speed**: ⚡ Good with DuckDB, slower with pandas
- **Compression**: ⚡⚡ Excellent (50-90% size reduction)
- **Scalability**: ⚠️ Limited by filesystem and memory
- **Concurrency**: ❌ Single writer, multiple readers with coordination

### ClickHouse Performance
- **Write Speed**: ⚡⚡ Very fast (optimized for analytics)
- **Read Speed**: ⚡⚡⚡ Excellent (native SQL engine)
- **Query Speed**: ⚡⚡⚡ Exceptional (parallel query execution)
- **Compression**: ⚡ Good (built-in compression algorithms)
- **Scalability**: ⚡⚡⚡ Excellent (cluster support)
- **Concurrency**: ⚡⚡⚡ Full concurrent read/write support

## Data Processing Differences

### Parquet Processing Flow
1. **Raw Data Loading**: Beacon API → `raw_blocks/`, `raw_validators/` directories
2. **Simple Processing**: Sequential file processing without fork detection
3. **Basic Parsing**: Uses simple parsers (blocks, validators, rewards only)
4. **File Movement**: Processed files moved to `processed/` subdirectories
5. **No State Management**: Progress tracked by file locations only

### ClickHouse Processing Flow
1. **Raw Data Loading**: Beacon API → `raw_blocks`, `raw_validators` tables
2. **Chunk Management**: Work distributed across multiple workers
3. **Fork Detection**: Automatic network and fork detection per slot
4. **Fork-Aware Parsing**: Uses appropriate parser for each fork
5. **State Tracking**: Detailed progress tracking in database tables
6. **Range Processing**: Gap-aware transformation with retry logic

## Migration Between Backends

### From Parquet to ClickHouse
```bash
# 1. Stop current services
make clean

# 2. Change backend in .env
sed -i 's/STORAGE_BACKEND=parquet/STORAGE_BACKEND=clickhouse/' .env

# 3. Set up ClickHouse configuration
echo "CLICKHOUSE_HOST=your-host" >> .env

# 4. Run migration and reload data
make build
make migration
make backfill
make transform
```

### From ClickHouse to Parquet
```bash
# 1. Stop current services
make clean

# 2. Change backend in .env
sed -i 's/STORAGE_BACKEND=clickhouse/STORAGE_BACKEND=parquet/' .env

# 3. Clean up and reload (or export existing data first)
rm -rf ./parquet_data
make build
make backfill
make transform
```

### Exporting Data Between Backends

#### Export ClickHouse to Parquet
```python
import clickhouse_connect
import pandas as pd

# Connect to ClickHouse
client = clickhouse_connect.get_client(host='your-host')

# Export table
df = client.query_df("SELECT * FROM blocks WHERE slot >= 9000000")
df.to_parquet('./exported_blocks.parquet', compression='snappy')
```

#### Import Parquet to ClickHouse
```python
import pandas as pd
import clickhouse_connect

# Read Parquet
df = pd.read_parquet('./parquet_data/blocks/')

# Connect and insert
client = clickhouse_connect.get_client(host='your-host')
client.insert_df('blocks', df)
```

## Monitoring and Troubleshooting

### Parquet Monitoring
```bash
# File counts
find ./parquet_data -name "*.parquet" | wc -l

# Directory sizes
du -sh ./parquet_data/*

# Processing status (check for processed files)
find ./parquet_data -name "processed" -type d | wc -l

# Sample data
python -c "import pandas as pd; print(pd.read_parquet('./parquet_data/blocks/').head())"
```

### ClickHouse Monitoring
```bash
# Chunk progress
python scripts/chunks.py overview

# Transformer status
python scripts/transformer_status.py

# Database queries
echo "SELECT COUNT(*) FROM blocks" | clickhouse-client
```

## Best Practices

### Parquet Best Practices
- **File Size**: Keep files 100MB-1GB for optimal performance
- **Partitioning**: Use date-based partitioning for time-series queries
- **Compression**: Use `snappy` for speed, `gzip` for storage efficiency
- **Memory Management**: Use `VALIDATOR_MODE=daily` for large datasets
- **Query Tools**: Use DuckDB for complex queries, pandas for simple analysis

### ClickHouse Best Practices
- **Workers**: Scale `BACKFILL_WORKERS` based on available CPU cores
- **Chunks**: Adjust `CHUNK_SIZE` based on available memory
- **Indexes**: Leverage ClickHouse's automatic indexing
- **Partitioning**: Use built-in date partitioning for large tables
- **Monitoring**: Set up proper monitoring for production deployments

## Summary

Choose your storage backend based on your specific requirements:

- **Parquet** for data analysis, development, and scenarios where simplicity and portability matter most
- **ClickHouse** for production systems, real-time analytics, and scenarios requiring high performance and concurrent access

Both backends use the same loaders and CLI commands, making it easy to switch between them as your needs evolve.