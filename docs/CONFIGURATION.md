# Configuration

The Beacon Chain Indexer is configured through environment variables, providing flexibility for different deployment scenarios.

## Environment Variables

### Core Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `STORAGE_BACKEND` | Storage backend choice | `clickhouse` | No |
| `BEACON_NODE_URL` | Beacon node API endpoint | `http://localhost:5052` | Yes |
| `LOG_LEVEL` | Logging level | `INFO` | No |

### ClickHouse Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `CLICKHOUSE_HOST` | ClickHouse host | `localhost` | Yes* |
| `CLICKHOUSE_PORT` | ClickHouse port | `9000` | No |
| `CLICKHOUSE_USER` | ClickHouse username | `default` | No |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | `` | No |
| `CLICKHOUSE_DATABASE` | ClickHouse database | `beacon_chain` | No |
| `CLICKHOUSE_SECURE` | Use secure connection | `false` | No |

*Required only when `STORAGE_BACKEND=clickhouse`

### Parquet Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `PARQUET_OUTPUT_DIR` | Parquet files directory | `./parquet_data` | No |
| `PARQUET_COMPRESSION` | Compression algorithm | `snappy` | No |
| `PARQUET_ROWS_PER_FILE` | Rows per file | `100000` | No |

### Data Loading Configuration

| Variable | Description | Default | Options |
|----------|-------------|---------|---------|
| `ENABLED_LOADERS` | Comma-separated loader names | `blocks,validators,specs,genesis,rewards` | Any combination |
| `VALIDATOR_MODE` | Validator processing mode | `daily` | `daily`, `all_slots` |
| `START_SLOT` | Default start slot for backfill | `0` | Any integer |
| `END_SLOT` | Default end slot for backfill | `` (current head) | Any integer |

### Performance Configuration

| Variable | Description | Default | ClickHouse Only |
|----------|-------------|---------|-----------------|
| `BACKFILL_WORKERS` | Number of backfill workers | `4` | Yes |
| `CHUNK_SIZE` | Slots per chunk | `1000` | Yes |

## Configuration Examples

### Development Setup (Parquet)

```bash
# .env
STORAGE_BACKEND=parquet
BEACON_NODE_URL=http://localhost:5052
PARQUET_OUTPUT_DIR=./data
PARQUET_COMPRESSION=snappy
VALIDATOR_MODE=daily
START_SLOT=9000000
END_SLOT=9010000
LOG_LEVEL=DEBUG
```

### Production Setup (ClickHouse)

```bash
# .env
STORAGE_BACKEND=clickhouse
BEACON_NODE_URL=https://beacon-api.mainnet.com
CLICKHOUSE_HOST=clickhouse.example.com
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=indexer_user
CLICKHOUSE_PASSWORD=secure_password
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true
BACKFILL_WORKERS=8
CHUNK_SIZE=2000
VALIDATOR_MODE=all_slots
LOG_LEVEL=INFO
```

### ClickHouse Cloud Setup

```bash
# .env
STORAGE_BACKEND=clickhouse
BEACON_NODE_URL=https://beacon-api.mainnet.com
CLICKHOUSE_HOST=abc123.us-central1.gcp.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_cloud_password
CLICKHOUSE_DATABASE=default
CLICKHOUSE_SECURE=true
BACKFILL_WORKERS=4
CHUNK_SIZE=1000
```

### Gnosis Chain Setup

```bash
# .env
STORAGE_BACKEND=clickhouse
BEACON_NODE_URL=https://beacon-chain.gnosis.io
CLICKHOUSE_HOST=gnosis-clickhouse.example.com
# ... other ClickHouse settings
VALIDATOR_MODE=daily
START_SLOT=0
# Network detection is automatic - no manual configuration needed!
```

## Loader Configuration

### Available Loaders

| Loader | Purpose | Resource Usage | Notes |
|--------|---------|----------------|-------|
| `blocks` | Beacon blocks | Medium | Always recommended |
| `validators` | Validator information | High (all_slots) / Low (daily) | Mode dependent |
| `rewards` | Block rewards | Low | Optional but useful |
| `specs` | Chain specifications | Very Low | Required for foundation |
| `genesis` | Genesis information | Very Low | Required for foundation |

### Loader Combinations

```bash
# Minimal setup
ENABLED_LOADERS=blocks,specs,genesis

# Standard setup  
ENABLED_LOADERS=blocks,validators,specs,genesis,rewards

# Analysis-focused
ENABLED_LOADERS=blocks,validators,rewards,specs,genesis

# Everything (resource intensive)
ENABLED_LOADERS=blocks,validators,rewards,specs,genesis
```

## Validator Processing Modes

### Daily Mode (Recommended)

```bash
VALIDATOR_MODE=daily
```

**Behavior:**
- Processes validators only on the last slot of each day (UTC)
- Significantly reduces data volume and processing time
- Provides daily snapshots of validator state
- Recommended for most use cases

**Use Cases:**
- Daily validator analytics
- Historical trend analysis  
- Resource-constrained environments
- Long-term data storage

### All Slots Mode

```bash
VALIDATOR_MODE=all_slots
```

**Behavior:**
- Processes validators for every single slot
- Complete historical validator data
- High resource usage and storage requirements
- Detailed slot-by-slot validator state

**Use Cases:**
- Detailed validator lifecycle analysis
- High-frequency validator monitoring
- Complete historical reconstruction
- Research requiring slot-level precision

### Mode Comparison

| Aspect | Daily Mode | All Slots Mode |
|--------|------------|----------------|
| **Data Volume** | ~1/7200 of full data | Complete data |
| **Processing Time** | Fast | Slow |
| **Storage Required** | Low | High |
| **Resource Usage** | Low | High |
| **Analysis Granularity** | Daily snapshots | Slot-level detail |

## Performance Tuning

### ClickHouse Backend Tuning

#### Worker Configuration
```bash
# CPU-bound workload
BACKFILL_WORKERS=8        # 2x CPU cores
CHUNK_SIZE=1000           # Standard chunk size

# Memory-constrained
BACKFILL_WORKERS=2        # Fewer workers
CHUNK_SIZE=500            # Smaller chunks

# High-performance setup
BACKFILL_WORKERS=16       # Many workers
CHUNK_SIZE=2000           # Larger chunks
```

#### Network-Specific Tuning

```bash
# Mainnet (12s slots)
CHUNK_SIZE=1000           # ~3.3 hours per chunk
BACKFILL_WORKERS=4        # Standard

# Gnosis Chain (5s slots)  
CHUNK_SIZE=2000           # ~2.8 hours per chunk
BACKFILL_WORKERS=6        # More workers for faster slots
```

### Parquet Backend Tuning

#### File Size Optimization
```bash
# Large files (better compression, slower queries)
PARQUET_ROWS_PER_FILE=1000000

# Small files (faster queries, less compression)
PARQUET_ROWS_PER_FILE=50000

# Balanced (recommended)
PARQUET_ROWS_PER_FILE=100000
```

#### Compression Settings
```bash
# Speed-optimized
PARQUET_COMPRESSION=snappy

# Size-optimized  
PARQUET_COMPRESSION=gzip

# Balanced
PARQUET_COMPRESSION=lz4
```

## Slot Range Configuration

### Range Specification

```bash
# Specific range
START_SLOT=9000000
END_SLOT=9100000

# From specific slot to current
START_SLOT=9000000
END_SLOT=                 # Empty = current head

# Recent data only (last 1M slots)
START_SLOT=             # Will be calculated as current - 1M
END_SLOT=               # Current head

# All historical data
START_SLOT=0
END_SLOT=               # Current head
```

### Common Range Examples

```bash
# Last week of mainnet data (~50K slots)
START_SLOT=9700000
END_SLOT=9750000

# Specific epoch range (epoch 300000-300100)
START_SLOT=9600000      # 300000 * 32
END_SLOT=9603200        # 300100 * 32

# Deneb fork activation period
START_SLOT=8626176      # Around Deneb activation
END_SLOT=8636176        # 10K slots after

# Testing range (100 slots)
START_SLOT=9000000
END_SLOT=9000100
```

## Logging Configuration

### Log Levels

| Level | Usage | Output Volume |
|-------|-------|---------------|
| `DEBUG` | Development, troubleshooting | Very High |
| `INFO` | Production monitoring | Medium |
| `WARNING` | Error tracking | Low |
| `ERROR` | Critical issues only | Very Low |

### Example Log Outputs

#### INFO Level (Recommended)
```
2024-12-01 16:40:25 [INFO ] Using Parquet storage backend
2024-12-01 16:40:25 [INFO ] Loader service initialized | loaders=['blocks', 'validators'] storage_backend=parquet
2024-12-01 16:40:25 [INFO ] Processing slot batch | start=0 end=999 total_progress=0/10000
2024-12-01 16:40:25 [INFO ] Batch completed | loader=blocks success_count=1000 slots=1000
```

#### DEBUG Level (Development)
```
2024-12-01 16:40:25 [DEBUG] Attempting to claim chunk for loader | worker=worker_1 loader=blocks
2024-12-01 16:40:25 [DEBUG] Executing statement | statement=CREATE TABLE IF NOT EXISTS raw_blocks...
2024-12-01 16:40:25 [DEBUG] Cached timing from time_helpers | genesis_time=1638993340 seconds_per_slot=5
```

## Network-Specific Configuration

The indexer automatically detects network parameters, but you can optimize for specific networks:

### Mainnet Optimization
```bash
BEACON_NODE_URL=https://mainnet-beacon-api.com
CHUNK_SIZE=1000           # 12s * 1000 = ~3.3 hours
BACKFILL_WORKERS=4
VALIDATOR_MODE=daily      # Efficient for mainnet scale
```

### Gnosis Chain Optimization  
```bash
BEACON_NODE_URL=https://beacon-chain.gnosis.io
CHUNK_SIZE=2000           # 5s * 2000 = ~2.8 hours  
BACKFILL_WORKERS=6        # More workers for faster blocks
VALIDATOR_MODE=daily      # Handle large validator set efficiently
```

### Testnet Configuration
```bash
# Holesky
BEACON_NODE_URL=https://holesky-beacon-api.com
CHUNK_SIZE=500            # Smaller chunks for testing
BACKFILL_WORKERS=2
VALIDATOR_MODE=all_slots  # Complete data for testing

# Sepolia
BEACON_NODE_URL=https://sepolia-beacon-api.com
CHUNK_SIZE=500
BACKFILL_WORKERS=2
VALIDATOR_MODE=all_slots
```

## Docker Configuration

### docker-compose.yml Environment

```yaml
services:
  backfill:
    environment:
      - STORAGE_BACKEND=${STORAGE_BACKEND}
      - BEACON_NODE_URL=${BEACON_NODE_URL}
      - CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
      - PARQUET_OUTPUT_DIR=/app/parquet_data
      - ENABLED_LOADERS=${ENABLED_LOADERS}
      - VALIDATOR_MODE=${VALIDATOR_MODE}
      - START_SLOT=${START_SLOT}
      - END_SLOT=${END_SLOT}
      - LOG_LEVEL=${LOG_LEVEL}
```

### Environment File Precedence

1. **docker-compose.yml**: Hardcoded values (highest priority)
2. **.env file**: Default values for docker-compose
3. **System environment**: Shell environment variables
4. **Application defaults**: Fallback values in code

## Configuration Validation

### Required Validation

The application validates configuration on startup:

```python
# Storage backend validation
if STORAGE_BACKEND not in ["clickhouse", "parquet"]:
    raise ValueError("STORAGE_BACKEND must be 'clickhouse' or 'parquet'")

# ClickHouse validation
if STORAGE_BACKEND == "clickhouse" and not CLICKHOUSE_HOST:
    raise ValueError("CLICKHOUSE_HOST required for ClickHouse backend")

# Slot range validation  
if START_SLOT and END_SLOT and START_SLOT >= END_SLOT:
    raise ValueError("START_SLOT must be less than END_SLOT")
```

### Runtime Validation

```bash
# Test beacon node connectivity
curl -s "${BEACON_NODE_URL}/eth/v1/node/health" || echo "Beacon node unreachable"

# Test ClickHouse connectivity (if using ClickHouse)
echo "SELECT 1" | clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" || echo "ClickHouse unreachable"

# Check parquet directory (if using Parquet)
ls -la "$PARQUET_OUTPUT_DIR" || echo "Parquet directory not accessible"
```

## Best Practices

### Development Configuration
```bash
# Use Parquet for simplicity
STORAGE_BACKEND=parquet
PARQUET_COMPRESSION=snappy
VALIDATOR_MODE=daily
# Use small slot ranges for testing
START_SLOT=9000000
END_SLOT=9001000
LOG_LEVEL=DEBUG
```

### Production Configuration
```bash
# Use ClickHouse for performance
STORAGE_BACKEND=clickhouse
CLICKHOUSE_SECURE=true
# Scale workers based on available resources
BACKFILL_WORKERS=8
CHUNK_SIZE=2000
VALIDATOR_MODE=all_slots
LOG_LEVEL=INFO
```

### Resource-Constrained Configuration
```bash
# Minimize resource usage
VALIDATOR_MODE=daily
BACKFILL_WORKERS=2
CHUNK_SIZE=500
PARQUET_COMPRESSION=gzip
ENABLED_LOADERS=blocks,specs,genesis
```