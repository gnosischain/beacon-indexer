# ClickHouse Backend Guide

The ClickHouse backend provides a production-ready database solution with real-time SQL queries, fork-aware processing, and high-performance analytics capabilities.

## Overview

The ClickHouse backend stores all data in a ClickHouse database, providing full SQL query capabilities, concurrent access, and sophisticated fork-aware processing with automatic network detection.

## Features

- **Real-time SQL Queries**: Full SQL support with high performance
- **Fork-Aware Processing**: Automatic fork detection and appropriate parsing
- **Concurrent Access**: Multiple users can query data simultaneously
- **Chunk-Based Processing**: Distributed workers for efficient backfill
- **Progress Tracking**: Detailed monitoring and status reporting
- **Production Ready**: Designed for high-availability deployments

## Architecture

### Fork-Aware Processing Flow
```
Beacon API → Raw ClickHouse Tables → Fork Detection → Parser Factory → Structured Tables
                ↓                         ↓              ↓
           Chunk Management          Network Detection   Fork-Specific Parsers
           Multi-worker Processing   Version Analysis    Range-based Transform
           Database Progress         Genesis/Specs       Gap-aware Processing
```

### Key Components

1. **ForkDetectionService**: Automatically detects network and active fork
2. **ParserFactory**: Creates appropriate parser for each fork
3. **Fork-Specific Parsers**: Extract data according to fork specifications
4. **Chunk Management**: Distributes work across multiple workers
5. **Progress Tracking**: Database-backed progress monitoring

## Configuration

### Environment Variables

```bash
# Required
STORAGE_BACKEND=clickhouse
BEACON_NODE_URL=http://your-beacon-node:5052

# ClickHouse connection
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=9000                    # 8123 for HTTP, 9000 for native
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=false                 # true for TLS

# Performance tuning
BACKFILL_WORKERS=4                      # Parallel workers
CHUNK_SIZE=1000                         # Slots per chunk
VALIDATOR_MODE=daily                    # daily or all_slots

# Processing ranges
START_SLOT=0
END_SLOT=                              # Empty = current head
```

### ClickHouse Setup

#### Option 1: ClickHouse Cloud
```bash
# Get connection details from ClickHouse Cloud console
CLICKHOUSE_HOST=your-cluster.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_SECURE=true
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-cloud-password
```

#### Option 2: Self-Hosted ClickHouse
```bash
# Docker setup
docker run -d --name clickhouse-server \
  -p 8123:8123 -p 9000:9000 \
  -v clickhouse_data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:latest

# Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_SECURE=false
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

## Setup and Usage

### 1. Database Migration
```bash
# Set up all database tables and schemas
make migration

# Verify migration
echo "SELECT name FROM system.tables WHERE database = 'beacon_chain'" | clickhouse-client
```

### 2. Load Historical Data
```bash
# Full backfill (use START_SLOT/END_SLOT in .env)
make backfill

# Custom range
python -m src.main load backfill --start-slot 9000000 --end-slot 9100000

# Monitor progress
make status
python scripts/chunks.py overview
```

### 3. Transform Data
```bash
# Batch mode - process all available data and exit
make transform

# Continuous mode - keep processing new data
make transform-continuous

# Monitor transformation progress
python scripts/transformer_status.py
```

### 4. Real-time Sync
```bash
# Start continuous loading of new blocks
make realtime

# Check logs
make logs
```

## Fork-Aware Processing

### Automatic Fork Detection

The system automatically detects your network and active forks:

```bash
# Check detected network and forks
python -m src.main fork list

# Example output:
# Detected network: mainnet (auto-detected)
# PHASE0      | Epoch: 0       | Version: 0x00000000
# ALTAIR      | Epoch: 74240   | Version: 0x01000000
# BELLATRIX   | Epoch: 144896  | Version: 0x02000000
# CAPELLA     | Epoch: 194048  | Version: 0x03000000
# DENEB       | Epoch: 269568  | Version: 0x04000000
# ELECTRA     | Epoch: 364032  | Version: 0x05000000
```

### Fork-Specific Processing

Each fork uses specialized parsers that extract the appropriate data:

```python
# Example: Deneb parser extracts blob commitments
class DenebParser(CapellaParser):
    def get_supported_tables(self):
        return super().get_supported_tables() + ["blob_sidecars", "blob_commitments"]
    
    def parse_fork_specific(self, slot, data):
        # Extract blob-specific data
        blob_kzg_commitments = body.get("blob_kzg_commitments", [])
        # ... processing logic
```

### Supported Forks and Tables

| Fork | Mainnet Epoch | Gnosis Epoch | New Tables | Key Features |
|------|---------------|---------------|------------|--------------|
| **Phase 0** | 0 | 0 | `blocks`, `attestations`, `deposits`, `voluntary_exits`, `proposer_slashings`, `attester_slashings` | Basic consensus |
| **Altair** | 74240 | 512 | `sync_aggregates`, `sync_committees` | Sync committees |
| **Bellatrix** | 144896 | 385536 | `execution_payloads`, `transactions` | The Merge |
| **Capella** | 194048 | 648704 | `withdrawals`, `bls_changes` | Withdrawals |
| **Deneb** | 269568 | 889856 | `blob_sidecars`, `blob_commitments` | Blob transactions |
| **Electra** | 364032 | 1337856 | `execution_requests` | Execution requests |

## Database Schema

### Raw Data Tables (Fork-Agnostic)
```sql
-- Raw beacon blocks with payload hash for fork protection
CREATE TABLE raw_blocks (
    slot UInt64,
    block_root String,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(...)
) ENGINE = ReplacingMergeTree()
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);
```

### Structured Tables (Fork-Aware)

#### Blocks Table (All Forks)
```sql
CREATE TABLE blocks (
    slot UInt64,
    proposer_index UInt64,
    parent_root String DEFAULT '',
    state_root String DEFAULT '',
    signature String DEFAULT '',
    version String DEFAULT '',              -- Fork version
    
    -- Phase 0 fields
    randao_reveal String DEFAULT '',
    graffiti String DEFAULT '',
    
    -- Altair fields
    sync_aggregate_participation UInt64 DEFAULT 0,
    sync_aggregate_signature String DEFAULT '',
    
    -- Bellatrix fields  
    execution_payload_block_hash String DEFAULT '',
    execution_payload_fee_recipient String DEFAULT '',
    
    -- Capella fields
    withdrawals_count UInt32 DEFAULT 0,
    
    -- Deneb fields
    blob_kzg_commitments_count UInt32 DEFAULT 0,
    
    -- Electra fields
    execution_requests_count UInt32 DEFAULT 0,
    
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(...),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY slot
PARTITION BY toStartOfMonth(slot_timestamp);
```

### State Management Tables
```sql
-- Chunk tracking for distributed processing
CREATE TABLE load_state_chunks (
    chunk_id String,
    start_slot UInt64,
    end_slot UInt64,
    loader_name String,
    status Enum8('pending' = 1, 'claimed' = 2, 'completed' = 3, 'failed' = 4),
    worker_id String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY chunk_id;

-- Transformation progress tracking
CREATE TABLE transformer_progress (
    raw_table_name String,
    start_slot UInt64,
    end_slot UInt64,
    status Enum8('processing' = 1, 'completed' = 2, 'failed' = 3),
    processed_count UInt64 DEFAULT 0,
    failed_count UInt64 DEFAULT 0,
    error_message String DEFAULT '',
    processed_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (raw_table_name, start_slot);
```

## Querying Data

### Basic Queries

```sql
-- Recent blocks
SELECT slot, proposer_index, version, slot_timestamp 
FROM blocks 
WHERE slot > 9000000 
ORDER BY slot DESC 
LIMIT 10;

-- Fork distribution
SELECT version, COUNT(*) as block_count
FROM blocks 
GROUP BY version 
ORDER BY block_count DESC;

-- Top proposers
SELECT proposer_index, COUNT(*) as blocks_proposed
FROM blocks 
WHERE slot_timestamp >= now() - INTERVAL 7 DAY
GROUP BY proposer_index 
ORDER BY blocks_proposed DESC 
LIMIT 20;
```

### Fork-Specific Queries

```sql
-- Deneb blob transactions
SELECT 
    b.slot,
    b.proposer_index,
    b.blob_kzg_commitments_count,
    COUNT(bc.commitment) as actual_commitments
FROM blocks b
LEFT JOIN blob_commitments bc ON b.slot = bc.slot
WHERE b.version = '0x04000000'  -- Deneb
  AND b.blob_kzg_commitments_count > 0
GROUP BY b.slot, b.proposer_index, b.blob_kzg_commitments_count
ORDER BY b.slot DESC
LIMIT 20;

-- Capella withdrawals analysis
SELECT 
    DATE(slot_timestamp) as date,
    COUNT(DISTINCT w.withdrawal_index) as total_withdrawals,
    SUM(w.amount) as total_amount,
    AVG(w.amount) as avg_amount
FROM blocks b
JOIN withdrawals w ON b.slot = w.slot  
WHERE b.version = '0x03000000'  -- Capella+
GROUP BY DATE(slot_timestamp)
ORDER BY date DESC
LIMIT 30;

-- Bellatrix execution payload analysis
SELECT 
    DATE(slot_timestamp) as date,
    COUNT(*) as blocks_with_execution,
    AVG(ep.gas_used) as avg_gas_used,
    AVG(ep.transactions_count) as avg_tx_count
FROM blocks b
JOIN execution_payloads ep ON b.slot = ep.slot
WHERE b.version >= '0x02000000'  -- Bellatrix+
GROUP BY DATE(slot_timestamp)
ORDER BY date DESC;
```

### Advanced Analytics

```sql
-- Validator performance across forks
SELECT 
    proposer_index,
    version,
    COUNT(*) as blocks,
    AVG(r.total) as avg_reward
FROM blocks b
LEFT JOIN rewards r ON b.slot = r.slot
GROUP BY proposer_index, version
HAVING blocks >= 10
ORDER BY proposer_index, version;

-- Network health metrics
SELECT 
    toStartOfHour(slot_timestamp) as hour,
    COUNT(*) as blocks_per_hour,
    COUNT(DISTINCT proposer_index) as active_validators,
    AVG(CASE WHEN LENGTH(graffiti) > 2 THEN 1 ELSE 0 END) as graffiti_rate
FROM blocks
WHERE slot_timestamp >= now() - INTERVAL 24 HOUR
GROUP BY toStartOfHour(slot_timestamp)
ORDER BY hour;

-- Fork transition analysis
SELECT 
    version,
    MIN(slot) as first_slot,
    MAX(slot) as last_slot,
    COUNT(*) as total_blocks,
    MIN(slot_timestamp) as activation_time
FROM blocks
GROUP BY version
ORDER BY first_slot;
```

## Performance Optimization

### Indexing Strategy

ClickHouse automatically creates efficient indexes, but you can optimize queries:

```sql
-- Check table sizes
SELECT 
    table,
    formatReadableSize(sum(bytes)) as size
FROM system.parts 
WHERE database = 'beacon_chain'
GROUP BY table
ORDER BY sum(bytes) DESC;

-- Check partition efficiency
SELECT 
    partition,
    COUNT(*) as parts,
    formatReadableSize(sum(bytes)) as size
FROM system.parts 
WHERE table = 'blocks'
GROUP BY partition
ORDER BY partition;
```

### Query Performance

```sql
-- Use FINAL for ReplacingMergeTree tables when needed
SELECT COUNT(*) FROM blocks FINAL;

-- Leverage partition pruning
SELECT * FROM blocks 
WHERE slot_timestamp >= '2024-01-01' 
  AND slot_timestamp < '2024-02-01';

-- Use materialized columns for computed fields
-- slot_timestamp is pre-computed for time-based queries
```

### Configuration Tuning

```bash
# Increase workers for faster backfill
BACKFILL_WORKERS=8

# Adjust chunk size based on memory
CHUNK_SIZE=2000  # Larger chunks for more memory

# Use all_slots mode for complete data
VALIDATOR_MODE=all_slots
```

## Monitoring and Troubleshooting

### Progress Monitoring

```bash
# Overall progress
make status

# Detailed chunk status
python scripts/chunks.py overview

# Transformer progress
python scripts/transformer_status.py

# Service logs
make logs
```

### Health Checks

```sql
-- Check data freshness
SELECT MAX(slot) as latest_slot, MAX(slot_timestamp) as latest_time
FROM blocks;

-- Check for processing gaps
SELECT 
    raw_table_name,
    COUNT(*) as ranges,
    MIN(start_slot) as earliest,
    MAX(end_slot) as latest,
    SUM(processed_count) as total_processed
FROM transformer_progress FINAL
GROUP BY raw_table_name;

-- Check worker distribution
SELECT 
    loader_name,
    status,
    COUNT(*) as chunks
FROM load_state_chunks FINAL
GROUP BY loader_name, status
ORDER BY loader_name, status;
```

### Common Issues

1. **Connection Issues**
   ```bash
   # Test connection
   echo "SELECT 1" | clickhouse-client --host your-host --port 9000
   ```

2. **Memory Issues**
   ```bash
   # Reduce chunk size
   CHUNK_SIZE=500
   
   # Reduce workers
   BACKFILL_WORKERS=2
   ```

3. **Stuck Chunks**
   ```sql
   -- Reset stuck chunks
   INSERT INTO load_state_chunks 
   SELECT chunk_id, start_slot, end_slot, loader_name, 'pending', '', created_at, now()
   FROM load_state_chunks FINAL
   WHERE status = 'claimed' AND updated_at < now() - INTERVAL 1 HOUR;
   ```

4. **Fork Detection Issues**
   ```bash
   # Check foundation data
   echo "SELECT COUNT(*) FROM genesis" | clickhouse-client
   echo "SELECT COUNT(*) FROM specs" | clickhouse-client
   
   # Verify fork detection
   python -m src.main fork list
   ```

## Production Deployment

### High Availability Setup

```yaml
# docker-compose.prod.yml
services:
  backfill:
    deploy:
      replicas: 1
    restart: always
    
  realtime:
    deploy:
      replicas: 1
    restart: always
    
  transform-continuous:
    deploy:
      replicas: 1  
    restart: always
```

### Monitoring Setup

```bash
# Set up monitoring queries
echo "
SELECT 
    'blocks' as table,
    COUNT(*) as rows,
    MAX(slot) as latest_slot
FROM blocks
UNION ALL
SELECT 
    'validators' as table,
    COUNT(*) as rows, 
    MAX(slot) as latest_slot
FROM validators
" | clickhouse-client --format JSONEachRow
```

### Backup Strategy

```bash
# Backup specific tables
clickhouse-client --query "
SELECT * FROM blocks 
WHERE slot_timestamp >= '2024-01-01'
FORMAT Parquet
" > blocks_backup.parquet

# Full database backup (requires filesystem access)
# Use ClickHouse backup tools for production
```

## Integration Examples

### Python Integration

```python
import clickhouse_connect

# Connect
client = clickhouse_connect.get_client(
    host='your-host',
    port=9000,
    username='default',
    password='password'
)

# Query data
df = client.query_df("""
    SELECT slot, proposer_index, version
    FROM blocks 
    WHERE slot > 9000000 
    ORDER BY slot DESC 
    LIMIT 1000
""")

print(f"Loaded {len(df)} blocks")
```

### Grafana Dashboard

```sql
-- Blocks per hour (for Grafana)
SELECT 
    toUnixTimestamp(toStartOfHour(slot_timestamp)) * 1000 as time,
    COUNT(*) as blocks
FROM blocks
WHERE $__timeFilter(slot_timestamp)
GROUP BY toStartOfHour(slot_timestamp)
ORDER BY time;

-- Active validators (for Grafana)
SELECT 
    toUnixTimestamp(toStartOfDay(slot_timestamp)) * 1000 as time,
    COUNT(DISTINCT proposer_index) as active_validators
FROM blocks  
WHERE $__timeFilter(slot_timestamp)
GROUP BY toStartOfDay(slot_timestamp)
ORDER BY time;
```

The ClickHouse backend provides a robust, scalable solution for beacon chain data with comprehensive fork awareness and production-ready monitoring capabilities.