# Deployment Guide

This guide covers deploying the Beacon Chain Indexer using Docker in various environments.

## Quick Docker Deployment

### Basic Setup

```bash
# 1. Clone and setup
git clone <repository-url>
cd beacon-indexer
cp .env.example .env

# 2. Choose your backend
echo "STORAGE_BACKEND=parquet" >> .env    # Simple option
# OR
echo "STORAGE_BACKEND=clickhouse" >> .env # Production option

# 3. Configure beacon node
echo "BEACON_NODE_URL=http://your-beacon-node:5052" >> .env

# 4. Run the pipeline
make build
make backfill
make transform
```

### Docker Compose Profiles

The indexer uses profiles for different operations:

| Command | Purpose | When to Use |
|---------|---------|-------------|
| `make migration` | Set up ClickHouse schema | Once, ClickHouse only |
| `make backfill` | Load historical data | When you need past data |
| `make transform` | Process raw data (batch) | After backfill |
| `make realtime` | Continuous data loading | For live data |
| `make transform-continuous` | Continuous processing | For production |

## Environment Configurations

### Development Environment

```bash
# .env for development
STORAGE_BACKEND=parquet
BEACON_NODE_URL=http://localhost:5052
PARQUET_OUTPUT_DIR=./parquet_data
START_SLOT=9000000
END_SLOT=9010000
LOG_LEVEL=DEBUG
VALIDATOR_MODE=daily
```

### Production Environment

```bash
# .env for production
STORAGE_BACKEND=clickhouse
BEACON_NODE_URL=https://beacon-api.mainnet.com
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-secure-password
CLICKHOUSE_DATABASE=beacon_chain
CLICKHOUSE_SECURE=true
BACKFILL_WORKERS=8
CHUNK_SIZE=2000
LOG_LEVEL=INFO
VALIDATOR_MODE=all_slots
```

## Production Deployment with Docker Compose

### Step 1: Production Docker Compose Override

Create `docker-compose.prod.yml`:

```yaml
services:
  realtime:
    restart: unless-stopped
    environment:
      - STORAGE_BACKEND=${STORAGE_BACKEND}
      - BEACON_NODE_URL=${BEACON_NODE_URL}
      - CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
      - LOG_LEVEL=INFO
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'

  transform-continuous:
    restart: unless-stopped
    environment:
      - STORAGE_BACKEND=${STORAGE_BACKEND}
      - CLICKHOUSE_HOST=${CLICKHOUSE_HOST}
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
      - LOG_LEVEL=INFO
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'

  # Optional: ClickHouse database
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    restart: unless-stopped
    environment:
      - CLICKHOUSE_USER=default
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    ports:
      - "9000:9000"
      - "8123:8123"

volumes:
  clickhouse_data:
```

### Step 2: Production Deployment Commands

```bash
# Set up production environment
cp .env.example .env.prod
# Edit .env.prod with production settings

# Deploy with production overrides
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d

# Or use environment-specific commands
COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml ENV_FILE=.env.prod make realtime
COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml ENV_FILE=.env.prod make transform-continuous
```

## Monitoring and Health Checks

### Basic Health Monitoring

```bash
# Check service status
docker compose ps

# View service logs
make logs

# Check progress
make status

# Monitor specific service
docker compose logs -f realtime
docker compose logs -f transform-continuous
```

### Simple Health Check Script

Create `scripts/health_check.sh`:

```bash
#!/bin/bash

echo "=== Beacon Indexer Health Check ==="

# Check if containers are running
echo "Container Status:"
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Check ClickHouse connection (if using ClickHouse)
if [ "$STORAGE_BACKEND" = "clickhouse" ]; then
    echo -e "\nClickHouse Connection:"
    docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" 2>/dev/null && echo "✓ ClickHouse OK" || echo "✗ ClickHouse Failed"
fi

# Check Parquet files (if using Parquet)
if [ "$STORAGE_BACKEND" = "parquet" ]; then
    echo -e "\nParquet Files:"
    file_count=$(find ./parquet_data -name "*.parquet" 2>/dev/null | wc -l)
    echo "✓ Found $file_count Parquet files"
fi

# Check beacon node connectivity
echo -e "\nBeacon Node Connection:"
curl -s "${BEACON_NODE_URL}/eth/v1/node/health" >/dev/null && echo "✓ Beacon Node OK" || echo "✗ Beacon Node Failed"

echo -e "\n=== End Health Check ==="
```

### Log Monitoring

```bash
# Monitor errors across all services
docker compose logs --tail=100 -f | grep -i error

# Monitor specific patterns
docker compose logs --tail=100 -f | grep -E "(ERROR|WARN|Failed|Exception)"

# Service-specific monitoring
docker compose logs -f realtime | grep -E "(blocks processed|error)"
docker compose logs -f transform-continuous | grep -E "(transformed|failed)"
```

## Backup and Recovery

### Parquet Backend Backup

```bash
# Simple backup - copy files
backup_dir="/backup/beacon-indexer/$(date +%Y%m%d)"
mkdir -p "$backup_dir"
cp -r ./parquet_data "$backup_dir/"

# Compressed backup
tar -czf "beacon-indexer-backup-$(date +%Y%m%d).tar.gz" ./parquet_data

# Restore from backup
tar -xzf beacon-indexer-backup-20241201.tar.gz
```

### ClickHouse Backup

```bash
# Export specific tables
docker compose exec clickhouse clickhouse-client --query "
SELECT * FROM blocks 
WHERE slot_timestamp >= '2024-01-01'
FORMAT Parquet
" > blocks_backup.parquet

# Full database backup (requires filesystem access)
docker compose exec clickhouse mkdir -p /backup
docker compose exec clickhouse clickhouse-backup create backup_$(date +%Y%m%d)
```

## Troubleshooting

### Common Issues

#### 1. Permission Errors (Parquet)
```bash
# Fix directory permissions
mkdir -p ./parquet_data
chmod 755 ./parquet_data
sudo chown -R $USER:$USER ./parquet_data
```

#### 2. ClickHouse Connection Issues
```bash
# Test connection
docker compose exec clickhouse clickhouse-client --query "SELECT 1"

# Check ClickHouse logs
docker compose logs clickhouse

# Verify environment variables
docker compose exec realtime env | grep CLICKHOUSE
```

#### 3. Out of Memory Errors
```bash
# Reduce resource usage in .env
BACKFILL_WORKERS=2
CHUNK_SIZE=500
VALIDATOR_MODE=daily
PARQUET_ROWS_PER_FILE=50000
```

#### 4. API Connection Issues
```bash
# Test beacon node connectivity
curl "${BEACON_NODE_URL}/eth/v1/node/health"

# Check API logs
docker compose logs realtime | grep -i "beacon\|api\|connection"
```

### Debug Mode

```bash
# Enable debug logging
echo "LOG_LEVEL=DEBUG" >> .env

# Run with debug
docker compose down
docker compose up realtime

# Process small range for testing
echo "START_SLOT=9000000" >> .env
echo "END_SLOT=9000100" >> .env
make backfill
```

## Maintenance

### Regular Maintenance Tasks

```bash
# Weekly: Clean up old logs
docker system prune -f
docker compose logs --tail=0 -f >/dev/null 2>&1

# Monthly: Update images
docker compose pull
make build
docker compose down && docker compose up -d

# As needed: Reset and restart
docker compose down -v  # Warning: removes data
make clean
make build
```

### Data Validation

```bash
# Parquet data validation
python -c "
import pandas as pd
blocks = pd.read_parquet('./parquet_data/blocks/')
print(f'Blocks: {len(blocks)}, Latest slot: {blocks.slot.max()}')
"

# ClickHouse data validation
docker compose exec clickhouse clickhouse-client --query "
SELECT 
  'blocks' as table, 
  count() as rows, 
  max(slot) as latest_slot 
FROM blocks
UNION ALL
SELECT 
  'validators' as table, 
  count() as rows, 
  max(slot) as latest_slot 
FROM validators
"
```

This deployment guide focuses on practical Docker usage while keeping complexity minimal and maintainable for production use.