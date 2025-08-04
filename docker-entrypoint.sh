#!/bin/bash
set -e

# Set Python path
export PYTHONPATH="/app:${PYTHONPATH}"

# Check storage backend
STORAGE_BACKEND=${STORAGE_BACKEND:-clickhouse}

if [ "$STORAGE_BACKEND" = "clickhouse" ]; then
    # Wait for ClickHouse to be ready
    echo "Using ClickHouse backend - waiting for database..."
    until python -c "
import sys
sys.path.insert(0, '/app/src')
from services.clickhouse import ClickHouse
try:
    ch = ClickHouse()
    ch.client.command('SELECT 1')
    print('ClickHouse is ready!')
except Exception as e:
    print(f'ClickHouse not ready: {e}')
    exit(1)
"; do
        echo "ClickHouse is unavailable - sleeping"
        sleep 5
    done
else
    echo "Using Parquet backend - no database connection required"
    # Ensure parquet directory exists
    mkdir -p ${PARQUET_OUTPUT_DIR:-/app/parquet_data}
fi

echo "Starting application with $STORAGE_BACKEND backend..."
exec "$@"