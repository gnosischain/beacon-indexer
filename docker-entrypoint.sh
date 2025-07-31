#!/bin/bash
set -e

# Set Python path
export PYTHONPATH="/app:${PYTHONPATH}"

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
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

echo "Starting application..."
exec "$@"