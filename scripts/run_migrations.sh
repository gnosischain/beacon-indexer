#!/bin/bash
set -e

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "Loading environment variables from .env file"
    export $(grep -v '^#' .env | xargs)
fi

# Check if we're using Docker Compose
if command -v docker-compose &> /dev/null || command -v docker &> /dev/null; then
    echo "Using Docker Compose to run migrations"
    
    # Try docker compose first (new format)
    if command -v docker &> /dev/null && docker compose version &> /dev/null; then
        # Run migrations
        docker compose run --rm migrate
        
        # Run specs updater
        echo "Updating specs after migrations"
        docker compose --profile specs up specs-updater --exit-code-from specs-updater
    # Fall back to docker-compose (old format)
    elif command -v docker-compose &> /dev/null; then
        # Run migrations
        docker-compose run --rm migrate
        
        # Run specs updater
        echo "Updating specs after migrations"
        docker-compose run --rm specs-updater
    fi
else
    # If not using Docker, run the Python script directly with environment variables
    echo "Running migration script directly"
    python scripts/run_clickhouse_migrations.py \
        host=${CLICKHOUSE_HOST:-localhost} \
        port=${CLICKHOUSE_PORT:-443} \
        user=${CLICKHOUSE_USER:-default} \
        password=${CLICKHOUSE_PASSWORD:-} \
        db=${CLICKHOUSE_DATABASE:-beacon_chain} \
        dir=${CH_MIGRATIONS_DIR:-./migrations} \
        direction=${CH_DIRECTION:-up} \
        secure=${CH_SECURE:-True} \
        verify=${CH_VERIFY:-False}
        
    # Also run specs updater
    echo "Updating specs after migrations"
    python scripts/update_specs.py
fi

echo "Migrations and specs update completed"