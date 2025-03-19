#!/bin/bash
set -e

# Create directory for scripts if it doesn't exist
mkdir -p /app/scripts

# Copy the migration script into the container if it doesn't already exist
MIGRATION_SCRIPT="/app/scripts/run_clickhouse_migrations.py"

# Check if the migration script already exists (might be mounted from host)
if [ ! -f "${MIGRATION_SCRIPT}" ]; then
    echo "Copying migration script to ${MIGRATION_SCRIPT}..."
    cp /app/run_clickhouse_migrations.py ${MIGRATION_SCRIPT}
    chmod +x ${MIGRATION_SCRIPT}
fi

# Execute the provided command
echo "Starting Beacon Chain Scraper..."
exec "$@"