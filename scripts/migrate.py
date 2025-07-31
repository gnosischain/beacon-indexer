#!/usr/bin/env python3
"""
Simple database migration script.
"""
import os
import sys

# Add the src directory to Python path for imports
sys.path.insert(0, '/app/src' if os.path.exists('/app/src') else 'src')

from services.clickhouse import ClickHouse
from utils.logger import setup_logger, logger

def run_migrations():
    """Run database migrations."""
    setup_logger()
    
    # Determine migrations directory path
    if os.path.exists('/app/migrations'):
        migrations_dir = '/app/migrations'
    else:
        migrations_dir = 'migrations'
    
    clickhouse = ClickHouse()
    
    # Get list of migration files
    migration_files = [f for f in os.listdir(migrations_dir) if f.endswith('.sql')]
    migration_files.sort()
    
    logger.info("Starting migrations", files=migration_files)
    
    for migration_file in migration_files:
        file_path = os.path.join(migrations_dir, migration_file)
        
        logger.info("Running migration", file=migration_file)
        
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for statement in statements:
            try:
                # Handle SET statements separately - they need to be executed alone
                if statement.upper().startswith('SET '):
                    logger.debug("Executing SET statement", statement=statement)
                    clickhouse.client.command(statement)
                else:
                    # Regular CREATE, INSERT, etc. statements
                    logger.debug("Executing statement", statement=statement[:100] + "...")
                    clickhouse.client.command(statement)
                    
            except Exception as e:
                logger.error("Migration statement failed", 
                           file=migration_file, 
                           statement=statement[:100] + "...",
                           error=str(e))
                raise
        
        logger.info("Migration completed", file=migration_file)
    
    logger.info("All migrations completed successfully")

if __name__ == "__main__":
    run_migrations()