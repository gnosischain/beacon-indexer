#!/usr/bin/env python3
import os
import sys
import logging
import time
from typing import Dict, List, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

# Configure logger
logger = logging.getLogger("clickhouse_migration")
logger.setLevel(logging.INFO)

def connect_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    secure: bool = True,
    verify: bool = False
) -> Client:
    """
    Connect to ClickHouse using clickhouse_connect and return a Client.
    Raises an exception if connection fails.
    """
    logger.info(f"Connecting to ClickHouse at {host}:{port}, secure={secure}, verify={verify}")
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,   # Enable TLS
            verify=verify    # Verify certificate if True
        )
        # Quick test
        client.command("SELECT 1")
        logger.info("ClickHouse connection established successfully.")
        return client
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {e}")
        raise

def get_applied_migrations(client: Client, database: str) -> Dict[str, Dict[str, int]]:
    """
    Retrieves the latest migration state for each migration name and direction.
    
    Returns a dictionary where:
    - Key is the migration name (e.g., "001_initial_schema")
    - Value is a dict with keys "up" and "down", each containing the highest ID for that direction
      (or 0 if no migrations of that direction exist)
    """
    query = f"""
    SELECT 
        name,
        direction,
        max(id) as max_id
    FROM {database}.migrations
    GROUP BY name, direction
    """
    
    migration_state = {}
    
    try:
        result = client.query(query)
        for row in result.result_rows:
            name, direction, max_id = row
            
            if name not in migration_state:
                migration_state[name] = {"up": 0, "down": 0}
                
            migration_state[name][direction] = max_id
            
        return migration_state
    except Exception as e:
        logger.warning(f"Failed to get migration state: {e}")
        return {}

def get_next_migration_id(client: Client, database: str) -> int:
    """Get the next available migration ID"""
    query = f"SELECT max(id) FROM {database}.migrations"
    
    try:
        result = client.query(query)
        max_id = result.result_rows[0][0]
        return (max_id or 0) + 1
    except Exception:
        return 1

def ensure_migrations_table(client: Client, database: str) -> None:
    """Create the migrations table if it doesn't exist"""
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.migrations (
            id UInt32,
            name String,
            direction String,
            executed_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (id, name, direction)
    """
    client.command(create_table_sql)

def wait_for_mutations(client: Client, database: str, max_wait: int = 300) -> None:
    """
    Wait for all pending mutations to complete before proceeding.
    
    Args:
        client: ClickHouse client
        database: Database name
        max_wait: Maximum time to wait in seconds
    """
    logger.info("Checking for pending mutations...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            # Check for running mutations
            query = f"""
            SELECT count() as pending_mutations
            FROM system.mutations 
            WHERE database = '{database}' 
            AND is_done = 0
            """
            
            result = client.query(query)
            pending_count = result.result_rows[0][0]
            
            if pending_count == 0:
                logger.info("No pending mutations found. Safe to proceed.")
                return
            else:
                logger.info(f"Found {pending_count} pending mutations. Waiting...")
                time.sleep(5)
                
        except Exception as e:
            logger.warning(f"Error checking mutations: {e}")
            time.sleep(5)
    
    logger.warning(f"Timed out waiting for mutations after {max_wait} seconds")

def execute_statement_with_retry(client: Client, statement: str, max_retries: int = 5, ignore_missing: bool = False) -> None:
    """
    Execute a statement with retry logic for mutation conflicts.
    
    Args:
        client: ClickHouse client
        statement: SQL statement to execute
        max_retries: Maximum number of retries
        ignore_missing: If True, ignore errors about missing objects (for down migrations)
    """
    for attempt in range(max_retries):
        try:
            logger.debug(f"Executing statement (attempt {attempt + 1}): {statement[:100]}...")
            client.command(statement)
            return
        except ClickHouseError as e:
            error_message = str(e)
            
            # Handle mutation conflicts
            if "CANNOT_ASSIGN_ALTER" in error_message or "mutation queries are not finished" in error_message:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # Progressive backoff: 5s, 10s, 15s, etc.
                    logger.warning(f"Mutation conflict detected. Waiting {wait_time}s before retry {attempt + 2}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Failed after {max_retries} attempts due to mutation conflicts")
                    raise
            
            # Handle missing objects in down migrations
            elif ignore_missing and any(phrase in error_message for phrase in [
                "Cannot find index",
                "Wrong index name", 
                "doesn't exist",
                "BAD_ARGUMENTS"
            ]):
                logger.warning(f"Ignoring missing object error (expected in down migration): {error_message}")
                return
            
            else:
                # For other errors, don't retry
                raise
    
def run_migrations(client: Client, database: str, migrations_dir: str, direction: str):
    """
    Create the 'migrations' table if not exists, then apply .up.sql or .down.sql files.
    Each .sql file is split by semicolons and run as individual statements.
    Now with improved handling for ALTER/DROP operations.

    :param direction: "up" to run *.up.sql in ascending order,
                      "down" to run *.down.sql in descending order.
    """
    # Make sure the migrations tracking table exists
    logger.debug("Ensuring 'migrations' tracking table exists if it doesn't already...")
    ensure_migrations_table(client, database)

    # Wait for any pending mutations before starting
    wait_for_mutations(client, database)

    # Get the current state of migrations
    migration_state = get_applied_migrations(client, database)
    logger.debug(f"Current migration state: {migration_state}")

    # 1) Decide which pattern to look for, and how to order them
    if direction.lower() == "down":
        file_pattern = ".down.sql"
        # Usually we run .down.sql in reverse alphabetical order
        sort_reverse = True
    else:
        direction = "up"  # Normalize the direction
        file_pattern = ".up.sql"
        sort_reverse = False

    logger.info(f"Looking for '{file_pattern}' files in '{migrations_dir}' (direction={direction})")

    # 2) Gather the relevant SQL files
    try:
        all_files = [
            f for f in os.listdir(migrations_dir)
            if f.endswith(file_pattern) and os.path.isfile(os.path.join(migrations_dir, f))
        ]
    except FileNotFoundError:
        logger.error(f"Migrations directory not found: {migrations_dir}")
        raise
    
    # Sort ascending for .up.sql, descending for .down.sql
    all_files.sort(reverse=sort_reverse)

    if not all_files:
        logger.info(f"No {file_pattern} files found. Nothing to run.")
        return

    logger.info(f"Found {len(all_files)} migration file(s) matching '{file_pattern}'.")

    # 3) Apply each migration based on its state
    migrations_to_run = []
    
    for filename in all_files:
        # Get base name without the direction suffix
        base_name = filename.replace(file_pattern, "")
        
        # Check if this migration should be applied
        if base_name in migration_state:
            up_id = migration_state[base_name].get("up", 0)
            down_id = migration_state[base_name].get("down", 0)
            
            if direction == "up" and up_id <= down_id:
                # For "up", we apply if the latest "up" is older than or equal to the latest "down"
                migrations_to_run.append(filename)
            elif direction == "down" and down_id < up_id:
                # For "down", we apply if the latest "down" is older than the latest "up"
                migrations_to_run.append(filename)
        else:
            # Migration not seen before, apply it if going "up"
            if direction == "up":
                migrations_to_run.append(filename)
    
    if not migrations_to_run:
        logger.info(f"No migrations to run in direction '{direction}'.")
        return
        
    logger.info(f"Will run {len(migrations_to_run)} migrations: {migrations_to_run}")
    
    # Apply migrations
    for filename in migrations_to_run:
        base_name = filename.replace(file_pattern, "")
        filepath = os.path.join(migrations_dir, filename)
        logger.info(f"Applying migration: {filename}")

        with open(filepath, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Split on semicolons to handle multiple statements
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        try:
            mutation_count = 0
            total_statements = len(statements)
            logger.info(f"Processing {total_statements} statements in {filename}")
            
            for i, stmt in enumerate(statements):
                if stmt:
                    # Log progress every 10 statements or for mutations
                    is_mutation = any(keyword in stmt.upper() for keyword in [
                        'ALTER TABLE', 'DROP INDEX', 'ADD INDEX', 'DROP COLUMN', 'ADD COLUMN'
                    ])
                    
                    if is_mutation or (i + 1) % 10 == 0:
                        logger.info(f"Processing statement {i + 1}/{total_statements}: {stmt[:60]}...")
                    
                    if is_mutation:
                        mutation_count += 1
                        # Only wait for mutations if we haven't checked recently or it's the first mutation
                        if mutation_count == 1 or mutation_count % 5 == 0:
                            wait_for_mutations(client, database, max_wait=60)
                        
                        # Execute with retry logic, ignoring missing objects for down migrations
                        execute_statement_with_retry(client, stmt, ignore_missing=(direction == "down"))
                        
                        # Add a small delay after mutation operations (reduced from 2s to 0.5s)
                        if i < len(statements) - 1:  # Not the last statement
                            time.sleep(0.5)
                    else:
                        # Regular statement, execute normally (but still handle missing objects in down migrations)
                        try:
                            logger.debug(f"Executing statement: {stmt[:100]}...")
                            client.command(stmt)
                        except ClickHouseError as e:
                            if direction == "down" and any(phrase in str(e) for phrase in [
                                "Cannot find index",
                                "Wrong index name", 
                                "doesn't exist",
                                "BAD_ARGUMENTS"
                            ]):
                                logger.warning(f"Ignoring missing object error in down migration: {str(e)}")
                            else:
                                raise

            # Record success in the migrations table
            next_id = get_next_migration_id(client, database)
            client.command(
                f"INSERT INTO {database}.migrations (id, name, direction) VALUES " +
                f"({next_id}, '{base_name}', '{direction}')"
            )
            logger.info(f"[DONE] {filename} applied successfully with ID {next_id}.")

        except ClickHouseError as che:
            logger.error(f"Migration {filename} failed with ClickHouse error: {che}")
            raise

        except Exception as e:
            logger.error(f"Migration {filename} failed with unexpected error: {e}")
            raise

        # Add a delay between migrations to prevent overwhelming ClickHouse
        time.sleep(1)

    logger.info(f"All '{direction}' migrations complete.")


if __name__ == "__main__":
    """
    CLI usage: 
      run_clickhouse_migrations.py host=... port=... user=... password=... db=... dir=... direction=... secure=... verify=...

    If not provided as CLI args, fallback to env variables:
      CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
      CH_MIGRATIONS_DIR, CH_DIRECTION, CH_SECURE, CH_VERIFY

    *.up.sql files -> "up" migrations
    *.down.sql files -> "down" migrations
    """
    # Logging config
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    args_dict = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            args_dict[k] = v

    # Fallback to environment variables if not provided
    host = args_dict.get("host", os.getenv("CLICKHOUSE_HOST", "localhost"))
    port_str = args_dict.get("port", os.getenv("CLICKHOUSE_PORT", "9000"))
    user = args_dict.get("user", os.getenv("CLICKHOUSE_USER", "default"))
    password = args_dict.get("password", os.getenv("CLICKHOUSE_PASSWORD", ""))
    db = args_dict.get("db", os.getenv("CLICKHOUSE_DATABASE", "default"))
    migrations_dir = args_dict.get("dir", os.getenv("CH_MIGRATIONS_DIR", "./migrations"))
    direction = args_dict.get("direction", os.getenv("CH_DIRECTION", "up"))
    secure_str = args_dict.get("secure", os.getenv("CH_SECURE", "True"))
    verify_str = args_dict.get("verify", os.getenv("CH_VERIFY", "False"))

    # Convert port
    try:
        port = int(port_str)
    except ValueError:
        logger.warning(f"Invalid port: '{port_str}'. Defaulting to 9000.")
        port = 9000

    # Convert booleans
    secure = secure_str.lower() in ("true", "1", "yes")
    verify = verify_str.lower() in ("true", "1", "yes")

    logger.info(f"Starting migrations with settings:")
    logger.info(f"  Host: {host}")
    logger.info(f"  Port: {port}")
    logger.info(f"  User: {user}")
    logger.info(f"  Database: {db}")
    logger.info(f"  Migrations dir: {migrations_dir}")
    logger.info(f"  Direction: {direction}")
    logger.info(f"  Secure: {secure}")
    logger.info(f"  Verify: {verify}")

    try:
        # Connect to ClickHouse
        client = connect_clickhouse(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            secure=secure,
            verify=verify
        )

        # Run migrations (up or down)
        run_migrations(client, db, migrations_dir, direction)
        logger.info("Migration completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)