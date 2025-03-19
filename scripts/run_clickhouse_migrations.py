#!/usr/bin/env python3
import os
import sys
import logging
from typing import Optional, Set

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


def run_migrations(client: Client, database: str, migrations_dir: str, direction: str):
    """
    Create the 'migrations' table if not exists, then apply .up.sql or .down.sql files.
    Each .sql file is split by semicolons and run as individual statements.

    :param direction: "up" to run *.up.sql in ascending order,
                      "down" to run *.down.sql in descending order.
    """
    # Make sure the migrations tracking table exists
    logger.debug("Ensuring 'migrations' tracking table exists if it doesn't already...")
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.migrations (
            name String,
            executed_at DateTime DEFAULT now(),
            success UInt8 DEFAULT 1
        )
        ENGINE = MergeTree()
        ORDER BY (name)
    """
    client.command(create_table_sql)

    logger.debug("Retrieving already-applied migrations...")
    existing_migrations = client.query(f"SELECT name FROM {database}.migrations")
    applied: Set[str] = {row[0] for row in existing_migrations.result_rows}

    # 1) Decide which pattern to look for, and how to order them
    if direction.lower() == "down":
        file_pattern = ".down.sql"
        # Usually we run .down.sql in reverse alphabetical order
        # so the most recent "up" is undone first
        sort_reverse = True
    else:
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

    # 3) Apply each new migration in order
    for filename in all_files:
        if filename in applied:
            logger.info(f"Skipping {filename}, already applied.")
            continue

        filepath = os.path.join(migrations_dir, filename)
        logger.info(f"Applying migration: {filename}")

        with open(filepath, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Split on semicolons to handle multiple statements
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

        try:
            for stmt in statements:
                if stmt:
                    logger.debug(f"Executing statement: {stmt[:100]}...")
                    client.command(stmt)

            # Record success in the migrations table
            client.command(f"INSERT INTO {database}.migrations (name) VALUES ('{filename}')")
            logger.info(f"[DONE] {filename} applied successfully.")

        except ClickHouseError as che:
            logger.error(f"Migration {filename} failed with ClickHouse error: {che}")
            raise

        except Exception as e:
            logger.error(f"Migration {filename} failed with unexpected error: {e}")
            raise

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