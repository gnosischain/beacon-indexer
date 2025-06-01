import asyncio
import logging
import time
import random
from typing import List, Dict, Any, Tuple, Optional
import pyarrow as pa
from datetime import datetime

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

from src.config import config
from src.utils.logger import logger


def connect_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    secure: bool = False,
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


class ClickHouseService:
    """Service to interact with ClickHouse database using clickhouse-connect with improved batch handling."""

    def __init__(
        self, 
        host: str = None, 
        port: int = None, 
        user: str = None, 
        password: str = None, 
        database: str = None,
        secure: bool = False,
        verify: bool = False,
        max_batch_size: int = 10000  # Limit batch size for better reliability
    ):
        """
        Initialize the ClickHouseService with given or default config parameters.
        """
        self.host = host or config.clickhouse.host
        self.port = port or config.clickhouse.port
        self.user = user or config.clickhouse.user
        self.password = password or config.clickhouse.password
        self.database = database or config.clickhouse.database
        self.secure = secure
        self.verify = verify
        self.max_batch_size = max_batch_size

        self.client = connect_clickhouse(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            secure=self.secure,
            verify=self.verify
        )
        
        # Cache for genesis and specs data
        self._genesis_time = None
        self._seconds_per_slot = None
        self._slots_per_epoch = None
        self._table_schemas = {}  # Cache for table schemas

    async def init_cache(self):
        """Initialize cache for frequently accessed configuration."""
        if self._genesis_time is None:
            # Get genesis time
            query = "SELECT toUnixTimestamp(genesis_time) as genesis_time FROM genesis LIMIT 1"
            result = self.execute(query)
            # Ensure result is a list
            result_list = list(result) if hasattr(result, '__iter__') and not isinstance(result, list) else result
            if result_list and len(result_list) > 0:
                self._genesis_time = result_list[0].get("genesis_time")
                
        if self._seconds_per_slot is None or self._slots_per_epoch is None:
            # Get specs
            query = """
            WITH latest_specs AS (
                SELECT 
                    parameter_name,
                    argMax(parameter_value, updated_at) as parameter_value
                FROM specs
                WHERE parameter_name IN ('SECONDS_PER_SLOT', 'SLOTS_PER_EPOCH')
                GROUP BY parameter_name
            )
            SELECT parameter_name, parameter_value FROM latest_specs
            """
            
            results = self.execute(query)
            # Ensure results is a list
            results_list = list(results) if hasattr(results, '__iter__') and not isinstance(results, list) else results
            for row in results_list:
                if row["parameter_name"] == "SECONDS_PER_SLOT":
                    self._seconds_per_slot = int(row["parameter_value"])
                elif row["parameter_name"] == "SLOTS_PER_EPOCH":
                    self._slots_per_epoch = int(row["parameter_value"])

    def execute(self, query: str, params: Dict = None) -> List[Dict]:
        """
        Execute a query with parameters, returning the results as a list of dictionaries.
        """
        try:
            # Log query for debugging
            if params:
                param_str = str(params)
                if len(param_str) > 100:
                    param_str = param_str[:100] + "..."
                logger.debug(f"Executing query with params: {param_str}")
            
            # For INSERT queries, handle them differently
            if query.strip().upper().startswith("INSERT INTO"):
                parts = query.split("VALUES")
                if len(parts) > 1:
                    table_part = parts[0].strip()
                    table_name = table_part.split("INTO")[1].strip().split("(")[0].strip()
                    columns_part = table_part.split("(")[1].split(")")[0].strip() if "(" in table_part else ""
                    columns = [col.strip() for col in columns_part.split(",")]
                    
                    # Build a ClickHouse-specific insert command
                    if params:
                        # For single row insert
                        columns_str = ", ".join(columns)
                        values = []
                        for col in columns:
                            val = params.get(col, None)
                            if val is None:
                                values.append("NULL")
                            elif isinstance(val, str):
                                escaped_val = val.replace("'", "''")
                                values.append(f"'{escaped_val}'")
                                #values.append(f"'{val.replace('\'', '\'\'')}'")
                            elif isinstance(val, bool):
                                values.append("1" if val else "0")
                            elif isinstance(val, (list, tuple)):
                                values.append(f"[{', '.join(str(x) for x in val)}]")
                            elif isinstance(val, datetime):
                                values.append(f"'{val.isoformat()}'")
                            else:
                                values.append(str(val))
                        
                        values_str = f"({', '.join(values)})"
                        full_query = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_str}"
                        self.client.command(full_query)
                        return []
            
            # For regular queries
            result = self.client.query(query, parameters=params)
            # Convert generator to list to make it subscriptable
            return list(result.named_results())
        except Exception as e:
            logger.error(f"ClickHouse error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def execute_many(self, query: str, params_list: List[Dict]) -> None:
        """
        Execute a query multiple times with different parameter sets efficiently.
        Now with improved batch handling and automatic batch size limitation.
        """
        if not params_list:
            return
            
        try:
            # For better performance, use batch insertion when possible
            if query.strip().upper().startswith("INSERT INTO"):
                # Extract table name from query
                table_name = query.split("INSERT INTO")[1].split("(")[0].strip()
                
                # Break into smaller batches if needed
                if len(params_list) > self.max_batch_size:
                    logger.info(f"Breaking large batch of {len(params_list)} into smaller batches of {self.max_batch_size}")
                    for i in range(0, len(params_list), self.max_batch_size):
                        batch = params_list[i:i+self.max_batch_size]
                        # Prepare data for optimized insertion
                        self._optimize_and_insert(table_name, batch)
                        
                        # Small delay between batches
                        time.sleep(0.2)
                else:
                    # Prepare data for optimized insertion
                    self._optimize_and_insert(table_name, params_list)
            else:
                # For non-INSERT queries, fall back to executing them individually
                for params in params_list:
                    self.client.command(query, parameters=params)
        except ClickHouseError as e:
            logger.error(f"ClickHouse error during batch operation: {e}")
            raise

    def _get_table_schema(self, table_name: str) -> Tuple[List[str], Dict[str, str]]:
        """
        Get table schema with caching to avoid repeated queries.
        Returns a tuple of (column_names, column_types)
        """
        if table_name in self._table_schemas:
            return self._table_schemas[table_name]
            
        try:
            schema_query = f"DESCRIBE TABLE {table_name}"
            schema_result = self.execute(schema_query)
            schema_list = list(schema_result) if hasattr(schema_result, '__iter__') and not isinstance(schema_result, list) else schema_result
            table_columns = [row['name'] for row in schema_list]
            
            # Get column name and type information for better diagnostics
            column_types = {row['name']: row['type'] for row in schema_list}
            
            # Cache the result
            self._table_schemas[table_name] = (table_columns, column_types)
            
            return table_columns, column_types
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {e}")
            return [], {}

    def _optimize_and_insert(self, table_name: str, data: List[Dict]) -> None:
        """
        Optimize insertion based on partitioning strategy and perform the insert.
        Now with schema caching and better error handling.
        """
        # Get column names from the first data item
        if not data:
            logger.warning(f"No data provided for {table_name} insert")
            return
        
        try:        
            # Check the table schema to get the correct column names
            table_columns, column_types = self._get_table_schema(table_name)
            
            logger.debug(f"Table {table_name} has columns: {table_columns}")
            logger.debug(f"Column types for {table_name}: {column_types}")
            
            # Start with all keys from the data
            all_keys = set()
            for item in data:
                all_keys.update(item.keys())
            
            column_names = list(all_keys)
            logger.debug(f"Keys in data for {table_name}: {column_names}")
            
            # If we have table schema, only use columns that exist in the table
            if table_columns:
                filtered_column_names = [col for col in column_names if col in table_columns]
                if len(filtered_column_names) < len(column_names):
                    removed = set(column_names) - set(filtered_column_names)
                    logger.debug(f"Removing non-existent columns from {table_name} insert: {removed}")
                column_names = filtered_column_names
                
                if not column_names:
                    logger.error(f"No valid columns for {table_name} insert after filtering")
                    return
            
            # Log data sample for debugging
            if data:
                sample_row = {k: v for k, v in data[0].items() if k in column_names}
                logger.debug(f"Sample row for {table_name}: {sample_row}")
            
            # Insert all at once
            try:
                self._insert_batch(table_name, data, column_names)
            except Exception as e:
                logger.error(f"Error in batch insert, will try row-by-row fallback: {e}")
                # Fallback to row-by-row insert
                self._insert_row_by_row(table_name, data, column_names)
            
        except Exception as e:
            logger.error(f"Error in optimize_and_insert for {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def _insert_row_by_row(self, table_name: str, data: List[Dict], column_names: List[str]) -> None:
        """
        Insert data row by row as a fallback method.
        """
        columns_str = ', '.join(column_names)
        success_count = 0
        
        for i, row_data in enumerate(data):
            try:
                row_values = []
                for col in column_names:
                    val = row_data.get(col, None)
                    if val is None:
                        row_values.append("NULL")
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        row_values.append(f"'{escaped_val}'")
                    elif isinstance(val, bool):
                        row_values.append("1" if val else "0")
                    elif isinstance(val, (list, tuple)):
                        items = []
                        for item in val:
                            if item is None:
                                items.append("NULL")
                            elif isinstance(item, str):
                                escaped_item = item.replace("'", "''")
                                items.append(f"'{escaped_item}'")
                            else:
                                items.append(str(item))
                        row_values.append(f"[{', '.join(items)}]")
                    elif isinstance(val, datetime):
                        row_values.append(f"'{val.isoformat()}'")
                    else:
                        row_values.append(str(val))
                
                single_row_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({', '.join(row_values)})"
                
                # Add small random delay to avoid thundering herd
                if i > 0 and i % 10 == 0:
                    time.sleep(random.uniform(0.05, 0.2))
                    
                self.client.command(single_row_query)
                success_count += 1
                
                # Log progress occasionally
                if i > 0 and i % 50 == 0:
                    logger.info(f"Inserted {i}/{len(data)} rows into {table_name} using row-by-row method")
                    
            except Exception as inner_e:
                logger.error(f"Error inserting row {i} into {table_name}: {inner_e}")
        
        if success_count > 0:
            logger.info(f"Inserted {success_count} out of {len(data)} rows using row-by-row fallback")
        else:
            logger.error(f"Failed to insert any rows into {table_name}")

    def _insert_batch(self, table_name: str, data: List[Dict], column_names: List[str]) -> None:
        """
        Insert a batch of data into a table with improved formatting and error handling.
        """
        if not data or not column_names:
            logger.warning(f"No data or column names provided for {table_name} insert")
            return
            
        # Debug log first row to help diagnose issues
        if data:
            sample_row = {k: data[0][k] for k in column_names if k in data[0]}
            logger.debug(f"Sample row for {table_name}: {sample_row}")
        
        # Prepare data in columnar format for more efficient insertion
        try:
            # Build the SQL query using values
            columns_str = ', '.join(column_names)
            
            # Convert to SQL-safe strings
            formatted_values = []
            
            for row_data in data:
                row_values = []
                for col in column_names:
                    val = row_data.get(col, None)
                    if val is None:
                        row_values.append("NULL")
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        row_values.append(f"'{escaped_val}'")
                    elif isinstance(val, bool):
                        row_values.append("1" if val else "0")
                    elif isinstance(val, (list, tuple)):
                        items = []
                        for item in val:
                            if item is None:
                                items.append("NULL")
                            elif isinstance(item, str):
                                escaped_item = item.replace("'", "''")
                                items.append(f"'{escaped_item}'")
                            else:
                                items.append(str(item))
                        row_values.append(f"[{', '.join(items)}]")
                    elif isinstance(val, datetime):
                        row_values.append(f"'{val.isoformat()}'")
                    else:
                        row_values.append(str(val))
                
                formatted_values.append(f"({', '.join(row_values)})")
            
            # Break into smaller subsets for each execute to avoid too-large queries
            batch_size = config.scraper.batch_size // 2 #min(config.scraper.batch_size // 2, 15000)
            
            for i in range(0, len(formatted_values), batch_size):
                batch_values = formatted_values[i:i+batch_size]
                values_str = ', '.join(batch_values)
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_str}"
                
                # Execute the query with retries
                try:
                    self.client.command(query)
                    logger.info(f"Inserted batch of {len(batch_values)} rows into {table_name} ({i+1}-{min(i+batch_size, len(formatted_values))} of {len(formatted_values)})")
                except Exception as e:
                    logger.error(f"Error executing batch insert query on {table_name}, batch {i//batch_size}: {e}")
                    raise
                    
                # Add a small delay between batches
                if i + batch_size < len(formatted_values):
                    time.sleep(0.1)
                    
        except Exception as e:
            logger.error(f"Error preparing data for {table_name} insert: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def bulk_insert(self, table_name: str, data: List[Dict], batch_size: int = 1000) -> None:
        """
        Perform a high-performance bulk insert into ClickHouse.
        Now with better batch size management.
        """
        if not data:
            return
            
        try:
            # Use smaller batch size if the table is known to be large/problematic
            adjusted_batch_size = min(batch_size, self.max_batch_size)
            
            if table_name == 'validators':
                # Validators table is especially large, use smaller batches
                adjusted_batch_size = min(adjusted_batch_size, 200)
            
            # Process in batches for better memory management
            for i in range(0, len(data), adjusted_batch_size):
                batch = data[i:i+adjusted_batch_size]
                self._optimize_and_insert(table_name, batch)
                logger.info(f"Bulk inserted batch of {len(batch)} rows into {table_name} (total: {i+len(batch)}/{len(data)})")
                
                # Add a small delay between batches
                if i + adjusted_batch_size < len(data):
                    time.sleep(0.2)
        
        except Exception as e:
            logger.error(f"Error during bulk insert to {table_name}: {e}")
            raise

    def get_missing_slots_in_range(self, start_slot: int, end_slot: int) -> List[int]:
        """
        Return all slot numbers that are missing from the 'blocks' table in a given range.
        
        :param start_slot: Start slot (inclusive)
        :param end_slot:   End slot (inclusive)
        :return:           List of missing slot integers
        """
        try:
            # Get existing slots in range first
            query = """
            SELECT slot
            FROM blocks
            WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
            ORDER BY slot
            """
            
            existing_slots = self.execute(query, {
                "start_slot": start_slot,
                "end_slot": end_slot
            })
            
            existing_slot_set = set(row["slot"] for row in existing_slots)
            all_slots = set(range(start_slot, end_slot + 1))
            
            # Compute the difference
            missing_slots = sorted(list(all_slots - existing_slot_set))
            
            return missing_slots
        except Exception as e:
            logger.error(f"Error getting missing slots: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Fallback: just return a small batch from the start
            return list(range(start_slot, min(start_slot + 5, end_slot + 1)))

    def get_canonical_blocks_in_range(self, start_slot: int, end_slot: int) -> List[Dict]:
        """
        Retrieve canonical blocks in a specified slot range.

        :param start_slot: Start slot (inclusive)
        :param end_slot:   End slot (inclusive)
        :return:           List of dicts with 'slot' and 'block_root' keys
        """
        query = """
        SELECT slot, block_root
        FROM blocks
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
          AND is_canonical = 1
        ORDER BY slot
        """
        return self.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })

    def get_last_processed_slot(self, scraper_id: str) -> int:
        """
        Get the last processed slot for a scraper from the scraper_state table.

        :param scraper_id: ID of the scraper
        :return:           The last processed slot if found, else 0
        """
        query = """
        SELECT last_processed_slot
        FROM scraper_state
        WHERE scraper_id = %(scraper_id)s
        ORDER BY updated_at DESC
        LIMIT 1
        """
        try:
            results = self.execute(query, {"scraper_id": scraper_id})
            if results and len(results) > 0:
                return int(results[0]["last_processed_slot"])
            return 0
        except Exception as e:
            logger.error(f"Error getting last processed slot: {e}")
            return 0

    def update_scraper_state(self, scraper_id: str, last_processed_slot: int, mode: str) -> None:
        """
        Update (insert) the state of a scraper in scraper_state.

        :param scraper_id:         ID of the scraper
        :param last_processed_slot: The last slot processed by the scraper
        :param mode:               Operation mode of the scraper
        """
        query = """
        INSERT INTO scraper_state (scraper_id, last_processed_slot, mode)
        VALUES (%(scraper_id)s, %(last_processed_slot)s, %(mode)s)
        """
        try:
            self.execute(query, {
                "scraper_id": scraper_id,
                "last_processed_slot": last_processed_slot,
                "mode": mode
            })
        except Exception as e:
            logger.error(f"Error updating scraper state: {e}")

    def get_genesis_time(self) -> Optional[int]:
        """Get the genesis time as a Unix timestamp."""
        if self._genesis_time is not None:
            return self._genesis_time
            
        query = "SELECT toUnixTimestamp(genesis_time) as genesis_time FROM genesis LIMIT 1"
        result = self.execute(query)
        if result:
            self._genesis_time = result[0].get("genesis_time")
            return self._genesis_time
        return None
        
    def get_time_parameters(self) -> Dict[str, int]:
        """Get the essential time parameters (SECONDS_PER_SLOT, SLOTS_PER_EPOCH) as a dictionary."""
        # Use cached values if available
        if self._seconds_per_slot is not None and self._slots_per_epoch is not None:
            return {
                "seconds_per_slot": self._seconds_per_slot,
                "slots_per_epoch": self._slots_per_epoch
            }
        
        query = """
        WITH latest_specs AS (
            SELECT 
                parameter_name,
                argMax(parameter_value, updated_at) as parameter_value
            FROM specs
            WHERE parameter_name IN ('SECONDS_PER_SLOT', 'SLOTS_PER_EPOCH')
            GROUP BY parameter_name
        )
        SELECT parameter_name, parameter_value FROM latest_specs
        """
        
        results = self.execute(query)
        time_params = {}
        
        for row in results:
            if row["parameter_name"] == "SECONDS_PER_SLOT":
                time_params["seconds_per_slot"] = int(row["parameter_value"])
                self._seconds_per_slot = int(row["parameter_value"])
            elif row["parameter_name"] == "SLOTS_PER_EPOCH":
                time_params["slots_per_epoch"] = int(row["parameter_value"])
                self._slots_per_epoch = int(row["parameter_value"])
                
        return time_params

    def get_threadsafe_client(self):
        """Get a thread-safe client for concurrent operations."""
        return ClickHouseService(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            secure=self.secure,
            verify=self.verify
        )