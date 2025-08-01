import clickhouse_connect
import json
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from typing import List, Dict, Any, Optional
from datetime import datetime
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
    Connect to ClickHouse Cloud with settings that work within cloud constraints.
    """
    logger.info("Connecting to ClickHouse Cloud with optimized settings", 
               host=host, 
               port=port, 
               secure=secure)
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,
            verify=verify,
            # Increased timeouts for large payloads
            send_receive_timeout=1800,  # 30 minutes
            connect_timeout=300,        # 5 minutes
            # Cloud-friendly settings (no memory limits - those are fixed by cloud)
            settings={
                # Insert optimization - smaller blocks to avoid memory spikes
                'max_insert_block_size': '10000',           # Much smaller blocks
                'min_insert_block_size_rows': '1000',       # Smaller minimum
                'min_insert_block_size_bytes': '10485760',  # 10MB instead of 256MB
                'max_insert_threads': '1',                  # Single thread to avoid memory competition
                
                # JSON processing - minimal memory usage
                'input_format_parallel_parsing': '0',        # Disable parallel parsing
                'min_chunk_bytes_for_parallel_parsing': '0', # No parallel parsing threshold
                'output_format_json_quote_64bit_integers': '0',
                'input_format_skip_unknown_fields': '1',
                'input_format_import_nested_json': '1',
                
                # Reduce logging and overhead
                'log_queries': '0',
                'log_query_threads': '0',
                'log_processors_profiles': '0',
                
                # Timeouts
                'max_execution_time': '3600',  # 1 hour max execution
                'send_timeout': '1800',        # 30 min send timeout
                'receive_timeout': '1800',     # 30 min receive timeout
                
                # Compression to reduce network usage
                'network_compression_method': 'lz4'
            }
        )
        
        # Test connection
        client.command("SELECT 1")
        logger.info("ClickHouse Cloud connection established")
        return client
        
    except Exception as e:
        logger.error("Error connecting to ClickHouse Cloud", error=str(e))
        raise

class ClickHouse:
    """ClickHouse Cloud client optimized for memory-constrained environments."""
    
    def __init__(self):
        self.client = connect_clickhouse(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
            secure=config.CLICKHOUSE_SECURE,
            verify=False
        )
        
        # Cache for time parameters
        self._genesis_time = None
        self._seconds_per_slot = None
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]]):
        """Cloud-optimized batch insert with aggressive chunking for large payloads."""
        if not data:
            return
        
        # Calculate total data size
        total_size_mb = self._calculate_data_size_mb(data)
        
        logger.info("Processing batch for cloud insert", 
                   table=table,
                   rows=len(data),
                   estimated_size_mb=round(total_size_mb, 2))
        
        if total_size_mb < 10:  # Very small batch - insert normally
            self._insert_single_batch(table, data)
        elif total_size_mb < 50:  # Medium batch - chunk conservatively
            self._insert_chunked_by_size(table, data, max_chunk_size_mb=5)
        else:
            # Large batch - aggressive chunking for cloud memory limits
            self._insert_chunked_by_size(table, data, max_chunk_size_mb=2)
    
    def _calculate_data_size_mb(self, data: List[Dict[str, Any]]) -> float:
        """Calculate approximate size of data in MB."""
        if not data:
            return 0
        
        # Estimate size based on JSON serialization of first few rows
        sample_size = min(3, len(data))  # Smaller sample to reduce overhead
        sample_data = data[:sample_size]
        
        total_sample_size = 0
        for row in sample_data:
            json_str = json.dumps(row, separators=(',', ':'))
            total_sample_size += len(json_str.encode('utf-8'))
        
        # Estimate total size with 20% buffer for safety
        avg_row_size = total_sample_size / sample_size
        total_size_bytes = avg_row_size * len(data) * 1.2  # 20% buffer
        return total_size_bytes / (1024 * 1024)
    
    def _insert_chunked_by_size(self, table: str, data: List[Dict[str, Any]], max_chunk_size_mb: float):
        """Insert data in very small chunks to avoid cloud memory limits."""
        current_chunk = []
        current_size_mb = 0
        chunks_inserted = 0
        
        for i, row in enumerate(data):
            # Estimate row size (cache for efficiency)
            if i % 100 == 0 or not hasattr(self, '_cached_row_size_mb'):
                row_json = json.dumps(row, separators=(',', ':'))
                self._cached_row_size_mb = len(row_json.encode('utf-8')) / (1024 * 1024)
            
            row_size_mb = self._cached_row_size_mb
            
            # If adding this row would exceed limit, insert current chunk
            if current_chunk and (current_size_mb + row_size_mb) > max_chunk_size_mb:
                success = self._insert_single_batch_safe(table, current_chunk)
                
                if success:
                    chunks_inserted += 1
                    logger.debug("Inserted cloud-safe chunk", 
                               table=table,
                               chunk_num=chunks_inserted,
                               rows=len(current_chunk),
                               size_mb=round(current_size_mb, 2))
                else:
                    # If even a small chunk fails, we need to go smaller
                    logger.warning("Chunk failed, splitting further", 
                                 table=table,
                                 chunk_size=len(current_chunk))
                    self._insert_micro_chunks(table, current_chunk)
                    chunks_inserted += 1
                
                current_chunk = []
                current_size_mb = 0
            
            current_chunk.append(row)
            current_size_mb += row_size_mb
        
        # Insert final chunk
        if current_chunk:
            success = self._insert_single_batch_safe(table, current_chunk)
            if success:
                chunks_inserted += 1
            else:
                self._insert_micro_chunks(table, current_chunk)
                chunks_inserted += 1
        
        logger.info("Completed cloud-safe chunked insertion", 
                   table=table,
                   total_chunks=chunks_inserted,
                   total_rows=len(data))
    
    def _insert_micro_chunks(self, table: str, data: List[Dict[str, Any]]):
        """Insert data in micro chunks (single rows if necessary)."""
        logger.info("Using micro-chunk insertion", table=table, rows=len(data))
        
        # Try progressively smaller chunk sizes
        chunk_sizes = [50, 20, 10, 5, 1]
        
        for chunk_size in chunk_sizes:
            failed_rows = []
            
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                success = self._insert_single_batch_safe(table, chunk)
                
                if not success:
                    failed_rows.extend(chunk)
                else:
                    logger.debug("Micro-chunk succeeded", 
                               table=table,
                               chunk_size=len(chunk))
            
            if not failed_rows:
                logger.info("All micro-chunks succeeded", 
                           table=table,
                           chunk_size=chunk_size)
                return
            else:
                data = failed_rows  # Retry with smaller chunks
                logger.debug("Retrying failed rows with smaller chunks", 
                           table=table,
                           failed_count=len(failed_rows),
                           next_chunk_size=chunk_sizes[chunk_sizes.index(chunk_size) + 1] if chunk_size != 1 else 1)
        
        # If we get here, even single rows are failing
        logger.error("Unable to insert data even with single-row chunks", 
                   table=table, 
                   failed_rows=len(data))
        raise Exception(f"Failed to insert {len(data)} rows into {table} even with micro-chunking")
    
    def _insert_single_batch_safe(self, table: str, data: List[Dict[str, Any]]) -> bool:
        """Insert a single batch with cloud-safe error handling."""
        try:
            # Prepare data with datetime handling
            json_rows = []
            for row in data:
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        dt_without_tz = value.replace(tzinfo=None) if value.tzinfo else value
                        processed_row[key] = dt_without_tz.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(value, bool):
                        processed_row[key] = 1 if value else 0
                    elif value is None:
                        processed_row[key] = None
                    else:
                        processed_row[key] = value
                json_rows.append(json.dumps(processed_row, separators=(',', ':')))
            
            json_data = '\n'.join(json_rows)
            data_size_mb = len(json_data.encode('utf-8')) / (1024 * 1024)
            
            query = f"INSERT INTO {table} FORMAT JSONEachRow"
            
            # Cloud-safe settings - no memory limits, just processing optimization
            insert_settings = {
                'max_insert_block_size': '5000',     # Very small blocks
                'max_insert_threads': '1',           # Single thread
                'input_format_parallel_parsing': '0', # No parallel processing
                'log_queries': '0'                   # Reduce overhead
            }
            
            self.client.command(query, data=json_data, settings=insert_settings)
            
            logger.debug("Cloud batch inserted successfully", 
                       table=table, 
                       rows=len(data),
                       size_mb=round(data_size_mb, 2))
            
            return True
            
        except Exception as e:
            # Check if it's a memory error
            error_str = str(e).lower()
            if 'memory' in error_str or 'limit exceeded' in error_str:
                logger.warning("Memory limit hit, will retry with smaller chunks", 
                              table=table, 
                              rows=len(data),
                              error=str(e)[:200])
                return False
            else:
                # Other errors should be raised
                logger.error("Non-memory batch insert error", 
                            table=table, 
                            rows=len(data), 
                            error=str(e))
                raise
    
    def _insert_single_batch(self, table: str, data: List[Dict[str, Any]]):
        """Legacy method for compatibility - delegates to safe version."""
        success = self._insert_single_batch_safe(table, data)
        if not success:
            raise Exception("Batch insert failed due to memory limits")
    
    # Keep all existing methods for compatibility
    async def init_cache(self):
        """Initialize cache for frequently accessed configuration."""
        if self._genesis_time is None:
            query = "SELECT toUnixTimestamp(genesis_time) as genesis_time FROM genesis LIMIT 1"
            result = self.execute(query)
            if result and len(result) > 0:
                self._genesis_time = result[0].get("genesis_time")
                
        if self._seconds_per_slot is None:
            query = """
            SELECT parameter_value 
            FROM specs 
            WHERE parameter_name = 'SECONDS_PER_SLOT' 
            LIMIT 1
            """
            result = self.execute(query)
            if result and len(result) > 0:
                self._seconds_per_slot = int(result[0].get("parameter_value", 12))
    
    def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query and return results as list of dictionaries."""
        try:
            if params:
                result = self.client.query(query, parameters=params)
            else:
                result = self.client.query(query)
            
            if result.result_rows:
                columns = result.column_names
                return [dict(zip(columns, row)) for row in result.result_rows]
            return []
            
        except Exception as e:
            logger.error("ClickHouse query failed", query=query, error=str(e))
            raise
    
    def claim_chunk(self, worker_id: str, loader_name: str) -> Optional[tuple]:
        """Worker-specific chunk claiming with better distribution."""
        try:
            try:
                worker_num = int(worker_id.split('_')[1])
            except (IndexError, ValueError):
                logger.error("Invalid worker_id format", worker_id=worker_id)
                return None
            
            get_chunks_query = """
            SELECT chunk_id, start_slot, end_slot, chunk_row_num
            FROM (
                SELECT 
                    chunk_id, 
                    start_slot, 
                    end_slot,
                    ROW_NUMBER() OVER (ORDER BY start_slot) - 1 as chunk_row_num
                FROM load_state_chunks FINAL
                WHERE loader_name = {loader_name:String} 
                  AND status = 'pending'
            )
            WHERE chunk_row_num % {total_workers:UInt64} = {worker_num:UInt64}
            ORDER BY start_slot 
            LIMIT 1
            """
            
            chunks = self.execute(get_chunks_query, {
                "loader_name": loader_name,
                "total_workers": config.BACKFILL_WORKERS,
                "worker_num": worker_num
            })
            
            if not chunks:
                logger.debug("No chunks assigned to this worker", 
                            worker=worker_id, 
                            loader=loader_name,
                            worker_num=worker_num,
                            total_workers=config.BACKFILL_WORKERS)
                return None
            
            chunk = chunks[0]
            chunk_id = chunk["chunk_id"]
            start_slot = chunk["start_slot"]
            end_slot = chunk["end_slot"]
            
            claim_query = """
            INSERT INTO load_state_chunks 
            (chunk_id, start_slot, end_slot, loader_name, status, worker_id, created_at, updated_at)
            VALUES ({chunk_id:String}, {start_slot:UInt64}, {end_slot:UInt64}, 
                    {loader_name:String}, 'claimed', {worker_id:String}, now(), now())
            """
            
            self.execute(claim_query, {
                "chunk_id": chunk_id,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "loader_name": loader_name,
                "worker_id": worker_id
            })
            
            logger.info("Successfully claimed chunk", 
                       worker=worker_id, 
                       loader=loader_name,
                       start_slot=start_slot, 
                       end_slot=end_slot,
                       chunk_id=chunk_id,
                       worker_assignment=f"{worker_num}/{config.BACKFILL_WORKERS}")
            
            return start_slot, end_slot
            
        except Exception as e:
            logger.error("Error claiming chunk", 
                        worker=worker_id, 
                        loader=loader_name, 
                        error=str(e))
            return None
    
    def update_chunk_status(self, start_slot: int, end_slot: int, loader_name: str, status: str):
        """Update chunk status using chunk_id for atomic updates."""
        try:
            chunk_id = f"{loader_name}_{start_slot}_{end_slot}"
            
            query = """
            INSERT INTO load_state_chunks 
            (chunk_id, start_slot, end_slot, loader_name, status, worker_id, created_at, updated_at)
            VALUES ({chunk_id:String}, {start_slot:UInt64}, {end_slot:UInt64}, 
                    {loader_name:String}, {status:String}, '', now(), now())
            """
            self.execute(query, {
                "chunk_id": chunk_id,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "loader_name": loader_name,
                "status": status
            })
            
            logger.debug("Updated chunk status", 
                        start_slot=start_slot,
                        end_slot=end_slot,
                        loader=loader_name,
                        status=status,
                        chunk_id=chunk_id)
        except Exception as e:
            logger.error("Failed to update chunk status", 
                        start_slot=start_slot,
                        end_slot=end_slot,
                        loader=loader_name,
                        status=status,
                        error=str(e))

    def get_chunk_counts(self):
        """Get simple status summary."""
        try:
            query = """
            SELECT loader_name, status, count() as count
            FROM load_state_chunks FINAL
            GROUP BY loader_name, status
            ORDER BY loader_name, status
            """
            results = self.execute(query)
            
            counts = {}
            for row in results:
                loader = row["loader_name"]
                status = row["status"]
                count = row["count"]
                
                if loader not in counts:
                    counts[loader] = {}
                counts[loader][status] = count
            
            return counts
        except Exception as e:
            logger.error("Error getting chunk counts", error=str(e))
            return {}

    def get_chunk_overview(self):
        """Get a simple overview of all chunks using FINAL for correct ReplacingMergeTree state."""
        try:
            query = """
            SELECT 
                loader_name,
                count() as total_chunks,
                countIf(status = 'pending') as pending,
                countIf(status = 'claimed') as claimed, 
                countIf(status = 'completed') as completed,
                countIf(status = 'failed') as failed
            FROM load_state_chunks FINAL
            GROUP BY loader_name
            ORDER BY loader_name
            """
            return self.execute(query)
        except Exception as e:
            logger.error("Error getting chunk overview", error=str(e))
            return []