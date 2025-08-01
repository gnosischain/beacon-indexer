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
    Connect to ClickHouse Cloud with minimal, safe settings.
    """
    logger.info("Connecting to ClickHouse Cloud with minimal settings", 
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
            connect_timeout=300         # 5 minutes
            # No settings - let ClickHouse Cloud use its defaults
        )
        
        # Test connection
        client.command("SELECT 1")
        logger.info("ClickHouse Cloud connection established")
        return client
        
    except Exception as e:
        logger.error("Error connecting to ClickHouse Cloud", error=str(e))
        raise

class ClickHouse:
    """Minimal ClickHouse Cloud client that works within cloud constraints."""
    
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
        """Cloud-optimized batch insert with progressive chunking."""
        if not data:
            return
        
        # Calculate total data size
        total_size_mb = self._calculate_data_size_mb(data)
        
        logger.info("Processing batch for cloud insert", 
                   table=table,
                   rows=len(data),
                   estimated_size_mb=round(total_size_mb, 2))
        
        # Start with reasonable chunks and get smaller if needed
        max_chunk_sizes_mb = [10, 5, 2, 1, 0.5]  # Try these chunk sizes in order
        
        for max_chunk_size_mb in max_chunk_sizes_mb:
            try:
                if total_size_mb <= max_chunk_size_mb:
                    # Small enough - try single batch
                    self._insert_single_batch_cloud_safe(table, data)
                    logger.info("Single batch insert successful", 
                               table=table, 
                               rows=len(data),
                               size_mb=round(total_size_mb, 2))
                    return
                else:
                    # Chunk it
                    self._insert_chunked_by_size_cloud_safe(table, data, max_chunk_size_mb)
                    logger.info("Chunked insert successful", 
                               table=table,
                               rows=len(data),
                               chunk_size_mb=max_chunk_size_mb)
                    return
                    
            except Exception as e:
                error_str = str(e).lower()
                if 'memory' in error_str or 'limit exceeded' in error_str:
                    logger.warning("Memory limit hit, trying smaller chunks", 
                                  table=table,
                                  chunk_size_mb=max_chunk_size_mb,
                                  error=str(e)[:100])
                    continue  # Try next smaller chunk size
                else:
                    # Non-memory error - re-raise
                    raise
        
        # If we get here, even the smallest chunks failed
        raise Exception(f"Unable to insert data into {table} even with 0.5MB chunks")
    
    def _calculate_data_size_mb(self, data: List[Dict[str, Any]]) -> float:
        """Calculate approximate size of data in MB."""
        if not data:
            return 0
        
        # Sample a few rows to estimate size
        sample_size = min(3, len(data))
        sample_data = data[:sample_size]
        
        total_sample_size = 0
        for row in sample_data:
            json_str = json.dumps(row, separators=(',', ':'))
            total_sample_size += len(json_str.encode('utf-8'))
        
        # Estimate total size with buffer
        avg_row_size = total_sample_size / sample_size
        total_size_bytes = avg_row_size * len(data) * 1.1  # 10% buffer
        return total_size_bytes / (1024 * 1024)
    
    def _insert_chunked_by_size_cloud_safe(self, table: str, data: List[Dict[str, Any]], max_chunk_size_mb: float):
        """Insert data in chunks, cloud-safe version."""
        current_chunk = []
        current_size_mb = 0
        chunks_inserted = 0
        
        # Estimate row size from first row
        if data:
            first_row_json = json.dumps(data[0], separators=(',', ':'))
            estimated_row_size_mb = len(first_row_json.encode('utf-8')) / (1024 * 1024)
        else:
            return
        
        for row in data:
            # Use estimated size for efficiency
            row_size_mb = estimated_row_size_mb
            
            # If adding this row would exceed limit, insert current chunk
            if current_chunk and (current_size_mb + row_size_mb) > max_chunk_size_mb:
                self._insert_single_batch_cloud_safe(table, current_chunk)
                chunks_inserted += 1
                
                logger.debug("Inserted cloud chunk", 
                           table=table,
                           chunk_num=chunks_inserted,
                           rows=len(current_chunk),
                           size_mb=round(current_size_mb, 2))
                
                current_chunk = []
                current_size_mb = 0
            
            current_chunk.append(row)
            current_size_mb += row_size_mb
        
        # Insert final chunk
        if current_chunk:
            self._insert_single_batch_cloud_safe(table, current_chunk)
            chunks_inserted += 1
        
        logger.info("Completed cloud chunked insertion", 
                   table=table,
                   total_chunks=chunks_inserted,
                   total_rows=len(data))
    
    def _insert_single_batch_cloud_safe(self, table: str, data: List[Dict[str, Any]]):
        """Insert a single batch with minimal settings for cloud compatibility."""
        if not data:
            return
            
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
            
            # Use NO custom settings - let ClickHouse Cloud handle it
            self.client.command(query, data=json_data)
            
            logger.debug("Cloud batch inserted successfully", 
                       table=table, 
                       rows=len(data),
                       size_mb=round(data_size_mb, 2))
            
        except Exception as e:
            logger.error("Cloud batch insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise
    
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