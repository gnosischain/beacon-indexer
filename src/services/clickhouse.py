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
    Connect to ClickHouse using clickhouse_connect and return a Client.
    Raises an exception if connection fails.
    """
    logger.info("Connecting to ClickHouse", 
               host=host, 
               port=port, 
               secure=secure, 
               verify=verify)
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,   # Enable TLS
            verify=verify,   # Verify certificate if True
            # Simple timeout fix for large validator payloads
            send_receive_timeout=600,  # 10 minutes instead of default
            connect_timeout=120        # 2 minutes for connection
        )
        # Quick test
        client.command("SELECT 1")
        logger.info("ClickHouse connection established successfully")
        return client
    except Exception as e:
        logger.error("Error connecting to ClickHouse", error=str(e))
        raise

class ClickHouse:
    """ClickHouse database client using clickhouse-connect."""
    
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
    
    async def init_cache(self):
        """Initialize cache for frequently accessed configuration."""
        if self._genesis_time is None:
            # Get genesis time
            query = "SELECT toUnixTimestamp(genesis_time) as genesis_time FROM genesis LIMIT 1"
            result = self.execute(query)
            if result and len(result) > 0:
                self._genesis_time = result[0].get("genesis_time")
                
        if self._seconds_per_slot is None:
            # Get seconds per slot from specs
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
                # Use parameterized query
                result = self.client.query(query, parameters=params)
            else:
                result = self.client.query(query)
            
            # Convert to list of dictionaries
            if result.result_rows:
                columns = result.column_names
                return [dict(zip(columns, row)) for row in result.result_rows]
            return []
            
        except Exception as e:
            logger.error("ClickHouse query failed", query=query, error=str(e))
            raise
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]]):
        """Insert a batch of data using JSONEachRow format consistently."""
        if not data:
            return
        
        try:
            # Use JSONEachRow format for all tables with proper datetime handling
            json_rows = []
            for row in data:
                # Convert datetime objects to strings for JSON serialization
                processed_row = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        if value.tzinfo is not None:
                            dt_without_tz = value.replace(tzinfo=None)
                        else:
                            dt_without_tz = value
                        processed_row[key] = dt_without_tz.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(value, bool):
                        processed_row[key] = 1 if value else 0
                    elif value is None:
                        processed_row[key] = None
                    else:
                        processed_row[key] = value
                json_rows.append(json.dumps(processed_row))
            
            # Create the data payload
            json_data = '\n'.join(json_rows)
            
            # Use the client's command method with data parameter
            query = f"INSERT INTO {table} FORMAT JSONEachRow"
            
            self.client.command(query, data=json_data, settings={
                'min_chunk_bytes_for_parallel_parsing': 200000000  # 200MB instead of 10MB default
            })
            
            logger.info("Batch inserted", table=table, rows=len(data))
            
        except Exception as e:
            logger.error("Batch insert failed", table=table, rows=len(data), error=str(e))
            # Log the actual data being inserted for debugging
            logger.error("Sample data", sample=data[0] if data else "empty")
            raise
    
    def claim_chunk(self, worker_id: str, loader_name: str) -> Optional[tuple]:
        """Worker-specific chunk claiming with better distribution."""
        try:
            # Extract worker number from worker_id (e.g., "worker_5" -> 5)
            try:
                worker_num = int(worker_id.split('_')[1])
            except (IndexError, ValueError):
                logger.error("Invalid worker_id format", worker_id=worker_id)
                return None
            
            # Use chunk ordering (ROW_NUMBER) instead of start_slot for even distribution
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
            
            # Since only this worker can claim this chunk, claiming is now safe
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
            # Generate the same chunk_id used during creation
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