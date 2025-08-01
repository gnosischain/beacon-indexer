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
            # Increased timeouts for large validator payloads
            send_receive_timeout=1800,  # 30 minutes
            connect_timeout=300         # 5 minutes
        )
        # Quick test
        client.command("SELECT 1")
        logger.info("ClickHouse connection established successfully")
        return client
    except Exception as e:
        logger.error("Error connecting to ClickHouse", error=str(e))
        raise

class ClickHouse:
    """ClickHouse database client using clickhouse-connect with Native protocol for large payloads."""
    
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

    def insert_batch(
        self,
        table: str,
        data: List[Dict[str, Any]],
        column_order: Optional[List[str]] = None,
        max_rows_per_chunk: int = 10_000,
        max_bytes_per_chunk: int = 64 * 1024 * 1024,  # 64 MB
        large_row_threshold: int = 128 * 1024 * 1024  # 128 MB
    ):
        """
        Robust insert with:
        - RowBinary (client.insert) for normal rows
        - Column-oriented encode for very large rows (different path in driver)
        - JSONEachRow fallback with parallel parsing OFF for a single huge row
        """
        if not data:
            return

        # Decide column order
        if column_order:
            cols = list(column_order)
        else:
            cols = list(data[0].keys())
            for r in data[1:]:
                for k in r.keys():
                    if k not in cols:
                        cols.append(k)

        def norm(v):
            from datetime import datetime
            if isinstance(v, datetime):
                return v.replace(tzinfo=None)
            if isinstance(v, bool):
                return 1 if v else 0
            if isinstance(v, (dict, list)):
                return json.dumps(v, ensure_ascii=False)
            return v

        def as_row_list(r: Dict[str, Any]):
            return [norm(r.get(c)) for c in cols]

        def approx_size(x: Any) -> int:
            if x is None:
                return 1
            if isinstance(x, (bytes, bytearray, memoryview)):
                return len(x)
            if isinstance(x, str):
                return len(x.encode('utf-8', 'ignore'))
            if isinstance(x, (int, float)):
                return 8
            s = str(x)
            return len(s.encode('utf-8', 'ignore'))

        # --- detect very large single rows ---
        for i, r in enumerate(data):
            size_i = sum(approx_size(v) for v in as_row_list(r))
            if size_i >= large_row_threshold:
                logger.warning("Very large single row detected", table=table, row_index=i, size_mb=round(size_i/1024/1024, 2))
                try:
                    # Try column-oriented RowBinary first (different encode path)
                    col_data = {c: [] for c in cols}
                    for c in cols:
                        v = norm(r.get(c))
                        # Hint to driver: encode big Strings as bytes to skip extra work
                        if isinstance(v, str) and len(v) >= large_row_threshold:
                            v = v.encode('utf-8', 'ignore')
                        col_data[c].append(v)
                    self.client.insert(
                        table,
                        [col_data[c] for c in cols],
                        column_names=cols,
                        column_oriented=True,
                    )
                    logger.info("Inserted large row via column-oriented RowBinary", table=table)
                    return
                except Exception as e1:
                    logger.error("Column-oriented RowBinary failed for large row, falling back to JSONEachRow",
                                table=table, error=str(e1))
                    # Last-resort fallback: JSONEachRow with parallel parsing disabled
                    try:
                        jr = {}
                        for c in cols:
                            v = r.get(c)
                            if isinstance(v, datetime):
                                v = v.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
                            elif isinstance(v, (dict, list)):
                                v = v  # let json.dumps handle it
                            elif isinstance(v, bytes):
                                # Store bytes as base64 if your schema expects String
                                import base64
                                v = base64.b64encode(v).decode('ascii')
                            jr[c] = v
                        line = json.dumps(jr, ensure_ascii=False)
                        self.client.command(
                            f"INSERT INTO {table} FORMAT JSONEachRow",
                            data=line + "\n",
                            settings={
                                # Key mitigations for server-side JSON parsing memory
                                'input_format_parallel_parsing': 0,
                                'min_chunk_bytes_for_parallel_parsing': 1048576,  # 1 MB
                                # Optional guardrails; tune to your service size
                                # 'max_memory_usage': 4_000_000_000,
                            }
                        )
                        logger.info("Inserted large row via JSONEachRow with parallel parsing OFF", table=table)
                        return
                    except Exception as e2:
                        logger.error("JSONEachRow fallback also failed", table=table, error=str(e2))
                        raise

        # --- normal path: chunked RowBinary, row-oriented ---
        buf: List[List[Any]] = []
        buf_bytes = 0

        def flush():
            nonlocal buf, buf_bytes
            if not buf:
                return
            # Encode very long Strings as bytes to reduce client-side overhead
            for r in range(len(buf)):
                for c in range(len(cols)):
                    v = buf[r][c]
                    if isinstance(v, str) and len(v) >= large_row_threshold:
                        buf[r][c] = v.encode('utf-8', 'ignore')
            self.client.insert(
                table,
                buf,
                column_names=cols,
                column_oriented=False,
            )
            logger.info("Chunk inserted", table=table, rows=len(buf), size_mb=round(buf_bytes/1024/1024, 2))
            buf = []
            buf_bytes = 0

        for r in data:
            lst = as_row_list(r)
            rb = sum(approx_size(v) for v in lst)
            if buf and (len(buf) >= max_rows_per_chunk or buf_bytes + rb > max_bytes_per_chunk):
                flush()
            buf.append(lst)
            buf_bytes += rb
            if buf_bytes > max_bytes_per_chunk:
                flush()

        flush()


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