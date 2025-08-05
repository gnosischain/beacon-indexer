import clickhouse_connect
import json
import asyncio
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
    """ClickHouse client with connection pool for concurrent operations."""
    
    # Class constants - all hardcoded values defined here
    SMALL_BATCH_ROWS = 1000
    MEDIUM_BATCH_ROWS = 10000
    LARGE_BATCH_ROWS = 50000
    SMALL_BATCH_MB = 8
    MEDIUM_BATCH_MB = 32
    LARGE_BATCH_MB = 128
    LARGE_ROW_THRESHOLD = 128 * 1024 * 1024  # 128 MB for large validator payloads
    POOL_SIZE = 10  # Pool of connections
    MAX_ROWS_PER_CHUNK = 10000
    MAX_BYTES_PER_CHUNK = 64 * 1024 * 1024  # 64 MB
    
    def __init__(self):
        # Main client for regular operations
        self.client = connect_clickhouse(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_PORT,
            user=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
            secure=config.CLICKHOUSE_SECURE,
            verify=False
        )
        
        # Connection pool for concurrent writes
        self._connection_pool = []
        self._pool_lock = asyncio.Lock()
        
        # Cache for time parameters
        self._genesis_time = None
        self._seconds_per_slot = None
        
        logger.info("ClickHouse client initialized with connection pool", pool_size=self.POOL_SIZE)

    async def _get_connection_from_pool(self) -> Client:
        """Get a connection from the pool (create if needed)."""
        async with self._pool_lock:
            if self._connection_pool:
                return self._connection_pool.pop()
            else:
                # Create new connection
                return connect_clickhouse(
                    host=config.CLICKHOUSE_HOST,
                    port=config.CLICKHOUSE_PORT,
                    user=config.CLICKHOUSE_USER,
                    password=config.CLICKHOUSE_PASSWORD,
                    database=config.CLICKHOUSE_DATABASE,
                    secure=config.CLICKHOUSE_SECURE,
                    verify=False
                )

    async def _return_connection_to_pool(self, connection: Client):
        """Return a connection to the pool."""
        async with self._pool_lock:
            if len(self._connection_pool) < self.POOL_SIZE:
                self._connection_pool.append(connection)
            # If pool is full, just let the connection be garbage collected

    def insert_batch(self, table: str, data: List[Dict[str, Any]], **kwargs):
        """Simple synchronous insert using main client."""
        if not data:
            return

        try:
            # Use simple insert for synchronous operations
            self._do_simple_insert(self.client, table, data)
            
        except Exception as e:
            logger.error("ClickHouse insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
    
    def claim_chunk(self, worker_id: str, loader_name: str) -> Optional[tuple]:
        """Worker-specific chunk claiming with balanced loader distribution."""
        try:
            # Extract worker number from worker_id (e.g., "worker_5" -> 5)
            try:
                worker_num = int(worker_id.split('_')[1])
            except (IndexError, ValueError):
                logger.error("Invalid worker_id format", worker_id=worker_id)
                return None
            
            # Use loader-specific chunk ordering for balanced distribution
            get_chunks_query = """
            SELECT chunk_id, start_slot, end_slot, loader_row_num
            FROM (
                SELECT 
                    chunk_id, 
                    start_slot, 
                    end_slot,
                    ROW_NUMBER() OVER (PARTITION BY loader_name ORDER BY start_slot) - 1 as loader_row_num
                FROM load_state_chunks FINAL
                WHERE loader_name = {loader_name:String} 
                  AND status = 'pending'
            )
            WHERE loader_row_num % {total_workers:UInt64} = {worker_num:UInt64}
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

    async def insert_batch_concurrent(self, table: str, data: List[Dict[str, Any]]):
        """Async insert using connection from pool - for concurrent operations."""
        if not data:
            return

        connection = await self._get_connection_from_pool()
        try:
            # Run the insert in a thread to avoid blocking
            await asyncio.get_event_loop().run_in_executor(
                None, 
                self._do_simple_insert, 
                connection, 
                table, 
                data
            )
            
        except Exception as e:
            logger.error("Concurrent ClickHouse insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise
        finally:
            await self._return_connection_to_pool(connection)

    def _insert_small_batch(self, table: str, data: List[Dict[str, Any]], column_order):
        """Small batch insert - compatibility method."""
        self._do_simple_insert(self.client, table, data)

    def _insert_medium_batch(self, table: str, data: List[Dict[str, Any]], column_order):
        """Medium batch insert - compatibility method."""  
        self._do_simple_insert(self.client, table, data)

    def _insert_large_batch(self, table: str, data: List[Dict[str, Any]], column_order):
        """Large batch insert - compatibility method."""
        self._do_simple_insert(self.client, table, data)

    def _do_insert(self, table: str, data: List[Dict[str, Any]], column_order, settings, max_rows_per_chunk, max_mb_per_chunk):
        """Legacy compatibility method - just use simple insert."""
        self._do_simple_insert(self.client, table, data)

    def _do_simple_insert(self, client: Client, table: str, data: List[Dict[str, Any]]):
        """Simple insert using any client connection."""
        if not data:
            return

        # Decide column order
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
            # Keep arrays as Python lists for ClickHouse Array types
            if isinstance(v, list):
                normalized_array = []
                for item in v:
                    if isinstance(item, str) and item.isdigit():
                        normalized_array.append(int(item))
                    elif isinstance(item, int):
                        normalized_array.append(item)
                    elif isinstance(item, str):
                        normalized_array.append(item)
                    else:
                        normalized_array.append(str(item))
                return normalized_array
            # Only stringify dictionaries
            if isinstance(v, dict):
                return json.dumps(v, ensure_ascii=False)
            return v

        def as_row_list(r: Dict[str, Any]):
            return [norm(r.get(c)) for c in cols]

        # Convert all data to row format
        rows = [as_row_list(r) for r in data]
        
        # Reduced memory settings for ClickHouse Cloud
        settings = {
            'max_insert_block_size': 1000,       # Much smaller blocks
            'max_memory_usage': '200000000',     # 200MB instead of 1GB
            'input_format_parallel_parsing': 0,
            'max_bytes_before_external_sort': '100000000',      # 100MB
            'max_bytes_before_external_group_by': '100000000'   # 100MB
        }
        
        client.insert(
            table,
            rows,
            column_names=cols,
            column_oriented=False,
            settings=settings
        )

    def insert_batch_optimized(self, table: str, data: List[Dict[str, Any]], **kwargs):
        """Simple optimized insert - just use the main client."""
        self.insert_batch(table, data)

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

    def insert_batch(
        self,
        table: str,
        data: List[Dict[str, Any]],
        column_order: Optional[List[str]] = None
    ):
        """
        Robust insert with large data handling capability.
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

        # Check for very large single rows
        for i, r in enumerate(data):
            size_i = sum(approx_size(v) for v in as_row_list(r))
            if size_i >= self.LARGE_ROW_THRESHOLD:
                logger.warning("Very large single row detected", 
                              table=table, 
                              row_index=i, 
                              size_mb=round(size_i/1024/1024, 2))
                try:
                    # Try column-oriented RowBinary first (different encode path)
                    col_data = {c: [] for c in cols}
                    for c in cols:
                        v = norm(r.get(c))
                        # Hint to driver: encode big Strings as bytes to skip extra work
                        if isinstance(v, str) and len(v) >= self.LARGE_ROW_THRESHOLD:
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
                            }
                        )
                        logger.info("Inserted large row via JSONEachRow with parallel parsing OFF", table=table)
                        return
                    except Exception as e2:
                        logger.error("JSONEachRow fallback also failed", table=table, error=str(e2))
                        raise

        # Normal path: chunked RowBinary, row-oriented
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
                    if isinstance(v, str) and len(v) >= self.LARGE_ROW_THRESHOLD:
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
            if buf and (len(buf) >= self.MAX_ROWS_PER_CHUNK or buf_bytes + rb > self.MAX_BYTES_PER_CHUNK):
                flush()
            buf.append(lst)
            buf_bytes += rb
            if buf_bytes > self.MAX_BYTES_PER_CHUNK:
                flush()

        flush()

    def _insert_small_batch(self, table: str, data: List[Dict[str, Any]], column_order: Optional[List[str]]):
        """Optimized insert for small batches (< 1K rows)."""
        try:
            # For small batches, use simple row-oriented insert with minimal settings
            settings = {
                'max_insert_block_size': 8192,
                'max_memory_usage': '500000000',  # 500MB
                'input_format_parallel_parsing': 0
            }
            
            self._do_insert(table, data, column_order, settings, max_rows_per_chunk=1000, max_mb_per_chunk=self.SMALL_BATCH_MB)
            
            logger.debug("Small batch inserted", 
                        table=table, 
                        rows=len(data))
            
        except Exception as e:
            logger.error("Small batch insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise

    def _insert_medium_batch(self, table: str, data: List[Dict[str, Any]], column_order: Optional[List[str]]):
        """Optimized insert for medium batches (1K-10K rows)."""
        try:
            # For medium batches, use balanced settings
            settings = {
                'max_insert_block_size': 16384,
                'max_memory_usage': '1000000000',  # 1GB
                'max_bytes_before_external_group_by': '300000000',
                'input_format_parallel_parsing': 0
            }
            
            self._do_insert(table, data, column_order, settings, max_rows_per_chunk=5000, max_mb_per_chunk=self.MEDIUM_BATCH_MB)
            
            logger.debug("Medium batch inserted", 
                        table=table, 
                        rows=len(data))
            
        except Exception as e:
            logger.error("Medium batch insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise

    def _insert_large_batch(self, table: str, data: List[Dict[str, Any]], column_order: Optional[List[str]]):
        """Optimized insert for large batches (10K+ rows)."""
        try:
            # For large batches, use chunking with optimized settings
            settings = {
                'max_insert_block_size': 32768,
                'max_memory_usage': '2000000000',  # 2GB
                'max_bytes_before_external_group_by': '500000000',
                'max_bytes_before_external_sort': '500000000',
                'input_format_parallel_parsing': 0
            }
            
            # Process in chunks to manage memory
            chunk_size = 10000
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                self._do_insert(table, chunk, column_order, settings, max_rows_per_chunk=10000, max_mb_per_chunk=self.LARGE_BATCH_MB)
                
                logger.debug("Large batch chunk inserted", 
                            table=table, 
                            chunk_rows=len(chunk),
                            progress=f"{min(i + chunk_size, len(data))}/{len(data)}")
            
            logger.info("Large batch completed", 
                       table=table, 
                       total_rows=len(data))
            
        except Exception as e:
            logger.error("Large batch insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise

    def _do_insert(
        self, 
        table: str, 
        data: List[Dict[str, Any]], 
        column_order: Optional[List[str]], 
        settings: Dict[str, Any],
        max_rows_per_chunk: int,
        max_mb_per_chunk: int
    ):
        """Core insert logic with chunking and error handling."""
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
            # CRITICAL: Keep arrays as Python lists for ClickHouse Array types
            if isinstance(v, list):
                normalized_array = []
                for item in v:
                    if isinstance(item, str) and item.isdigit():
                        normalized_array.append(int(item))
                    elif isinstance(item, int):
                        normalized_array.append(item)
                    elif isinstance(item, str):
                        normalized_array.append(item)
                    else:
                        normalized_array.append(str(item))
                return normalized_array
            # Only stringify dictionaries
            if isinstance(v, dict):
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

        # Check for very large single rows
        for i, r in enumerate(data):
            size_i = sum(approx_size(v) for v in as_row_list(r))
            if size_i >= self.LARGE_ROW_THRESHOLD:
                logger.warning("Very large single row detected", 
                              table=table, 
                              row_index=i, 
                              size_mb=round(size_i/1024/1024, 2))
                
                # Handle large row with JSONEachRow
                try:
                    jr = {}
                    for c in cols:
                        v = r.get(c)
                        if isinstance(v, datetime):
                            v = v.replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
                        elif isinstance(v, bytes):
                            import base64
                            v = base64.b64encode(v).decode('ascii')
                        jr[c] = v
                    
                    line = json.dumps(jr, ensure_ascii=False)
                    self.client.command(
                        f"INSERT INTO {table} FORMAT JSONEachRow",
                        data=line + "\n",
                        settings={**settings, 'input_format_parallel_parsing': 0}
                    )
                    logger.info("Large row inserted via JSONEachRow", table=table)
                    return
                    
                except Exception as e2:
                    logger.error("Large row JSONEachRow fallback failed", table=table, error=str(e2))
                    raise

        # Normal chunked processing
        buf: List[List[Any]] = []
        buf_bytes = 0
        max_bytes_per_chunk = max_mb_per_chunk * 1024 * 1024

        def flush():
            nonlocal buf, buf_bytes
            if not buf:
                return
            
            try:
                self.client.insert(
                    table,
                    buf,
                    column_names=cols,
                    column_oriented=False,
                    settings=settings
                )
                
                buf = []
                buf_bytes = 0
                
            except Exception as e:
                logger.error("Chunk insert failed", 
                           table=table, 
                           rows=len(buf), 
                           size_mb=round(buf_bytes/1024/1024, 2),
                           error=str(e))
                buf = []
                buf_bytes = 0
                raise

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

    def insert_batch_optimized(self, table: str, data: List[Dict[str, Any]], **kwargs):
        """Ultra-optimized insert for high-performance scenarios."""
        if not data:
            return
        
        batch_size = len(data)
        
        # For validators, use much smaller chunks if configured
        if table == "validators":
            # Check if we have a small batch size configured (< 1000)
            # This indicates we want small, fast inserts
            if batch_size <= 1000:
                # Use small batch optimization for small validator batches
                self._insert_small_batch(table, data, None)
                logger.debug("Used small batch optimization for validators", 
                           table=table, 
                           rows=batch_size)
                return
            elif batch_size > 5000:
                # For large validator batches, use smaller chunks
                chunk_size = 1000  # Much smaller chunks for validators
                
                for i in range(0, batch_size, chunk_size):
                    chunk = data[i:i + chunk_size]
                    self._insert_small_batch(table, chunk, None)
                    
                    # Log progress less frequently
                    if i % 5000 == 0:
                        logger.debug("Validator batch progress", 
                                   table=table,
                                   processed=min(i + chunk_size, batch_size),
                                   total=batch_size)
                
                logger.info("Completed validator batch", 
                           table=table, 
                           total_rows=batch_size)
                return
        
        # Use normal smart batching for other tables
        self.insert_batch(table, data)