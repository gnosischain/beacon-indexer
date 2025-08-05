import asyncio
import time
from typing import List, Dict, Tuple, Optional
from src.config import config
from src.services.storage_factory import create_storage
from src.parsers.validators import ValidatorsParser
from src.parsers.rewards import RewardsParser
from src.utils.logger import logger

class TransformerService:
    """Simple, fast transformer with parallel writes."""
    
    # Class constants - all hardcoded values defined here
    MAX_CONCURRENT_WRITES = 8      # Write 8 batches simultaneously 
    VALIDATOR_WRITE_CHUNK = 5000   # Write 5K validators per chunk
    CHUNKS_PER_FETCH = 50          # Chunks to fetch per round
    VALIDATORS_BATCH_SIZE = 20000  # Validator batch processing size
    CHUNKS_PER_BATCH = 20          # Process chunks in parallel batches
    
    def __init__(self):
        self.storage = create_storage()
        self.enabled_loaders = set(config.ENABLED_LOADERS)
        
        if config.STORAGE_BACKEND.lower() == "clickhouse":
            from src.services.fork import ForkDetectionService
            from src.parsers.factory import ParserFactory
            self.fork_service = ForkDetectionService(clickhouse_client=self.storage)
            self.parser_factory = ParserFactory(self.fork_service)
            
            self.loader_configs = {
                "blocks": {"raw_table": "raw_blocks", "use_final": False, "fork_aware": True},
                "validators": {"raw_table": "raw_validators", "use_final": True, "fork_aware": False},
                "rewards": {"raw_table": "raw_rewards", "use_final": True, "fork_aware": False}
            }
            
            # Only include enabled loaders
            self.loader_configs = {
                name: config_data for name, config_data in self.loader_configs.items() 
                if name in self.enabled_loaders
            }
        else:
            self.fork_service = None
            self.parser_factory = None
            self.loader_configs = {}
        
        self.validator_parser = ValidatorsParser()
        self.rewards_parser = RewardsParser()
        
        # Write semaphore for parallel writes
        self._write_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_WRITES)
        
        logger.info("Simple transformer initialized", 
                   storage_backend=config.STORAGE_BACKEND,
                   enabled_loaders=list(self.enabled_loaders),
                   max_concurrent_writes=self.MAX_CONCURRENT_WRITES,
                   validator_write_chunk=self.VALIDATOR_WRITE_CHUNK,
                   chunks_per_fetch=self.CHUNKS_PER_FETCH)
    
    async def initialize(self):
        """Initialize the transformer service."""
        if self.fork_service:
            logger.info("Fork service initialized", 
                       network=self.fork_service.get_network_name())
        
        # Clean up stale processing records
        if config.STORAGE_BACKEND.lower() == "clickhouse":
            try:
                stale_query = """
                SELECT raw_table_name, start_slot, end_slot
                FROM transformer_progress FINAL
                WHERE status = 'processing'
                AND processed_at < now() - INTERVAL 30 MINUTE
                """
                
                stale_records = self.storage.execute(stale_query)
                
                for record in stale_records:
                    self._record_processing_result(
                        record["raw_table_name"],
                        record["start_slot"],
                        record["end_slot"],
                        'failed',
                        0, 1,
                        'Reset stale processing record on startup'
                    )
                
                if stale_records:
                    logger.info("Reset stale processing records", count=len(stale_records))
                    
            except Exception as e:
                logger.warning("Failed to cleanup stale processing records", error=str(e))
    
    async def run(self, batch_size: int = 100, continuous: bool = False):
        """Run simple transformer."""
        logger.info("Starting simple transformer", 
                   mode="continuous" if continuous else "batch",
                   enabled_loaders=list(self.enabled_loaders))
        
        await self.initialize()
        
        if config.STORAGE_BACKEND.lower() == "parquet":
            await self._run_parquet_transformer(continuous)
        else:
            await self._run_clickhouse_transformer(continuous)
    
    async def _run_parquet_transformer(self, continuous: bool):
        """Simple Parquet transformer."""
        logger.info("Running Parquet transformer")
        # Keep existing parquet logic - it's simple enough
        pass
    
    async def _run_clickhouse_transformer(self, continuous: bool):
        """Simple ClickHouse transformer with parallel writes."""
        consecutive_empty_rounds = 0
        max_empty_rounds = 3 if not continuous else float('inf')
        
        logger.info("Starting simple ClickHouse transformer", 
                   enabled_loaders=list(self.loader_configs.keys()),
                   chunks_per_fetch=self.CHUNKS_PER_FETCH)
        
        while consecutive_empty_rounds < max_empty_rounds:
            try:
                processed_any = False
                
                for loader_name in self.loader_configs.keys():
                    # Get chunks to process
                    chunks = self._get_untransformed_chunks(loader_name)
                    
                    if chunks:
                        processed_any = True
                        logger.info("Processing chunks", 
                                   loader=loader_name, 
                                   chunks=len(chunks))
                        
                        # Process chunks in parallel
                        chunk_tasks = []
                        for i, chunk in enumerate(chunks[:self.CHUNKS_PER_BATCH]):  # Use class constant
                            task = asyncio.create_task(
                                self._process_chunk_simple(loader_name, chunk)
                            )
                            chunk_tasks.append(task)
                            
                            # Log progress every 5 chunks
                            if (i + 1) % 5 == 0:
                                logger.info("Started chunk batch", 
                                           loader=loader_name,
                                           started=i + 1,
                                           total=min(len(chunks), self.CHUNKS_PER_BATCH))
                        
                        # Wait for all chunks to complete with progress updates
                        logger.info("Processing chunks in parallel", 
                                   loader=loader_name,
                                   total_chunks=len(chunk_tasks))
                        
                        results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
                        successful = sum(1 for r in results if r is True)
                        failed = len(results) - successful
                        
                        # Log failures for debugging
                        if failed > 0:
                            failed_errors = [str(r) for r in results if r is not True]
                            logger.error("Chunk processing failures", 
                                       loader=loader_name,
                                       failed=failed,
                                       errors=failed_errors[:3])  # Show first 3 errors
                        
                        logger.info("Chunks completed", 
                                   loader=loader_name,
                                   successful=successful,
                                   failed=failed,
                                   total=len(chunk_tasks))
                
                if processed_any:
                    consecutive_empty_rounds = 0
                else:
                    consecutive_empty_rounds += 1
                    if continuous:
                        logger.info("No new chunks to transform, waiting...")
                        await asyncio.sleep(10)
                    else:
                        await asyncio.sleep(1)
                        
            except Exception as e:
                logger.error("Error in transformer", error=str(e))
                if continuous:
                    await asyncio.sleep(30)
                else:
                    break
        
        if not continuous:
            logger.info("Simple batch processing completed")
    
    def _get_untransformed_chunks(self, loader_name: str) -> List[Dict]:
        """Get chunks that need transforming."""
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        
        query = """
        SELECT 
            lsc.chunk_id, 
            lsc.start_slot, 
            lsc.end_slot, 
            lsc.loader_name
        FROM load_state_chunks lsc FINAL
        LEFT JOIN (
            SELECT DISTINCT raw_table_name, start_slot, end_slot
            FROM transformer_progress FINAL
            WHERE raw_table_name = {raw_table:String}
            AND status = 'completed'
        ) tp ON (
            tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
        )
        WHERE lsc.loader_name = {loader_name:String}
        AND lsc.status = 'completed'
        AND tp.start_slot IS NULL
        ORDER BY lsc.start_slot
        LIMIT {limit:UInt64}
        SETTINGS join_use_nulls = 1
        """
        
        return self.storage.execute(query, {
            "loader_name": loader_name,
            "raw_table": raw_table,
            "limit": self.CHUNKS_PER_FETCH
        })

    async def _process_chunk_simple(self, loader_name: str, chunk: Dict) -> bool:
        """Process chunk - simple approach."""
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        start_slot = chunk["start_slot"]
        end_slot = chunk["end_slot"]
        
        logger.debug("Processing chunk", 
                    loader=loader_name,
                    start_slot=start_slot,
                    end_slot=end_slot)
        
        # Mark as processing
        self._record_processing_started(raw_table, start_slot, end_slot)
        
        try:
            # Get ALL data for this chunk
            raw_data = self._get_chunk_data_simple(loader_name, start_slot, end_slot)
            
            if not raw_data:
                self._record_processing_result(raw_table, start_slot, end_slot, 'completed', 0, 0, 'No data')
                return True
            
            # Parse ALL data
            if config_data["fork_aware"]:
                all_tables_data, processed_count, failed_count = await self._process_fork_aware_batch(raw_data, loader_name)
            else:
                all_tables_data, processed_count, failed_count = await self._process_non_fork_aware_batch(raw_data, loader_name)
            
            if failed_count > 0:
                self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                             processed_count, failed_count, f'{failed_count} parse failures')
                return False
            
            # Write ALL data in parallel chunks
            if all_tables_data:
                try:
                    await self._write_data_parallel(all_tables_data)
                except Exception as write_error:
                    error_msg = f"Write error: {str(write_error)}"
                    self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                                 processed_count, 1, error_msg)
                    
                    logger.error("Write failed for chunk", 
                                loader=loader_name,
                                start_slot=start_slot,
                                end_slot=end_slot,
                                error=str(write_error))
                    return False
            
            # Mark as completed
            self._record_processing_result(raw_table, start_slot, end_slot, 'completed', processed_count, 0, '')
            
            logger.debug("Chunk completed", 
                        loader=loader_name,
                        start_slot=start_slot,
                        end_slot=end_slot,
                        processed=processed_count)
            
            return True
            
        except Exception as e:
            error_msg = f"Processing error: {str(e)}"
            self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 0, 1, error_msg)
            
            logger.error("Chunk processing failed", 
                        loader=loader_name,
                        start_slot=start_slot,
                        end_slot=end_slot,
                        error=str(e))
            return False

    def _get_chunk_data_simple(self, loader_name: str, start_slot: int, end_slot: int) -> List[Dict]:
        """Get chunk data - simple, no limits."""
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        use_final = config_data["use_final"]
        
        if use_final:
            query = f"""
            SELECT slot, payload, payload_hash
            FROM {raw_table} FINAL
            WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
            ORDER BY slot
            """
        else:
            query = f"""
            SELECT slot, payload, payload_hash
            FROM (
                SELECT 
                    slot, payload, payload_hash,
                    ROW_NUMBER() OVER (PARTITION BY slot ORDER BY retrieved_at DESC) as rn
                FROM {raw_table}
                WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
            )
            WHERE rn = 1
            ORDER BY slot
            """
        
        result = self.storage.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
    async def _write_data_parallel(self, all_tables_data: Dict[str, List[Dict]]):
        """Write data in parallel chunks - THIS IS THE KEY OPTIMIZATION."""
        write_tasks = []
        
        for table_name, rows in all_tables_data.items():
            if not rows:
                continue
            
            if table_name == "validators" and len(rows) > self.VALIDATOR_WRITE_CHUNK:
                # Split validators into parallel write chunks
                for i in range(0, len(rows), self.VALIDATOR_WRITE_CHUNK):
                    chunk = rows[i:i + self.VALIDATOR_WRITE_CHUNK]
                    task = asyncio.create_task(
                        self._write_chunk_with_semaphore(table_name, chunk)
                    )
                    write_tasks.append(task)
            else:
                # Write smaller tables normally
                task = asyncio.create_task(
                    self._write_chunk_with_semaphore(table_name, rows)
                )
                write_tasks.append(task)
        
        # Execute all writes in parallel
        if write_tasks:
            try:
                logger.debug("Starting parallel writes", 
                           total_write_tasks=len(write_tasks),
                           estimated_rows=sum(len(rows) for rows in all_tables_data.values()))
                
                await asyncio.gather(*write_tasks)
                
                logger.debug("Parallel writes completed", 
                            total_write_tasks=len(write_tasks))
            except Exception as e:
                logger.error("Parallel write failed", 
                           total_write_tasks=len(write_tasks),
                           error=str(e))
                raise
    
    async def _write_chunk_with_semaphore(self, table_name: str, data: List[Dict]):
        """Write a chunk of data with concurrency control using separate connections."""
        async with self._write_semaphore:
            try:
                # Use concurrent insert method that gets its own connection
                await self.storage.insert_batch_concurrent(table_name, data)
                
                logger.debug("Write chunk completed", 
                            table=table_name, 
                            rows=len(data))
            except Exception as e:
                logger.error("Write chunk failed", 
                           table=table_name,
                           rows=len(data),
                           error=str(e))
                raise
    
    async def _process_fork_aware_batch(self, raw_data: List[Dict], loader_name: str) -> Tuple[Dict[str, List[Dict]], int, int]:
        """Process fork-aware data."""
        loader_table_filter = {
            "blocks": {
                "blocks", "attestations", "deposits", "voluntary_exits", 
                "proposer_slashings", "attester_slashings", "sync_aggregates", 
                "execution_payloads", "transactions", "withdrawals", "bls_changes", 
                "blob_sidecars", "blob_commitments", "execution_requests"
            },
            "validators": {"validators"},
            "rewards": {"rewards"}
        }
        
        allowed_tables = loader_table_filter.get(loader_name, set())
        
        # Group data by fork
        fork_groups = {}
        for item in raw_data:
            slot = item["slot"]
            fork_info = self.fork_service.get_fork_at_slot(slot)
            fork_name = fork_info.name
            
            if fork_name not in fork_groups:
                fork_groups[fork_name] = []
            fork_groups[fork_name].append(item)
        
        total_processed = 0
        total_failed = 0
        all_tables_data = {}
        
        # Process each fork group
        for fork_name, fork_data in fork_groups.items():
            try:
                parser = self.parser_factory.get_parser_for_fork(fork_name)
                
                for raw_item in fork_data:
                    try:
                        parsed_data = parser.parse(raw_item)
                        
                        if parsed_data:
                            for table_name, rows in parsed_data.items():
                                if table_name in allowed_tables and rows:
                                    if table_name not in all_tables_data:
                                        all_tables_data[table_name] = []
                                    all_tables_data[table_name].extend(rows)
                        
                        total_processed += 1
                            
                    except Exception as e:
                        logger.error("Fork parser failed", 
                                    parser=parser.fork_name, 
                                    slot=raw_item.get("slot"), 
                                    error=str(e))
                        total_failed += 1
                
            except Exception as e:
                logger.error("Fork batch processing failed", 
                           fork=fork_name,
                           items=len(fork_data),
                           error=str(e))
                total_failed += len(fork_data)
        
        return all_tables_data, total_processed, total_failed
    
    async def _process_non_fork_aware_batch(self, raw_data: List[Dict], loader_name: str) -> Tuple[Dict[str, List[Dict]], int, int]:
        """Process non-fork-aware data."""
        if loader_name == "validators":
            parser = self.validator_parser
            target_table = "validators"
        elif loader_name == "rewards":
            parser = self.rewards_parser
            target_table = "rewards"
        else:
            raise ValueError(f"Unknown non-fork-aware loader: {loader_name}")
        
        processed_count = 0
        failed_count = 0
        all_tables_data = {target_table: []}
        
        for raw_item in raw_data:
            try:
                parsed_data = parser.parse(raw_item)
                
                if parsed_data and target_table in parsed_data and parsed_data[target_table]:
                    all_tables_data[target_table].extend(parsed_data[target_table])
                
                processed_count += 1
                    
            except Exception as e:
                logger.error(f"{loader_name.title()} parser failed", 
                           slot=raw_item.get("slot"), 
                           error=str(e))
                failed_count += 1
        
        return all_tables_data, processed_count, failed_count
    
    def _record_processing_started(self, raw_table: str, start_slot: int, end_slot: int):
        """Record that processing has started."""
        row = {
            "raw_table_name": raw_table,
            "start_slot": start_slot,
            "end_slot": end_slot,
            "status": "processing",
            "processed_count": 0,
            "failed_count": 0,
            "error_message": "",
            "processed_at": self.storage.execute("SELECT now() as now")[0]["now"]
        }
        
        self.storage.insert_batch("transformer_progress", [row])

    def _record_processing_result(self, raw_table: str, start_slot: int, end_slot: int, 
                                 status: str, processed_count: int, failed_count: int, 
                                 error_message: str):
        """Record the final result of processing a range."""
        row = {
            "raw_table_name": raw_table,
            "start_slot": start_slot,
            "end_slot": end_slot,
            "status": status,
            "processed_count": processed_count,
            "failed_count": failed_count,
            "error_message": error_message,
            "processed_at": self.storage.execute("SELECT now() as now")[0]["now"]
        }
        
        self.storage.insert_batch("transformer_progress", [row])

    def get_processing_status(self) -> Dict[str, Dict]:
        """Get processing status."""
        if config.STORAGE_BACKEND.lower() == "parquet":
            return {"message": "Status tracking not implemented for Parquet backend"}
        
        status = {}
        
        for loader_name, config_item in self.loader_configs.items():
            raw_table = config_item["raw_table"]
            
            stats_query = """
            SELECT 
                COUNT(*) as total_ranges,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_ranges,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_ranges,
                SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing_ranges,
                SUM(processed_count) as total_processed,
                SUM(failed_count) as total_failed,
                MAX(CASE WHEN status = 'completed' THEN end_slot ELSE NULL END) as max_completed
            FROM transformer_progress FINAL
            WHERE raw_table_name = {table:String}
            """
            
            result = self.storage.execute(stats_query, {"table": raw_table})
            if result:
                row = result[0]
                status[raw_table] = {
                    "total_ranges": row["total_ranges"],
                    "completed_ranges": row["completed_ranges"], 
                    "failed_ranges": row["failed_ranges"],
                    "processing_ranges": row["processing_ranges"],
                    "total_processed_items": row["total_processed"],
                    "total_failed_items": row["total_failed"],
                    "max_completed_slot": row["max_completed"]
                }
            else:
                status[raw_table] = {
                    "total_ranges": 0, "completed_ranges": 0, "failed_ranges": 0,
                    "processing_ranges": 0, "total_processed_items": 0, 
                    "total_failed_items": 0, "max_completed_slot": None
                }
        
        return status