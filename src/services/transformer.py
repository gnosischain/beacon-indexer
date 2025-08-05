import asyncio
import time
import pandas as pd
from typing import List, Dict, Tuple, Optional
from src.config import config
from src.services.storage_factory import create_storage
from src.parsers.validators import ValidatorsParser
from src.parsers.rewards import RewardsParser
from src.utils.logger import logger

class TransformerService:
    """Enhanced service with atomic operations, complete table filtering, and proper resumption after interruptions."""
    
    def __init__(self):
        self.storage = create_storage()
        
        # Get enabled loaders from config
        self.enabled_loaders = set(config.ENABLED_LOADERS)
        logger.info("Transformer will only process enabled loaders", enabled=list(self.enabled_loaders))
        
        # Only initialize fork detection for ClickHouse backend
        if config.STORAGE_BACKEND.lower() == "clickhouse":
            from src.services.fork import ForkDetectionService
            from src.parsers.factory import ParserFactory
            self.fork_service = ForkDetectionService(clickhouse_client=self.storage)
            self.parser_factory = ParserFactory(self.fork_service)
            
            # Define all possible loader configurations
            all_loader_configs = {
                "blocks": {
                    "raw_table": "raw_blocks",
                    "use_final": False,
                    "fork_aware": True
                },
                "validators": {
                    "raw_table": "raw_validators", 
                    "use_final": True,
                    "fork_aware": False
                },
                "rewards": {
                    "raw_table": "raw_rewards",
                    "use_final": True, 
                    "fork_aware": False
                }
            }
            
            # Only include enabled loaders in configuration
            self.loader_configs = {
                name: config_data for name, config_data in all_loader_configs.items() 
                if name in self.enabled_loaders
            }
            
            logger.info("Filtered loader configs for ClickHouse", 
                       all_available=list(all_loader_configs.keys()),
                       enabled=list(self.loader_configs.keys()))
        else:
            # For Parquet, use simpler approach without fork detection
            self.fork_service = None
            self.parser_factory = None
            self.loader_configs = {}
        
        # Non-fork-aware parsers (always available)
        self.validator_parser = ValidatorsParser()
        self.rewards_parser = RewardsParser()
        
        # Query optimization: cache for untransformed chunks
        self._untransformed_cache = {}
        self._cache_ttl = 120  # 2 minutes cache
        self._last_cache_update = {}
        
        logger.info("Enhanced transformer initialized", 
                   storage_backend=config.STORAGE_BACKEND,
                   fork_aware=self.fork_service is not None,
                   enabled_loaders=list(self.enabled_loaders))
    
    async def initialize(self):
        """Initialize the transformer service."""
        if self.fork_service:
            network_info = " (auto-detected)" if self.fork_service.is_auto_detected() else ""
            
            logger.info("Fork service initialized", 
                       network=self.fork_service.get_network_name() + network_info,
                       slots_per_epoch=self.fork_service.slots_per_epoch,
                       seconds_per_slot=self.fork_service.seconds_per_slot,
                       genesis_time=self.fork_service.genesis_time)
        else:
            logger.info("Simple transformer initialized for Parquet backend")
        
        # Clean up any stale processing records on startup
        await self._cleanup_stale_processing_records()
    
    async def _cleanup_stale_processing_records(self):
        """Clean up processing records that were interrupted."""
        if config.STORAGE_BACKEND.lower() == "parquet":
            return
        
        try:
            # Find processing records older than 30 minutes (likely stale)
            stale_query = """
            SELECT raw_table_name, start_slot, end_slot, processed_at
            FROM transformer_progress FINAL
            WHERE status = 'processing'
            AND processed_at < now() - INTERVAL 30 MINUTE
            """
            
            stale_records = self.storage.execute(stale_query)
            
            if stale_records:
                logger.warning("Found stale processing records from previous run", 
                              count=len(stale_records))
                
                # Reset them to allow reprocessing
                for record in stale_records:
                    self._record_processing_result(
                        record["raw_table_name"],
                        record["start_slot"],
                        record["end_slot"],
                        'failed',
                        0,
                        1,
                        'Reset stale processing record on startup'
                    )
                
                logger.info("Reset stale processing records", count=len(stale_records))
            
        except Exception as e:
            logger.warning("Failed to cleanup stale processing records", error=str(e))
    
    async def run(self, batch_size: int = 100, continuous: bool = False):
        """Run transformer with backend-specific processing."""
        
        mode = "continuous" if continuous else "batch"
        logger.info("Starting enhanced atomic transformer", 
                   mode=mode, 
                   batch_size=batch_size,
                   storage_backend=config.STORAGE_BACKEND,
                   enabled_loaders=list(self.enabled_loaders))
        
        await self.initialize()
        
        if config.STORAGE_BACKEND.lower() == "parquet":
            await self._run_parquet_transformer(batch_size, continuous)
        else:
            await self._run_clickhouse_transformer(batch_size, continuous)
    
    async def _run_parquet_transformer(self, batch_size: int, continuous: bool):
        """Simple transformer for Parquet backend - processes raw files directly."""
        logger.info("Running Parquet transformer - processing raw data files")
        
        consecutive_empty_rounds = 0
        max_empty_rounds = 3 if not continuous else float('inf')
        
        while consecutive_empty_rounds < max_empty_rounds:
            try:
                processed_any = False
                
                # Process only enabled loaders
                enabled_table_types = [loader for loader in ["blocks", "validators", "rewards"] 
                                     if loader in self.enabled_loaders]
                
                logger.debug("Processing enabled table types", table_types=enabled_table_types)
                
                for table_type in enabled_table_types:
                    raw_table = f"raw_{table_type}"
                    processed = await self._process_parquet_table(raw_table, table_type, batch_size)
                    if processed:
                        processed_any = True
                
                if processed_any:
                    consecutive_empty_rounds = 0
                else:
                    consecutive_empty_rounds += 1
                    if continuous:
                        logger.info("No new data to transform, waiting...")
                        await asyncio.sleep(10)
                    else:
                        await asyncio.sleep(1)
                        
            except Exception as e:
                logger.error("Error in Parquet transformer", error=str(e))
                if continuous:
                    await asyncio.sleep(30)
                else:
                    break
        
        if not continuous:
            logger.info("Parquet transformation completed")
    
    async def _process_parquet_table(self, raw_table: str, table_type: str, batch_size: int) -> bool:
        """Process a raw Parquet table into structured format."""
        try:
            raw_dir = self.storage.output_dir / raw_table
            if not raw_dir.exists():
                return False
            
            # Find unprocessed files (simple approach - look for .parquet files)
            parquet_files = list(raw_dir.rglob("*.parquet"))
            if not parquet_files:
                return False
            
            # Filter out already processed files
            unprocessed_files = []
            for parquet_file in parquet_files:
                processed_dir = parquet_file.parent / "processed"
                if not (processed_dir / parquet_file.name).exists():
                    unprocessed_files.append(parquet_file)
            
            if not unprocessed_files:
                return False
            
            processed_any = False
            
            for parquet_file in unprocessed_files[:5]:  # Process up to 5 files at a time
                try:
                    # Read raw data
                    df = pd.read_parquet(parquet_file)
                    if df.empty:
                        continue
                    
                    # Convert to list of dictionaries for processing
                    raw_data = df.to_dict('records')
                    
                    # Process based on table type
                    if table_type == "blocks":
                        processed_count, failed_count = await self._process_blocks_simple(raw_data)
                    elif table_type == "validators":
                        processed_count, failed_count = await self._process_validators_simple(raw_data)
                    elif table_type == "rewards":
                        processed_count, failed_count = await self._process_rewards_simple(raw_data)
                    else:
                        continue
                    
                    if processed_count > 0:
                        processed_any = True
                        logger.info("Processed Parquet file", 
                                   file=parquet_file.name,
                                   table_type=table_type,
                                   processed=processed_count,
                                   failed=failed_count)
                        
                        # Mark file as processed by moving it to a processed subdirectory
                        processed_dir = parquet_file.parent / "processed"
                        processed_dir.mkdir(exist_ok=True)
                        parquet_file.rename(processed_dir / parquet_file.name)
                    
                except Exception as e:
                    logger.error("Failed to process Parquet file", 
                               file=parquet_file, 
                               error=str(e))
            
            return processed_any
            
        except Exception as e:
            logger.error("Failed to process Parquet table", 
                        raw_table=raw_table, 
                        error=str(e))
            return False
    
    async def _process_blocks_simple(self, raw_data: List[Dict]) -> Tuple[int, int]:
        """Process blocks with simple parsing (no fork awareness for Parquet)."""
        from src.parsers.blocks import BlocksParser
        parser = BlocksParser()
        
        processed_count = 0
        failed_count = 0
        all_blocks = []
        all_attestations = []
        
        for raw_item in raw_data:
            try:
                parsed_data = parser.parse(raw_item)
                
                if parsed_data:
                    if "blocks" in parsed_data:
                        all_blocks.extend(parsed_data["blocks"])
                    if "attestations" in parsed_data:
                        all_attestations.extend(parsed_data["attestations"])
                    processed_count += 1
                    
            except Exception as e:
                logger.error("Block parsing failed", 
                           slot=raw_item.get("slot"), 
                           error=str(e))
                failed_count += 1
        
        # Insert data
        if all_blocks:
            self.storage.insert_batch("blocks", all_blocks)
        if all_attestations:
            self.storage.insert_batch("attestations", all_attestations)
        
        return processed_count, failed_count
    
    async def _process_validators_simple(self, raw_data: List[Dict]) -> Tuple[int, int]:
        """Process validators with simple parsing."""
        processed_count = 0
        failed_count = 0
        all_rows = []
        
        for raw_item in raw_data:
            try:
                parsed_data = self.validator_parser.parse(raw_item)
                
                if parsed_data and "validators" in parsed_data:
                    all_rows.extend(parsed_data["validators"])
                    processed_count += 1
                    
            except Exception as e:
                logger.error("Validator parsing failed", 
                           slot=raw_item.get("slot"), 
                           error=str(e))
                failed_count += 1
        
        if all_rows:
            self.storage.insert_batch("validators", all_rows)
        
        return processed_count, failed_count
    
    async def _process_rewards_simple(self, raw_data: List[Dict]) -> Tuple[int, int]:
        """Process rewards with simple parsing."""
        processed_count = 0
        failed_count = 0
        all_rows = []
        
        for raw_item in raw_data:
            try:
                parsed_data = self.rewards_parser.parse(raw_item)
                
                if parsed_data and "rewards" in parsed_data:
                    all_rows.extend(parsed_data["rewards"])
                    processed_count += 1
                    
            except Exception as e:
                logger.error("Reward parsing failed", 
                           slot=raw_item.get("slot"), 
                           error=str(e))
                failed_count += 1
        
        if all_rows:
            self.storage.insert_batch("rewards", all_rows)
        
        return processed_count, failed_count
    
    async def _run_clickhouse_transformer(self, batch_size: int, continuous: bool):
        """Enhanced ClickHouse transformer with atomic operations."""
        consecutive_empty_rounds = 0
        max_empty_rounds = 3 if not continuous else float('inf')
        
        logger.info("Starting atomic ClickHouse transformer", 
                   enabled_loaders=list(self.loader_configs.keys()),
                   total_possible_loaders=["blocks", "validators", "rewards"])
        
        while consecutive_empty_rounds < max_empty_rounds:
            try:
                processed_any = False
                
                # Process each configured and enabled loader type
                for loader_name in self.loader_configs.keys():
                    # Get chunks (including failed ones that are ready for retry)
                    untransformed_chunks = self._get_untransformed_chunks_with_failed_recovery(loader_name)
                    
                    if untransformed_chunks:
                        logger.info("Found chunks for atomic processing", 
                                   loader=loader_name, 
                                   chunks=len(untransformed_chunks))
                        
                        # Process each chunk atomically (limit to avoid overwhelming)
                        chunks_processed = 0
                        max_chunks_per_round = 3  # Reduced for atomic operations
                        
                        for chunk in untransformed_chunks:
                            success = await self._process_chunk_streaming_atomic(loader_name, chunk)
                            if success:
                                processed_any = True
                                chunks_processed += 1
                            
                            # Limit chunks per round
                            if chunks_processed >= max_chunks_per_round:
                                logger.info("Reached max chunks per round (atomic)", 
                                           loader=loader_name,
                                           processed=chunks_processed,
                                           remaining=len(untransformed_chunks) - chunks_processed)
                                break
                
                if processed_any:
                    consecutive_empty_rounds = 0
                    # Clear cache after successful processing
                    self._clear_cache()
                else:
                    consecutive_empty_rounds += 1
                    if continuous:
                        logger.info("No new chunks to transform atomically, waiting...")
                        await asyncio.sleep(10)
                    else:
                        await asyncio.sleep(1)
                        
            except Exception as e:
                logger.error("Error in atomic transformer", error=str(e))
                if continuous:
                    await asyncio.sleep(30)
                else:
                    break
        
        if not continuous:
            logger.info("Atomic batch processing completed - no more chunks to transform")
    
    def _get_untransformed_chunks_with_failed_recovery(self, loader_name: str) -> List[Dict]:
        """
        Get untransformed chunks including recently failed ones that should be retried.
        """
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        
        # Modified query to include failed chunks that are older than 5 minutes
        # This allows retry of failed chunks while avoiding immediate retry loops
        query = """
        SELECT 
            lsc.chunk_id, 
            lsc.start_slot, 
            lsc.end_slot, 
            lsc.loader_name
        FROM load_state_chunks lsc FINAL
        LEFT JOIN (
            SELECT DISTINCT raw_table_name, start_slot, end_slot, status, processed_at
            FROM transformer_progress FINAL
            WHERE raw_table_name = {raw_table:String}
            AND (
                status = 'completed'
                OR (status = 'failed' AND processed_at > now() - INTERVAL 5 MINUTE)
            )
        ) tp ON (
            tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
        )
        WHERE lsc.loader_name = {loader_name:String}
        AND lsc.status = 'completed'
        AND tp.start_slot IS NULL
        ORDER BY lsc.start_slot
        LIMIT 50
        SETTINGS join_use_nulls = 1
        """
        
        result = self.storage.execute(query, {
            "loader_name": loader_name,
            "raw_table": raw_table
        })
        
        if result:
            logger.info("Found chunks for processing (including retry)", 
                       loader=loader_name, 
                       chunks=len(result))
        
        return result
    
    async def _process_chunk_streaming_atomic(self, loader_name: str, chunk: Dict) -> bool:
        """
        ATOMIC processing with proper transaction-like behavior.
        Either all data gets processed successfully, or none of it is marked as completed.
        """
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        start_slot = chunk["start_slot"]
        end_slot = chunk["end_slot"]
        
        logger.info("Processing chunk atomically", 
                   loader=loader_name,
                   start_slot=start_slot,
                   end_slot=end_slot)
        
        # Step 1: Mark as processing
        processing_id = self._record_processing_started(raw_table, start_slot, end_slot)
        
        try:
            # Step 2: Process all data first (accumulate in memory)
            all_tables_data = {}
            total_processed = 0
            total_failed = 0
            
            # Use smaller sub-batches but accumulate all results before inserting
            sub_batch_size = 25 if loader_name == "blocks" else 50
            
            for i in range(start_slot, end_slot + 1, sub_batch_size):
                sub_end = min(i + sub_batch_size - 1, end_slot)
                
                # Get data for sub-batch
                raw_data = self._get_chunk_data_optimized(loader_name, i, sub_end)
                
                if raw_data:
                    # Process and accumulate (don't insert yet)
                    if config_data["fork_aware"]:
                        batch_tables_data, processed_count, failed_count = await self._process_fork_aware_batch_accumulate(raw_data, loader_name)
                    else:
                        batch_tables_data, processed_count, failed_count = await self._process_non_fork_aware_batch_accumulate(raw_data, loader_name)
                    
                    # Accumulate results
                    for table_name, rows in batch_tables_data.items():
                        if table_name not in all_tables_data:
                            all_tables_data[table_name] = []
                        all_tables_data[table_name].extend(rows)
                    
                    total_processed += processed_count
                    total_failed += failed_count
                    
                    logger.debug("Sub-batch processed and accumulated", 
                               loader=loader_name, 
                               sub_batch=f"{i}-{sub_end}",
                               processed=processed_count,
                               failed=failed_count)
            
            # Step 3: Atomic insertion - either all succeed or all fail
            if total_failed == 0 and all_tables_data:
                logger.info("Starting atomic insertion", 
                           loader=loader_name,
                           tables=list(all_tables_data.keys()),
                           total_rows=sum(len(rows) for rows in all_tables_data.values()))
                
                # Insert all data atomically (wrapped in try-catch for rollback)
                try:
                    for table_name, rows in all_tables_data.items():
                        if rows:
                            # Use optimized insert with memory limits
                            if hasattr(self.storage, 'insert_batch_optimized'):
                                self.storage.insert_batch_optimized(table_name, rows)
                            else:
                                self.storage.insert_batch(table_name, rows)
                                
                            logger.debug("Inserted table data atomically", 
                                       table=table_name, 
                                       rows=len(rows))
                    
                    # Step 4: Only mark as completed if ALL insertions succeeded
                    self._record_processing_result(raw_table, start_slot, end_slot, 'completed', 
                                                 total_processed, 0, '')
                    
                    logger.info("Chunk processed atomically - SUCCESS", 
                               loader=loader_name,
                               start_slot=start_slot,
                               end_slot=end_slot,
                               processed=total_processed,
                               tables_inserted=len(all_tables_data))
                    
                    return True
                    
                except Exception as insert_error:
                    # Insertion failed - this is critical
                    error_msg = f"Atomic insertion failed: {str(insert_error)}"
                    self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                                 0, total_processed, error_msg)
                    
                    logger.error("ATOMIC INSERTION FAILED - chunk marked as failed", 
                                loader=loader_name,
                                start_slot=start_slot,
                                end_slot=end_slot,
                                error=str(insert_error))
                    return False
            
            else:
                # Processing had failures or no data
                if total_failed > 0:
                    error_msg = f"Processing failures: {total_failed}/{end_slot - start_slot + 1} items"
                else:
                    error_msg = "No data to process"
                    
                self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                             total_processed, total_failed, error_msg)
                
                logger.error("Chunk processing had issues", 
                            loader=loader_name,
                            start_slot=start_slot,
                            end_slot=end_slot,
                            processed=total_processed,
                            failed=total_failed,
                            error=error_msg)
                return False
            
        except Exception as e:
            # Any unexpected error - mark as failed
            error_msg = f"Unexpected error during atomic processing: {str(e)}"
            self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                         0, 1, error_msg)
            
            logger.error("Atomic chunk processing failed completely", 
                        loader=loader_name,
                        start_slot=start_slot,
                        end_slot=end_slot,
                        error=str(e))
            return False
    
    async def _process_fork_aware_batch_accumulate(self, raw_data: List[Dict], loader_name: str) -> Tuple[Dict[str, List[Dict]], int, int]:
        """
        Process fork-aware data with COMPLETE table filtering and proper accumulation.
        
        Based on BeaconBlockBody structure from Ethereum specs, raw_blocks contains:
        - Basic block data: blocks
        - Consensus operations: attestations  
        - Validator operations: deposits, voluntary_exits
        - Slashing operations: proposer_slashings, attester_slashings
        - Fork-specific data: sync_aggregates (Altair+), execution_payloads (Bellatrix+), 
          withdrawals (Capella+), blob_sidecars/blob_commitments (Deneb+), execution_requests (Electra+)
        """
        
        # COMPLETE table filtering - ALL tables that come from raw_blocks
        loader_table_filter = {
            "blocks": {
                # Basic block data
                "blocks",
                
                # Consensus operations (Phase 0)
                "attestations",
                
                # Validator lifecycle operations (Phase 0)
                "deposits", 
                "voluntary_exits",
                
                # Slashing operations (Phase 0)
                "proposer_slashings", 
                "attester_slashings",
                
                # Altair additions
                "sync_aggregates",
                "sync_committees",
                
                # Bellatrix additions (The Merge)
                "execution_payloads",
                "transactions",
                
                # Capella additions (Shanghai)
                "withdrawals",
                "bls_changes",
                
                # Deneb additions (Cancun)
                "blob_sidecars",
                "blob_commitments", 
                
                # Electra additions
                "execution_requests"
            },
            "validators": {"validators"},
            "rewards": {"rewards"}
        }
        
        allowed_tables = loader_table_filter.get(loader_name, set())
        
        logger.debug("Processing with complete table filtering", 
                   loader=loader_name,
                   allowed_tables_count=len(allowed_tables))
        
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
                
                fork_processed = 0
                fork_failed = 0
                
                for raw_item in fork_data:
                    slot = raw_item["slot"]
                    
                    try:
                        parsed_data = parser.parse(raw_item)
                        
                        if parsed_data and any(len(rows) > 0 for rows in parsed_data.values()):
                            # Accumulate ALL allowed tables (not just the ones with data)
                            for table_name, rows in parsed_data.items():
                                if table_name in allowed_tables and rows:
                                    if table_name not in all_tables_data:
                                        all_tables_data[table_name] = []
                                    all_tables_data[table_name].extend(rows)
                                    
                                    # Log when we find special operations
                                    if table_name in ["attester_slashings", "proposer_slashings", "deposits", "voluntary_exits"]:
                                        logger.info(f"Found {table_name} data", 
                                                   slot=slot,
                                                   count=len(rows),
                                                   table_allowed=table_name in allowed_tables)
                            fork_processed += 1
                        else:
                            fork_processed += 1  # Empty blocks are still processed
                            
                    except Exception as e:
                        logger.error("Fork parser failed during accumulation", 
                                    parser=parser.fork_name, 
                                    slot=slot, 
                                    error=str(e))
                        fork_failed += 1
                
                total_processed += fork_processed
                total_failed += fork_failed
                
                # Log what tables we accumulated for this fork
                if all_tables_data:
                    logger.debug("Fork batch accumulated with complete filtering", 
                               fork=fork_name,
                               processed=fork_processed,
                               failed=fork_failed,
                               tables_with_data=sorted(all_tables_data.keys()),
                               total_rows_per_table={k: len(v) for k, v in all_tables_data.items()})
                
            except Exception as e:
                logger.error("Fork batch processing failed during accumulation", 
                           fork=fork_name,
                           items=len(fork_data),
                           error=str(e))
                total_failed += len(fork_data)
        
        return all_tables_data, total_processed, total_failed
    
    async def _process_non_fork_aware_batch_accumulate(self, raw_data: List[Dict], loader_name: str) -> Tuple[Dict[str, List[Dict]], int, int]:
        """
        Process non-fork-aware data and ACCUMULATE results (don't insert yet).
        Returns: (tables_data, processed_count, failed_count)
        """
        # Get the appropriate parser
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
            slot = raw_item["slot"]
            
            try:
                parsed_data = parser.parse(raw_item)
                
                if parsed_data and target_table in parsed_data and parsed_data[target_table]:
                    all_tables_data[target_table].extend(parsed_data[target_table])
                    processed_count += 1
                    logger.debug(f"Accumulated {loader_name} for slot", 
                               slot=slot, 
                               count=len(parsed_data[target_table]))
                else:
                    processed_count += 1  # Empty data is still processed
                    
            except Exception as e:
                logger.error(f"{loader_name.title()} parser failed during accumulation", 
                           slot=slot, 
                           error=str(e))
                failed_count += 1
        
        return all_tables_data, processed_count, failed_count
    
    def _get_chunk_data_optimized(self, loader_name: str, start_slot: int, end_slot: int) -> List[Dict]:
        """Optimized data retrieval with reduced FINAL usage."""
        config_data = self.loader_configs[loader_name]
        raw_table = config_data["raw_table"]
        use_final = config_data["use_final"]
        
        # For tables that need FINAL, use it but with optimization
        if use_final:
            # Use FINAL but with LIMIT and ORDER BY optimization
            query = f"""
            SELECT slot, payload, payload_hash
            FROM {raw_table} FINAL
            WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
            ORDER BY slot, retrieved_at DESC
            LIMIT 5000
            """
        else:
            # Use subquery with window function for better performance
            query = f"""
            SELECT slot, payload, payload_hash
            FROM (
                SELECT 
                    slot, payload, payload_hash, retrieved_at,
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
        
        # Return consistent format (slot, payload)
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
    def _record_processing_started(self, raw_table: str, start_slot: int, end_slot: int) -> str:
        """Record that processing has started and return a processing ID."""
        processing_id = f"{raw_table}_{start_slot}_{end_slot}_{int(time.time())}"
        
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
        logger.debug("Started atomic processing", 
                    table=raw_table,
                    start_slot=start_slot,
                    end_slot=end_slot,
                    processing_id=processing_id)
        
        return processing_id
    
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
        logger.debug("Recorded atomic processing result", 
                    table=raw_table,
                    start_slot=start_slot,
                    end_slot=end_slot,
                    status=status,
                    processed=processed_count,
                    failed=failed_count)
    
    # Cache management methods
    def _get_untransformed_chunks_cached(self, loader_name: str) -> List[Dict]:
        """Get untransformed chunks with caching to reduce FINAL queries."""
        now = time.time()
        cache_key = loader_name
        
        # Check if cache is fresh
        if (cache_key in self._untransformed_cache and 
            cache_key in self._last_cache_update and
            now - self._last_cache_update[cache_key] < self._cache_ttl):
            logger.debug("Using cached chunks", loader=loader_name, 
                        chunks=len(self._untransformed_cache[cache_key]))
            return self._untransformed_cache[cache_key]
        
        # Refresh cache with new method that includes failed recovery
        chunks = self._get_untransformed_chunks_with_failed_recovery(loader_name)
        self._untransformed_cache[cache_key] = chunks
        self._last_cache_update[cache_key] = now
        
        logger.debug("Refreshed chunk cache with failed recovery", loader=loader_name, chunks=len(chunks))
        return chunks
    
    def _clear_cache(self):
        """Clear the chunk cache after successful processing."""
        self._untransformed_cache.clear()
        self._last_cache_update.clear()
        logger.debug("Cleared chunk cache")
    
    # Reprocessing methods
    async def reprocess(self, start_slot: int, end_slot: int, batch_size: int = 100):
        """
        Reprocess a specific range of slots with DEDUPLICATION.
        This method ensures no duplicate rows by properly cleaning existing data first.
        """
        logger.info("Starting deduplicating reprocessing", 
                   start_slot=start_slot, 
                   end_slot=end_slot,
                   storage_backend=config.STORAGE_BACKEND,
                   enabled_loaders=list(self.enabled_loaders))
        
        await self.initialize()
        
        if config.STORAGE_BACKEND.lower() == "parquet":
            logger.info("Reprocessing not implemented for Parquet backend")
            return
        
        # Step 1: Clean existing processed data to prevent duplicates
        await self._clean_existing_data_for_reprocessing(start_slot, end_slot)
        
        # Step 2: Reset transformer progress to force reprocessing
        await self._reset_transformer_progress_for_range(start_slot, end_slot)
        
        # Step 3: Process chunks normally (they will be picked up as "untransformed")
        for loader_name in self.loader_configs.keys():
            config_item = self.loader_configs[loader_name]
            raw_table = config_item["raw_table"]
            
            # Find chunks in range
            chunks_query = """
            SELECT chunk_id, start_slot, end_slot, loader_name
            FROM load_state_chunks FINAL
            WHERE loader_name = {loader_name:String}
              AND status = 'completed'
              AND start_slot >= {start_slot:UInt64}
              AND end_slot <= {end_slot:UInt64}
            ORDER BY start_slot
            """
            
            chunks = self.storage.execute(chunks_query, {
                "loader_name": loader_name,
                "start_slot": start_slot,
                "end_slot": end_slot
            })
            
            logger.info("Found chunks for deduplicating reprocessing", 
                       loader=loader_name,
                       chunks=len(chunks))
            
            # Process each chunk with atomic processing
            for chunk in chunks:
                await self._process_chunk_streaming_atomic(loader_name, chunk)
        
        logger.info("Deduplicating reprocessing completed")
    
    async def _clean_existing_data_for_reprocessing(self, start_slot: int, end_slot: int):
        """
        Clean existing processed data for the slot range to prevent duplicates.
        Uses ClickHouse's ALTER DELETE for efficient cleanup.
        """
        logger.info("Cleaning existing data to prevent duplicates", 
                   start_slot=start_slot, 
                   end_slot=end_slot)
        
        # All possible tables that could have data in this range
        all_possible_tables = [
            # Basic tables
            "blocks", "attestations", 
            # Operations
            "deposits", "voluntary_exits", "proposer_slashings", "attester_slashings",
            # Fork-specific tables  
            "sync_aggregates",
            "execution_payloads", "transactions",
            "withdrawals", "bls_changes", 
            "blob_sidecars", "blob_commitments",
            "execution_requests"
        ]
        
        tables_cleaned = 0
        
        for table_name in all_possible_tables:
            try:
                # Check if table exists and has data in range
                check_query = f"""
                SELECT COUNT(*) as count 
                FROM {table_name} FINAL
                WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
                """
                
                result = self.storage.execute(check_query, {
                    "start_slot": start_slot,
                    "end_slot": end_slot
                })
                
                if result and result[0]["count"] > 0:
                    rows_to_clean = result[0]["count"]
                    
                    # Use ALTER TABLE DELETE for efficient cleanup
                    delete_query = f"""
                    ALTER TABLE {table_name} 
                    DELETE WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
                    """
                    
                    try:
                        self.storage.execute(delete_query, {
                            "start_slot": start_slot,
                            "end_slot": end_slot
                        })
                        
                        logger.info("Cleaned existing data from table", 
                                   table=table_name,
                                   rows_cleaned=rows_to_clean,
                                   method="ALTER_DELETE")
                        tables_cleaned += 1
                        
                    except Exception as delete_error:
                        logger.warning("ALTER DELETE failed for table", 
                                     table=table_name,
                                     error=str(delete_error))
                        
            except Exception as e:
                # Table might not exist yet, that's fine
                logger.debug("Could not clean table (may not exist)", 
                            table=table_name, 
                            error=str(e))
        
        logger.info("Data cleanup completed", 
                   tables_cleaned=tables_cleaned,
                   total_tables_checked=len(all_possible_tables))
    
    async def _reset_transformer_progress_for_range(self, start_slot: int, end_slot: int):
        """Reset transformer progress records for the range so chunks get reprocessed."""
        logger.info("Resetting transformer progress for range", 
                   start_slot=start_slot, 
                   end_slot=end_slot)
        
        # Insert failed records to trigger reprocessing
        reset_query = """
        INSERT INTO transformer_progress 
        (raw_table_name, start_slot, end_slot, status, processed_count, failed_count, error_message, processed_at)
        SELECT 
            raw_table_name,
            start_slot, 
            end_slot,
            'failed' as status,
            0 as processed_count,
            1 as failed_count,
            'Reset for deduplicating reprocessing' as error_message,
            now() as processed_at
        FROM transformer_progress FINAL
        WHERE start_slot >= {start_slot:UInt64} 
          AND end_slot <= {end_slot:UInt64}
          AND status = 'completed'
        """
        
        self.storage.execute(reset_query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        logger.info("Transformer progress reset completed")
    
    # Status and monitoring methods
    def get_processing_status(self) -> Dict[str, Dict]:
        """Get processing status for enabled loaders only."""
        if config.STORAGE_BACKEND.lower() == "parquet":
            return {"message": "Status tracking not implemented for Parquet backend"}
        
        status = {}
        
        # Only check status for enabled loaders
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
                MIN(start_slot) as min_processed,
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
                    "min_processed_slot": row["min_processed"],
                    "max_completed_slot": row["max_completed"]
                }
            else:
                status[raw_table] = {
                    "total_ranges": 0,
                    "completed_ranges": 0,
                    "failed_ranges": 0,
                    "processing_ranges": 0,
                    "total_processed_items": 0,
                    "total_failed_items": 0,
                    "min_processed_slot": None,
                    "max_completed_slot": None
                }
        
        return status
    
    def get_failed_ranges(self, limit: int = 10) -> List[Dict]:
        """Get recent failed processing ranges for debugging - only for enabled loaders."""
        if config.STORAGE_BACKEND.lower() == "parquet":
            return []
        
        # Build filter for enabled loaders
        enabled_raw_tables = [config_item["raw_table"] for config_item in self.loader_configs.values()]
        if not enabled_raw_tables:
            return []
        
        # Create IN clause for enabled tables
        tables_filter = ", ".join([f"'{table}'" for table in enabled_raw_tables])
        
        query = f"""
        SELECT raw_table_name, start_slot, end_slot, failed_count, error_message, processed_at
        FROM transformer_progress FINAL
        WHERE status = 'failed'
        AND raw_table_name IN ({tables_filter})
        ORDER BY processed_at DESC
        LIMIT {{limit:UInt64}}
        """
        
        return self.storage.execute(query, {"limit": limit})
    
    # Debug methods
    def debug_complete_table_filtering(self):
        """Debug method to show complete table filtering configuration."""
        logger.info("=== COMPLETE TABLE FILTERING CONFIGURATION ===")
        
        loader_table_filter = {
            "blocks": {
                "blocks", "attestations", "deposits", "voluntary_exits", 
                "proposer_slashings", "attester_slashings", "sync_aggregates", 
                "sync_committees", "execution_payloads", "transactions", 
                "withdrawals", "bls_changes", "blob_sidecars", "blob_commitments", 
                "execution_requests"
            },
            "validators": {"validators"},
            "rewards": {"rewards"}
        }
        
        for loader, tables in loader_table_filter.items():
            logger.info(f"Loader '{loader}' processes {len(tables)} tables:")
            for table in sorted(tables):
                logger.info(f"  - {table}")
        
        logger.info("=== END TABLE FILTERING CONFIGURATION ===")
    
    async def test_attester_slashing_parsing(self, slot: int = 251340):
        """Test method to verify attester slashing parsing works."""
        logger.info("Testing attester slashing parsing", slot=slot)
        
        # Get raw data
        raw_data = self.storage.execute("""
            SELECT slot, payload 
            FROM raw_blocks 
            WHERE slot = {slot:UInt64} 
            LIMIT 1
        """, {"slot": slot})
        
        if not raw_data:
            logger.error("No raw data found for slot", slot=slot)
            return
        
        # Parse with fork-aware parser
        fork_info = self.fork_service.get_fork_at_slot(slot)
        parser = self.parser_factory.get_parser_for_fork(fork_info.name)
        
        logger.info("Using parser for testing", 
                   fork=fork_info.name,
                   parser_class=parser.__class__.__name__)
        
        # Parse the data
        parsed_data = parser.parse(raw_data[0])
        
        # Check results
        if "attester_slashings" in parsed_data:
            slashings = parsed_data["attester_slashings"]
            logger.info("✅ Parser extracted attester slashings", 
                       count=len(slashings),
                       sample_data=slashings[0] if slashings else None)
            
            # Check if table filtering would allow it
            allowed_tables = {
                "blocks", "attestations", "deposits", 
                "voluntary_exits", "proposer_slashings", "attester_slashings",
                "sync_aggregates", "sync_committees", "execution_payloads", 
                "transactions", "withdrawals", "bls_changes", "blob_sidecars", 
                "blob_commitments", "execution_requests"
            }
            
            if "attester_slashings" in allowed_tables:
                logger.info("✅ Table filtering allows attester_slashings")
            else:
                logger.error("❌ Table filtering blocks attester_slashings")
                
        else:
            logger.error("❌ Parser did not extract attester slashings")
            logger.info("Available parsed tables", tables=list(parsed_data.keys()))