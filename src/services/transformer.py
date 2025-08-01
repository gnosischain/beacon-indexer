import asyncio
from typing import List, Dict, Tuple, Optional
from src.services.clickhouse import ClickHouse
from src.services.fork import ForkDetectionService
from src.parsers.factory import ParserFactory
from src.parsers.validators import ValidatorsParser
from src.utils.logger import logger

class TransformerService:
    """Service for transforming raw data into structured tables with range-based progress tracking."""
    
    def __init__(self):
        self.clickhouse = ClickHouse()
        self.fork_service = ForkDetectionService(clickhouse_client=self.clickhouse)
        self.parser_factory = ParserFactory(self.fork_service)
        self.validator_parser = ValidatorsParser()
        
        logger.info("Transformer initialized with separate block/validator processing")
    
    async def initialize(self):
        """Initialize the transformer service."""
        network_info = " (auto-detected)" if self.fork_service.is_auto_detected() else ""
        
        logger.info("Fork service initialized", 
                   network=self.fork_service.get_network_name() + network_info,
                   slots_per_epoch=self.fork_service.slots_per_epoch,
                   seconds_per_slot=self.fork_service.seconds_per_slot,
                   genesis_time=self.fork_service.genesis_time)
    
    async def run(self, batch_size: int = 100, continuous: bool = False):
        """
        Run the transformer with per-table range tracking.
        
        Args:
            batch_size: Number of items to process per batch
            continuous: If True, run continuously waiting for new data.
                       If False, process all available data and exit.
        """
        mode = "continuous" if continuous else "batch"
        logger.info("Starting range-aware transformer", mode=mode, batch_size=batch_size)
        
        # Initialize if needed
        await self.initialize()
        
        consecutive_empty_rounds = 0
        max_empty_rounds = 3 if not continuous else float('inf')
        
        while consecutive_empty_rounds < max_empty_rounds:
            try:
                processed_any = False
                
                # Process blocks (fork-aware)
                if await self._process_table_batch("raw_blocks", "blocks", batch_size):
                    processed_any = True
                
                # Process validators (NOT fork-aware)
                if await self._process_table_batch("raw_validators", "validators", batch_size):
                    processed_any = True

                if await self._process_table_batch("raw_rewards", "rewards", batch_size):
                    processed_any = True
                
                if processed_any:
                    consecutive_empty_rounds = 0  # Reset counter
                    logger.debug("Processed data, resetting empty counter")
                else:
                    consecutive_empty_rounds += 1
                    logger.debug("No data processed", 
                               consecutive_empty=consecutive_empty_rounds,
                               max_empty=max_empty_rounds)
                    
                    if continuous:
                        # In continuous mode, wait for new data
                        logger.info("No new data found, waiting for more data...")
                        await asyncio.sleep(10)
                    else:
                        # In batch mode, short sleep then check again
                        await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error("Error in transformer", error=str(e))
                if continuous:
                    await asyncio.sleep(30)
                else:
                    break  # Exit on error in batch mode
        
        if not continuous:
            logger.info("Batch processing completed - no more data to process")
        else:
            logger.info("Continuous processing stopping after too many empty rounds")
    
    async def _process_table_batch(self, raw_table: str, data_type: str, batch_size: int) -> bool:
        """Process a batch from a specific raw table. Returns True if processed anything."""
        # Get next unprocessed slots
        raw_data = self._get_unprocessed_batch(raw_table, batch_size)
        if not raw_data:
            logger.debug("No unprocessed data found", table=raw_table)
            return False
        
        min_slot = min(item["slot"] for item in raw_data)
        max_slot = max(item["slot"] for item in raw_data)
        
        logger.info("Processing batch", 
                   table=raw_table,
                   items=len(raw_data),
                   min_slot=min_slot,
                   max_slot=max_slot)
        
        # Mark as processing first
        self._record_processing_started(raw_table, min_slot, max_slot)
        
        try:
            # Process the data using the appropriate method
            if data_type == "blocks":
                processed_count, failed_count = await self._process_blocks_batch(raw_data)
            elif data_type == "validators":
                processed_count, failed_count = await self._process_validators_batch(raw_data)
            else:
                raise ValueError(f"Unknown data type: {data_type}")
            
            if failed_count > 0:
                # Some failures occurred
                error_msg = f"Failed to process {failed_count}/{len(raw_data)} items"
                self._record_processing_result(raw_table, min_slot, max_slot, 'failed', 
                                             processed_count, failed_count, error_msg)
                logger.error("Batch processing had failures", 
                           table=raw_table,
                           total=len(raw_data),
                           processed=processed_count,
                           failed=failed_count,
                           min_slot=min_slot,
                           max_slot=max_slot)
            else:
                # All succeeded
                self._record_processing_result(raw_table, min_slot, max_slot, 'completed', 
                                             processed_count, 0, '')
                logger.info("Processed table batch successfully", 
                           table=raw_table,
                           count=processed_count,
                           min_slot=min_slot,
                           max_slot=max_slot)
            
            return True
            
        except Exception as e:
            # Complete batch failure
            error_msg = f"Batch processing failed: {str(e)}"
            self._record_processing_result(raw_table, min_slot, max_slot, 'failed', 
                                         0, len(raw_data), error_msg)
            logger.error("Batch processing failed completely", 
                        table=raw_table,
                        min_slot=min_slot,
                        max_slot=max_slot,
                        error=str(e))
            return False
    
    def _get_unprocessed_batch(self, raw_table: str, batch_size: int) -> List[dict]:
        """Get next batch of unprocessed data from a raw table - handles gaps properly."""
        # Step 1: Get all available slots in raw data
        if raw_table == "raw_blocks":
            available_query = """
            SELECT slot 
            FROM raw_blocks
            ORDER BY slot
            """
        elif raw_table == "raw_validators":
            available_query = """
            SELECT slot 
            FROM raw_validators FINAL
            ORDER BY slot
            """
        else:
            return []
        
        available_slots = self.clickhouse.execute(available_query)
        if not available_slots:
            logger.debug("No raw data available", table=raw_table)
            return []
        
        # Step 2: Get all completed slot ranges  
        completed_ranges_query = """
        SELECT start_slot, end_slot
        FROM transformer_progress FINAL
        WHERE raw_table_name = {table:String}
          AND status = 'completed'
        ORDER BY start_slot
        """
        
        completed_ranges = self.clickhouse.execute(completed_ranges_query, {"table": raw_table})
        
        # Step 3: Find unprocessed slots (not in any completed range)
        unprocessed_slots = []
        for row in available_slots:
            slot = row["slot"]
            is_processed = False
            
            # Check if this slot is in any completed range
            for range_row in completed_ranges:
                start_slot = range_row["start_slot"]
                end_slot = range_row["end_slot"]
                if start_slot <= slot <= end_slot:
                    is_processed = True
                    break
            
            if not is_processed:
                unprocessed_slots.append(slot)
                if len(unprocessed_slots) >= batch_size:
                    break
        
        if not unprocessed_slots:
            logger.debug("No unprocessed slots found", table=raw_table)
            return []
        
        # Step 4: Get the actual data for unprocessed slots
        slots_list = ",".join(str(slot) for slot in unprocessed_slots)
        
        if raw_table == "raw_blocks":
            data_query = f"""
            SELECT slot, payload 
            FROM raw_blocks
            WHERE slot IN ({slots_list})
            ORDER BY slot
            """
        else:  # raw_validators
            data_query = f"""
            SELECT slot, payload 
            FROM raw_validators FINAL
            WHERE slot IN ({slots_list})
            ORDER BY slot
            """
        
        result = self.clickhouse.execute(data_query)
        
        if not result:
            return []
        
        unprocessed_data = [{"slot": row["slot"], "payload": row["payload"]} for row in result]
        
        logger.debug("Found unprocessed data with gap handling", 
                    table=raw_table,
                    count=len(unprocessed_data),
                    first_slot=unprocessed_data[0]["slot"] if unprocessed_data else None,
                    last_slot=unprocessed_data[-1]["slot"] if unprocessed_data else None,
                    total_available=len(available_slots),
                    completed_ranges=len(completed_ranges))
        
        return unprocessed_data
    
    async def _process_blocks_batch(self, raw_data: List[dict]) -> Tuple[int, int]:
        """Process blocks using fork-aware parsers."""
        return await self._process_fork_aware_batch(raw_data, "blocks")
    
    async def _process_validators_batch(self, raw_data: List[dict]) -> Tuple[int, int]:
        """Process validators using the dedicated validator parser (not fork-aware)."""
        processed_count = 0
        failed_count = 0
        
        # Collect all validator data
        all_validator_rows = []
        
        for raw_item in raw_data:
            slot = raw_item["slot"]
            
            try:
                # Use the existing validator parser
                parsed_data = self.validator_parser.parse(raw_item)
                
                if parsed_data and "validators" in parsed_data and parsed_data["validators"]:
                    all_validator_rows.extend(parsed_data["validators"])
                    processed_count += 1
                    logger.debug("Parsed validators for slot", 
                               slot=slot, 
                               validator_count=len(parsed_data["validators"]))
                else:
                    logger.warning("No validator data parsed for slot", slot=slot)
                    failed_count += 1
                    
            except Exception as e:
                logger.error("Validator parser failed", 
                           slot=slot, 
                           error=str(e))
                failed_count += 1
        
        # Insert all validator data
        if all_validator_rows:
            try:
                self.clickhouse.insert_batch("validators", all_validator_rows)
                logger.info("Inserted validator data", 
                           rows=len(all_validator_rows),
                           processed_items=processed_count)
            except Exception as e:
                logger.error("Failed to insert validator data", 
                           rows=len(all_validator_rows),
                           error=str(e))
                # If insertion fails, count all as failed
                failed_count += processed_count
                processed_count = 0
        
        return processed_count, failed_count
    
    async def _process_fork_aware_batch(self, raw_data: List[dict], data_type: str) -> Tuple[int, int]:
        """Process blocks using fork-aware parsers."""
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
        
        # Process each fork group
        for fork_name, fork_data in fork_groups.items():
            try:
                parser = self.parser_factory.get_parser_for_fork(fork_name)
                
                # Track success/failure per item
                fork_processed = 0
                fork_failed = 0
                tables_data = {}
                
                for raw_item in fork_data:
                    slot = raw_item["slot"]
                    
                    try:
                        parsed_data = parser.parse(raw_item)
                        
                        # Check if parser returned meaningful data
                        if parsed_data and any(len(rows) > 0 for rows in parsed_data.values()):
                            # Parser returned actual data - count as success
                            for table_name, rows in parsed_data.items():
                                if rows:  # Only count tables with actual rows
                                    if table_name not in tables_data:
                                        tables_data[table_name] = []
                                    tables_data[table_name].extend(rows)
                            
                            fork_processed += 1
                        else:
                            # Parser returned empty/no data - might be normal for empty slots
                            logger.debug("Parser returned no data", 
                                       slot=slot, fork=fork_name)
                            fork_processed += 1  # Empty blocks are still "processed"
                            
                    except Exception as e:
                        logger.error("Fork parser failed", 
                                    parser=parser.fork_name, 
                                    slot=slot, 
                                    error=str(e))
                        fork_failed += 1
                
                # Insert all data for this fork
                if tables_data:
                    for table_name, rows in tables_data.items():
                        if rows:
                            try:
                                self.clickhouse.insert_batch(table_name, rows)
                                logger.debug("Inserted fork-aware data", 
                                           table=table_name, 
                                           rows=len(rows),
                                           fork=fork_name)
                            except Exception as e:
                                logger.error("Failed to insert data", 
                                           table=table_name,
                                           rows=len(rows),
                                           fork=fork_name,
                                           error=str(e))
                                # If insertion fails, count all related items as failed
                                fork_failed += fork_processed
                                fork_processed = 0
                
                total_processed += fork_processed
                total_failed += fork_failed
                
                if fork_failed == 0:
                    logger.info("Processed fork batch successfully", 
                               fork=fork_name,
                               items=fork_processed,
                               tables=len(tables_data))
                else:
                    logger.error("Fork batch had failures", 
                               fork=fork_name,
                               processed=fork_processed,  
                               failed=fork_failed,
                               tables=len(tables_data))
                
            except Exception as e:
                logger.error("Fork batch processing failed completely", 
                           fork=fork_name,
                           items=len(fork_data),
                           error=str(e))
                total_failed += len(fork_data)
        
        return total_processed, total_failed
    
    def _record_processing_started(self, raw_table: str, start_slot: int, end_slot: int):
        """Record that processing has started for a range."""
        row = {
            "raw_table_name": raw_table,
            "start_slot": start_slot,
            "end_slot": end_slot,
            "status": "processing",
            "processed_count": 0,
            "failed_count": 0,
            "error_message": "",
            "processed_at": self.clickhouse.execute("SELECT now() as now")[0]["now"]
        }
        
        self.clickhouse.insert_batch("transformer_progress", [row])
        logger.debug("Started processing range", 
                    table=raw_table,
                    start_slot=start_slot,
                    end_slot=end_slot)
    
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
            "processed_at": self.clickhouse.execute("SELECT now() as now")[0]["now"]
        }
        
        self.clickhouse.insert_batch("transformer_progress", [row])
        logger.debug("Recorded processing result", 
                    table=raw_table,
                    start_slot=start_slot,
                    end_slot=end_slot,
                    status=status,
                    processed=processed_count,
                    failed=failed_count)
    
    async def reprocess(self, start_slot: int, end_slot: int, batch_size: int = 100):
        """Reprocess a specific range of slots by clearing progress and reprocessing."""
        logger.info("Starting range-aware reprocessing", 
                   start_slot=start_slot, 
                   end_slot=end_slot)
        
        # Initialize if needed
        await self.initialize()
        
        # Process in batches
        current_slot = start_slot
        
        while current_slot <= end_slot:
            batch_end = min(current_slot + batch_size - 1, end_slot)
            
            # Process blocks
            raw_blocks = self._get_raw_blocks_range(current_slot, batch_end)
            if raw_blocks:
                processed, failed = await self._process_blocks_batch(raw_blocks)
                status = 'completed' if failed == 0 else 'failed'
                error_msg = f"Reprocess: {failed} failures" if failed > 0 else ""
                self._record_processing_result("raw_blocks", current_slot, batch_end, 
                                             status, processed, failed, error_msg)
                logger.info("Reprocessed blocks batch", 
                           start=current_slot, 
                           end=batch_end,
                           processed=processed,
                           failed=failed)
            
            # Process validators
            raw_validators = self._get_raw_validators_range(current_slot, batch_end)
            if raw_validators:
                processed, failed = await self._process_validators_batch(raw_validators)
                status = 'completed' if failed == 0 else 'failed'
                error_msg = f"Reprocess: {failed} failures" if failed > 0 else ""
                self._record_processing_result("raw_validators", current_slot, batch_end,
                                             status, processed, failed, error_msg)
                logger.info("Reprocessed validators batch", 
                           start=current_slot, 
                           end=batch_end,
                           processed=processed,
                           failed=failed)
            
            current_slot = batch_end + 1
        
        logger.info("Reprocessing completed")
    
    def _get_raw_blocks_range(self, start_slot: int, end_slot: int) -> List[dict]:
        """Get raw blocks for a specific range."""
        query = """
        SELECT slot, payload 
        FROM raw_blocks 
        WHERE slot >= {start_slot:UInt64} AND slot <= {end_slot:UInt64}
        ORDER BY slot
        """
        
        result = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
    def _get_raw_validators_range(self, start_slot: int, end_slot: int) -> List[dict]:
        """Get raw validators for a specific range."""
        query = """
        SELECT slot, payload 
        FROM raw_validators FINAL
        WHERE slot >= {start_slot:UInt64} AND slot <= {end_slot:UInt64}
        ORDER BY slot
        """
        
        result = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
    def get_processing_status(self) -> Dict[str, Dict]:
        """Get processing status for all raw tables."""
        tables = ["raw_blocks", "raw_validators"]
        status = {}
        
        for table in tables:
            # Get processing statistics
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
            
            result = self.clickhouse.execute(stats_query, {"table": table})
            if result:
                row = result[0]
                status[table] = {
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
                status[table] = {
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
        """Get recent failed processing ranges for debugging."""
        query = """
        SELECT raw_table_name, start_slot, end_slot, failed_count, error_message, processed_at
        FROM transformer_progress FINAL
        WHERE status = 'failed'
        ORDER BY processed_at DESC
        LIMIT {limit:UInt64}
        """
        
        return self.clickhouse.execute(query, {"limit": limit})