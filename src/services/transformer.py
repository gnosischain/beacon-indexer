import asyncio
from typing import List, Dict, Tuple, Optional
from src.services.clickhouse import ClickHouse
from src.services.fork import ForkDetectionService
from src.parsers.factory import ParserFactory
from src.parsers.validators import ValidatorsParser
from src.parsers.rewards import RewardsParser
from src.utils.logger import logger

class TransformerService:
    """Service for transforming raw data into structured tables using load_state_chunks."""
    
    def __init__(self):
        self.clickhouse = ClickHouse()
        self.fork_service = ForkDetectionService(clickhouse_client=self.clickhouse)
        self.parser_factory = ParserFactory(self.fork_service)
        self.validator_parser = ValidatorsParser()
        self.rewards_parser = RewardsParser()
        
        # Define loader configurations
        self.loader_configs = {
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
        
        logger.info("Transformer initialized with chunk-based processing")
    
    async def initialize(self):
        """Initialize the transformer service."""
        network_info = " (auto-detected)" if self.fork_service.is_auto_detected() else ""
        
        logger.info("Fork service initialized", 
                   network=self.fork_service.get_network_name() + network_info,
                   slots_per_epoch=self.fork_service.slots_per_epoch,
                   seconds_per_slot=self.fork_service.seconds_per_slot,
                   genesis_time=self.fork_service.genesis_time)
    
    async def run(self, batch_size: int = 100, continuous: bool = False):
        """Run transformer by processing completed load chunks that haven't been transformed yet."""
        
        mode = "continuous" if continuous else "batch"
        logger.info("Starting chunk-based transformer", mode=mode, batch_size=batch_size)
        
        await self.initialize()
        
        consecutive_empty_rounds = 0
        max_empty_rounds = 3 if not continuous else float('inf')
        
        while consecutive_empty_rounds < max_empty_rounds:
            try:
                processed_any = False
                
                # Process each configured loader type
                for loader_name in self.loader_configs.keys():
                    # Get completed chunks that haven't been transformed
                    untransformed_chunks = self._get_untransformed_chunks(loader_name)
                    
                    if untransformed_chunks:
                        logger.info("Found untransformed chunks", 
                                   loader=loader_name, 
                                   chunks=len(untransformed_chunks))
                        
                        # Process each chunk (limit to avoid overwhelming)
                        chunks_processed = 0
                        max_chunks_per_round = 5
                        
                        for chunk in untransformed_chunks:
                            success = await self._process_chunk(loader_name, chunk)
                            if success:
                                processed_any = True
                                chunks_processed += 1
                            
                            # Limit chunks per round
                            if chunks_processed >= max_chunks_per_round:
                                break
                
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
            logger.info("Batch processing completed - no more chunks to transform")
    
    def _get_untransformed_chunks(self, loader_name: str) -> List[Dict]:
        """Get completed load chunks that haven't been transformed yet."""
        
        config = self.loader_configs[loader_name]
        raw_table = config["raw_table"]
        
        query = """
        SELECT chunk_id, start_slot, end_slot, loader_name
        FROM load_state_chunks FINAL
        WHERE loader_name = {loader_name:String}
          AND status = 'completed'
          AND chunk_id NOT IN (
              SELECT CONCAT(raw_table_name, '_', toString(start_slot), '_', toString(end_slot))
              FROM transformer_progress FINAL
              WHERE raw_table_name = {raw_table:String}
                AND status = 'completed'
          )
        ORDER BY start_slot
        LIMIT 20
        """
        
        return self.clickhouse.execute(query, {
            "loader_name": loader_name,
            "raw_table": raw_table
        })
    
    async def _process_chunk(self, loader_name: str, chunk: Dict) -> bool:
        """Process a single chunk."""
        
        config = self.loader_configs[loader_name]
        raw_table = config["raw_table"]
        start_slot = chunk["start_slot"]
        end_slot = chunk["end_slot"]
        
        logger.info("Processing chunk", 
                   loader=loader_name,
                   start_slot=start_slot,
                   end_slot=end_slot)
        
        # Mark as processing
        self._record_processing_started(raw_table, start_slot, end_slot)
        
        try:
            # Get the raw data for this chunk
            raw_data = self._get_chunk_data(loader_name, start_slot, end_slot)
            
            if not raw_data:
                logger.warning("No raw data found for chunk", 
                              loader=loader_name,
                              start_slot=start_slot,
                              end_slot=end_slot)
                self._record_processing_result(raw_table, start_slot, end_slot, 'completed', 0, 0, 'No data')
                return True
            
            # Process based on loader configuration
            if config["fork_aware"]:
                processed_count, failed_count = await self._process_fork_aware_batch(raw_data, loader_name)
            else:
                processed_count, failed_count = await self._process_non_fork_aware_batch(raw_data, loader_name)
            
            # Record result
            if failed_count > 0:
                error_msg = f"Failed {failed_count}/{len(raw_data)} items"
                self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                             processed_count, failed_count, error_msg)
                logger.error("Chunk processing had failures", 
                            loader=loader_name,
                            start_slot=start_slot,
                            end_slot=end_slot,
                            processed=processed_count,
                            failed=failed_count)
            else:
                self._record_processing_result(raw_table, start_slot, end_slot, 'completed', 
                                             processed_count, 0, '')
                logger.info("Chunk processed successfully", 
                           loader=loader_name,
                           start_slot=start_slot,
                           end_slot=end_slot,
                           processed=processed_count)
            
            return True
            
        except Exception as e:
            error_msg = f"Chunk processing failed: {str(e)}"
            self._record_processing_result(raw_table, start_slot, end_slot, 'failed', 
                                         0, 1, error_msg)
            logger.error("Chunk processing failed", 
                        loader=loader_name,
                        start_slot=start_slot,
                        end_slot=end_slot,
                        error=str(e))
            return False
    
    def _get_chunk_data(self, loader_name: str, start_slot: int, end_slot: int) -> List[Dict]:
        """Get raw data for a specific chunk range with fork handling."""
        
        config = self.loader_configs[loader_name]
        raw_table = config["raw_table"]
        use_final = config["use_final"]
        
        final_clause = "FINAL" if use_final else ""
        
        if loader_name in ["blocks", "validators", "rewards"]:
            # Tables with payload_hash - get the latest payload per slot
            # This is not the way to handle fork protection, but let's keep it for now
            query = f"""
            SELECT slot, payload, payload_hash
            FROM (
                SELECT 
                    slot, 
                    payload, 
                    payload_hash,
                    retrieved_at,
                    ROW_NUMBER() OVER (PARTITION BY slot ORDER BY retrieved_at DESC) as rn
                FROM {raw_table} {final_clause}
                WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
            )
            WHERE rn = 1
            ORDER BY slot
            """
        else:
            # Legacy tables without payload_hash
            query = f"""
            SELECT slot, payload 
            FROM {raw_table} {final_clause}
            WHERE slot >= {{start_slot:UInt64}} AND slot <= {{end_slot:UInt64}}
            ORDER BY slot
            """
        
        result = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        # Return consistent format (slot, payload)
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
    async def _process_fork_aware_batch(self, raw_data: List[Dict], loader_name: str) -> Tuple[int, int]:
        """Process data using fork-aware parsers (for blocks)."""
        
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
                
                fork_processed = 0
                fork_failed = 0
                tables_data = {}
                
                for raw_item in fork_data:
                    slot = raw_item["slot"]
                    
                    try:
                        parsed_data = parser.parse(raw_item)
                        
                        if parsed_data and any(len(rows) > 0 for rows in parsed_data.values()):
                            for table_name, rows in parsed_data.items():
                                if rows:
                                    if table_name not in tables_data:
                                        tables_data[table_name] = []
                                    tables_data[table_name].extend(rows)
                            fork_processed += 1
                        else:
                            fork_processed += 1  # Empty blocks are still processed
                            
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
    
    async def _process_non_fork_aware_batch(self, raw_data: List[Dict], loader_name: str) -> Tuple[int, int]:
        """Process data using non-fork-aware parsers (for validators, rewards)."""
        
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
        all_rows = []
        
        for raw_item in raw_data:
            slot = raw_item["slot"]
            
            try:
                parsed_data = parser.parse(raw_item)
                
                if parsed_data and target_table in parsed_data and parsed_data[target_table]:
                    all_rows.extend(parsed_data[target_table])
                    processed_count += 1
                    logger.debug(f"Parsed {loader_name} for slot", 
                               slot=slot, 
                               count=len(parsed_data[target_table]))
                else:
                    processed_count += 1  # Empty data is still processed
                    
            except Exception as e:
                logger.error(f"{loader_name.title()} parser failed", 
                           slot=slot, 
                           error=str(e))
                failed_count += 1
        
        # Insert all data
        if all_rows:
            try:
                self.clickhouse.insert_batch(target_table, all_rows)
                logger.info(f"Inserted {loader_name} data", 
                           rows=len(all_rows),
                           processed_items=processed_count)
            except Exception as e:
                logger.error(f"Failed to insert {loader_name} data", 
                           rows=len(all_rows),
                           error=str(e))
                failed_count += processed_count
                processed_count = 0
        
        return processed_count, failed_count
    
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
        """Reprocess a specific range of slots."""
        logger.info("Starting chunk-based reprocessing", 
                   start_slot=start_slot, 
                   end_slot=end_slot)
        
        await self.initialize()
        
        # Find all chunks in the range that need reprocessing
        for loader_name in self.loader_configs.keys():
            config = self.loader_configs[loader_name]
            raw_table = config["raw_table"]
            
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
            
            chunks = self.clickhouse.execute(chunks_query, {
                "loader_name": loader_name,
                "start_slot": start_slot,
                "end_slot": end_slot
            })
            
            logger.info("Found chunks for reprocessing", 
                       loader=loader_name,
                       chunks=len(chunks))
            
            # Process each chunk
            for chunk in chunks:
                await self._process_chunk(loader_name, chunk)
        
        logger.info("Reprocessing completed")
    
    def get_processing_status(self) -> Dict[str, Dict]:
        """Get processing status for all configured loaders."""
        status = {}
        
        for loader_name, config in self.loader_configs.items():
            raw_table = config["raw_table"]
            
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
            
            result = self.clickhouse.execute(stats_query, {"table": raw_table})
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
        """Get recent failed processing ranges for debugging."""
        query = """
        SELECT raw_table_name, start_slot, end_slot, failed_count, error_message, processed_at
        FROM transformer_progress FINAL
        WHERE status = 'failed'
        ORDER BY processed_at DESC
        LIMIT {limit:UInt64}
        """
        
        return self.clickhouse.execute(query, {"limit": limit})