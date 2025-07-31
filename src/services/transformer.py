import asyncio
from typing import List, Dict
from src.services.clickhouse import ClickHouse
from src.services.fork import ForkDetectionService
from src.parsers.factory import ParserFactory
from src.utils.logger import logger

class TransformerService:
    """Service for transforming raw data into structured tables with auto-detected fork awareness."""
    
    def __init__(self):
        self.clickhouse = ClickHouse()
        # Initialize fork service with ClickHouse client for auto-detection
        # Auto-detection happens in constructor, no need for separate update method
        self.fork_service = ForkDetectionService(clickhouse_client=self.clickhouse)
        self.parser_factory = ParserFactory(self.fork_service)
        
        logger.info("Fork-aware transformer initialized with auto-detection")
    
    async def initialize(self):
        """Initialize the transformer service - auto-detection already done in constructor."""
        network_info = " (auto-detected)" if self.fork_service.is_auto_detected() else ""
        
        logger.info("Fork service initialized", 
                   network=self.fork_service.get_network_name() + network_info,
                   slots_per_epoch=self.fork_service.slots_per_epoch,
                   seconds_per_slot=self.fork_service.seconds_per_slot,
                   genesis_time=self.fork_service.genesis_time)
    
    async def run(self, batch_size: int = 100):
        """Run the transformer in continuous mode."""
        logger.info("Starting fork-aware transformer")
        
        # Initialize if needed
        await self.initialize()
        
        last_processed_slot = self.clickhouse.get_last_processed_slot("transformer")
        
        while True:
            try:
                processed_any = False
                max_slot = last_processed_slot
                
                # Process blocks
                raw_blocks = self._get_raw_blocks_batch(last_processed_slot, batch_size)
                if raw_blocks:
                    block_max_slot = await self._process_fork_aware_batch(raw_blocks, "blocks")
                    max_slot = max(max_slot, block_max_slot)
                    processed_any = True
                
                # Process validators
                raw_validators = self._get_raw_validators_batch(last_processed_slot, batch_size)
                if raw_validators:
                    validator_max_slot = await self._process_fork_aware_batch(raw_validators, "validators")
                    max_slot = max(max_slot, validator_max_slot)
                    processed_any = True
                
                # Update progress
                if max_slot > last_processed_slot:
                    self.clickhouse.update_last_processed_slot("transformer", max_slot)
                    last_processed_slot = max_slot
                    logger.info("Progress updated", last_slot=max_slot)
                
                if not processed_any:
                    # No new data, wait
                    await asyncio.sleep(10)
                    
            except Exception as e:
                logger.error("Error in transformer", error=str(e))
                await asyncio.sleep(30)
    
    async def _process_fork_aware_batch(self, raw_data: List[dict], data_type: str) -> int:
        """Process a batch of raw data using fork-aware parsers."""
        # Group data by fork
        fork_groups = {}
        max_slot = 0
        
        for item in raw_data:
            slot = item["slot"]
            max_slot = max(max_slot, slot)
            
            fork_info = self.fork_service.get_fork_at_slot(slot)
            fork_name = fork_info.name
            
            if fork_name not in fork_groups:
                fork_groups[fork_name] = []
            fork_groups[fork_name].append(item)
        
        # Process each fork group
        for fork_name, fork_data in fork_groups.items():
            try:
                parser = self.parser_factory.get_parser_for_fork(fork_name)
                
                # Collect all parsed data by table
                tables_data = {}
                
                for raw_item in fork_data:
                    slot = raw_item["slot"]
                    
                    try:
                        parsed_data = parser.parse(raw_item)
                        
                        # Merge into tables_data
                        for table_name, rows in parsed_data.items():
                            if table_name not in tables_data:
                                tables_data[table_name] = []
                            tables_data[table_name].extend(rows)
                            
                    except Exception as e:
                        logger.error("Fork parser failed", 
                                    parser=parser.fork_name, 
                                    slot=slot, 
                                    error=str(e))
                
                # Insert all data for this fork
                for table_name, rows in tables_data.items():
                    if rows:
                        self.clickhouse.insert_batch(table_name, rows)
                        logger.debug("Inserted fork-aware data", 
                                   table=table_name, 
                                   rows=len(rows),
                                   fork=fork_name)
                
                logger.info("Processed fork batch", 
                           fork=fork_name,
                           items=len(fork_data),
                           tables=len(tables_data))
                
            except Exception as e:
                logger.error("Fork batch processing failed", 
                           fork=fork_name,
                           items=len(fork_data),
                           error=str(e))
        
        return max_slot
    
    async def reprocess(self, start_slot: int, end_slot: int, batch_size: int = 100):
        """Reprocess a specific range of slots."""
        logger.info("Starting fork-aware reprocessing", 
                   start_slot=start_slot, 
                   end_slot=end_slot)
        
        # Initialize if needed
        await self.initialize()
        
        current_slot = start_slot
        
        while current_slot <= end_slot:
            batch_end = min(current_slot + batch_size - 1, end_slot)
            
            # Process blocks
            raw_blocks = self._get_raw_blocks_range(current_slot, batch_end)
            if raw_blocks:
                await self._process_fork_aware_batch(raw_blocks, "blocks")
                logger.info("Reprocessed blocks batch", 
                           start=current_slot, 
                           end=batch_end,
                           count=len(raw_blocks))
            
            # Process validators
            raw_validators = self._get_raw_validators_range(current_slot, batch_end)
            if raw_validators:
                await self._process_fork_aware_batch(raw_validators, "validators")
                logger.info("Reprocessed validators batch", 
                           start=current_slot, 
                           end=batch_end,
                           count=len(raw_validators))
            
            current_slot = batch_end + 1
        
        logger.info("Reprocessing completed")
    
    def _get_raw_blocks_batch(self, last_processed_slot: int, batch_size: int) -> List[dict]:
        """Get next batch of raw blocks to process."""
        query = """
        SELECT slot, payload 
        FROM raw_blocks 
        WHERE slot > {last_slot:UInt64}
        ORDER BY slot 
        LIMIT {limit:UInt64}
        """
        
        result = self.clickhouse.execute(query, {
            "last_slot": last_processed_slot,
            "limit": batch_size
        })
        
        return [{"slot": row["slot"], "payload": row["payload"]} for row in result]
    
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
    
    def _get_raw_validators_batch(self, last_processed_slot: int, batch_size: int) -> List[dict]:
        """Get next batch of raw validators to process."""
        query = """
        SELECT slot, payload 
        FROM raw_validators FINAL
        WHERE slot > {last_slot:UInt64}
        ORDER BY slot 
        LIMIT {limit:UInt64}
        """
        
        result = self.clickhouse.execute(query, {
            "last_slot": last_processed_slot,
            "limit": batch_size
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