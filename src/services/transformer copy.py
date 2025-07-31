import json
import asyncio
from typing import List
from src.services.clickhouse import ClickHouse
from src.parsers import get_enabled_parsers
from src.config import config
from src.utils.logger import logger

class TransformerService:
    """Service for transforming raw data into structured tables."""
    
    def __init__(self):
        self.clickhouse = ClickHouse()
        self.parsers = get_enabled_parsers(config.ENABLED_LOADERS)
        
        logger.info("Transformer service initialized", 
                   parsers=[parser.name for parser in self.parsers])
    
    async def run(self, batch_size: int = 100):
        """Run the transformer in continuous mode."""
        logger.info("Starting transformer")
        
        last_processed_slot = self.clickhouse.get_last_processed_slot("transformer")
        
        while True:
            try:
                # Get next batch of raw blocks
                raw_blocks = self._get_raw_blocks_batch(last_processed_slot, batch_size)
                
                if not raw_blocks:
                    # No new data, wait
                    await asyncio.sleep(10)
                    continue
                
                # Process batch
                max_slot = await self._process_batch(raw_blocks)
                
                # Update progress
                if max_slot > last_processed_slot:
                    self.clickhouse.update_last_processed_slot("transformer", max_slot)
                    last_processed_slot = max_slot
                    
                    logger.info("Batch processed", 
                               processed_slots=len(raw_blocks),
                               last_slot=max_slot)
                
            except Exception as e:
                logger.error("Error in transformer", error=str(e))
                await asyncio.sleep(30)
    
    async def reprocess(self, start_slot: int, end_slot: int, batch_size: int = 100):
        """Reprocess a specific range of slots."""
        logger.info("Starting reprocessing", start_slot=start_slot, end_slot=end_slot)
        
        current_slot = start_slot
        
        while current_slot <= end_slot:
            batch_end = min(current_slot + batch_size - 1, end_slot)
            
            # Get raw blocks for this range
            raw_blocks = self._get_raw_blocks_range(current_slot, batch_end)
            
            if raw_blocks:
                await self._process_batch(raw_blocks)
                logger.info("Reprocessed batch", 
                           start=current_slot, 
                           end=batch_end,
                           count=len(raw_blocks))
            
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
        
        return [{"slot": row["slot"], "payload": json.loads(row["payload"])} for row in result]
    
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
        
        return [{"slot": row["slot"], "payload": json.loads(row["payload"])} for row in result]
    
    async def _process_batch(self, raw_blocks: List[dict]) -> int:
        """Process a batch of raw blocks."""
        # Collect all parsed data by table
        tables_data = {}
        max_slot = 0
        
        for raw_block in raw_blocks:
            slot = raw_block["slot"]
            payload = raw_block["payload"]
            max_slot = max(max_slot, slot)
            
            # Process with each parser
            for parser in self.parsers:
                try:
                    parsed_data = parser.parse(payload)
                    
                    # Merge into tables_data
                    for table_name, rows in parsed_data.items():
                        if table_name not in tables_data:
                            tables_data[table_name] = []
                        tables_data[table_name].extend(rows)
                        
                except Exception as e:
                    logger.error("Parser failed", 
                                parser=parser.name, 
                                slot=slot, 
                                error=str(e))
        
        # Insert all data
        for table_name, rows in tables_data.items():
            if rows:
                self.clickhouse.insert_batch(table_name, rows)
        
        return max_slot