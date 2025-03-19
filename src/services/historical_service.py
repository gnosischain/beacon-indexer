import asyncio
from typing import List, Optional

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.scrapers.base_scraper import BaseScraper
from src.utils.block_processor import BlockProcessor


class HistoricalService:
    """Service to scrape historical data from the beacon chain."""
    
    def __init__(
        self,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List[BaseScraper],
        specs_manager,
        start_slot: Optional[int] = None,
        end_slot: Optional[int] = None,
        batch_size: Optional[int] = None
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.specs_manager = specs_manager
        self.start_slot = start_slot or config.scraper.historical_start_slot
        self.end_slot = end_slot or config.scraper.historical_end_slot
        self.batch_size = batch_size or config.scraper.batch_size
        
        # Set specs manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
    
    async def start(self):
        """Start the historical scraper."""
        await self.beacon_api.start()
        
        # Get the latest slot from the beacon chain if end_slot is not provided
        if self.end_slot is None:
            latest_header = await self.beacon_api.get_block_header("head")
            self.end_slot = int(latest_header["header"]["message"]["slot"])
            logger.info(f"Setting end slot to latest slot: {self.end_slot}")
        
        # Process slots in batches
        for batch_start in range(self.start_slot, self.end_slot + 1, self.batch_size):
            batch_end = min(batch_start + self.batch_size - 1, self.end_slot)
            await self._process_slot_batch(batch_start, batch_end)
            
            # Update scraper state
            for scraper in self.scrapers:
                self.clickhouse.update_scraper_state(
                    scraper_id=scraper.scraper_id,
                    last_processed_slot=batch_end,
                    mode="historical"
                )
            
            logger.info(f"Processed slots {batch_start} to {batch_end}")
    
    async def _process_slot_batch(self, start_slot: int, end_slot: int):
        """Process a batch of slots using the block processor."""
        # Get missing slots in the range
        missing_slots = self.clickhouse.get_missing_slots_in_range(start_slot, end_slot)

        if not missing_slots:
            logger.info(f"No missing slots in range {start_slot} to {end_slot}")
            return
        
        logger.info(f"Processing {len(missing_slots)} missing slots in range {start_slot} to {end_slot}")
        
        # Use a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(min(10, config.scraper.max_concurrent_requests))
        block_processor = BlockProcessor(self.beacon_api)
        
        # Process slots with limited concurrency
        async def process_slot_with_semaphore(slot):
            async with semaphore:
                # Create processor functions for each scraper
                processors = []
                for scraper in self.scrapers:
                    if scraper.one_time and scraper.clickhouse.get_last_processed_slot(scraper.scraper_id) > 0:
                        continue
                    processors.append(scraper.process)
                
                # Process the block once
                await block_processor.get_and_process_block(slot, processors)
        
        # Create tasks for each missing slot
        tasks = [process_slot_with_semaphore(slot) for slot in missing_slots]
        
        # Wait for all tasks to complete
        if tasks:
            await asyncio.gather(*tasks)