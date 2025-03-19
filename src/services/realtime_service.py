import asyncio
import time
from typing import List, Dict, Optional

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.scrapers.base_scraper import BaseScraper
from src.utils.block_processor import BlockProcessor

class RealtimeService:
    """Service to scrape realtime data from the beacon chain."""
    
    def __init__(
        self,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List[BaseScraper],
        poll_interval: Optional[int] = None
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.running = False
        self.last_processed_slot = 0
        
        # Initialize poll interval from specs if not provided
        self.poll_interval = poll_interval
        if not self.poll_interval:
            time_params = self.clickhouse.get_time_parameters()
            self.poll_interval = time_params.get('seconds_per_slot', 5) // 2  # Poll at half slot time
    
    
    async def start(self):
        """Start the realtime scraper."""
        await self.beacon_api.start()
        self.running = True
        
        # Get the last processed slot for each scraper
        for scraper in self.scrapers:
            last_slot = self.clickhouse.get_last_processed_slot(scraper.scraper_id)
            self.last_processed_slot = max(self.last_processed_slot, last_slot)
        
        logger.info(f"Starting realtime scraper from slot {self.last_processed_slot + 1}")
        
        try:
            while self.running:
                await self._check_new_blocks()
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info("Realtime scraper cancelled")
            self.running = False
        except Exception as e:
            logger.error(f"Error in realtime scraper: {e}")
            self.running = False
            raise
    
    async def _check_new_blocks(self):
        """Check for new blocks and process them using the block processor."""
        try:
            # Get the latest header
            latest_header = await self.beacon_api.get_block_header("head")
            latest_slot = int(latest_header["header"]["message"]["slot"])
            
            if latest_slot <= self.last_processed_slot:
                logger.debug(f"No new blocks. Latest slot: {latest_slot}, Last processed: {self.last_processed_slot}")
                return
            
            # Create a block processor
            block_processor = BlockProcessor(self.beacon_api)
            
            # Process new slots
            for slot in range(self.last_processed_slot + 1, latest_slot + 1):
                # Create processor functions for each scraper
                processors = []
                for scraper in self.scrapers:
                    if scraper.one_time and scraper.clickhouse.get_last_processed_slot(scraper.scraper_id) > 0:
                        continue
                    processors.append(scraper.process)
                
                # Process the block once
                success = await block_processor.get_and_process_block(slot, processors)
                
                # Update the last processed slot even if no block was found (might be a skipped slot)
                self.last_processed_slot = slot
                
                # Update scraper state
                for scraper in self.scrapers:
                    self.clickhouse.update_scraper_state(
                        scraper_id=scraper.scraper_id,
                        last_processed_slot=slot,
                        mode="realtime"
                    )
                
                if success:
                    logger.info(f"Processed block at slot {slot}")
                else:
                    logger.info(f"Skipped slot {slot} (no block found)")
        
        except Exception as e:
            logger.error(f"Error checking for new blocks: {e}")