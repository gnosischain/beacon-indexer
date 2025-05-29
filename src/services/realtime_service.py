import asyncio
import time
from typing import List, Dict, Optional

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.utils.block_processor import BlockProcessor
from src.utils.specs_manager import SpecsManager

class RealtimeService:
    """Service to scrape realtime data from the beacon chain."""
    
    def __init__(
        self,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List[BaseScraper],
        specs_manager: SpecsManager,
        poll_interval: Optional[int] = None
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.specs_manager = specs_manager
        self.running = False
        self.last_processed_slot = 0
        
        # Initialize poll interval from specs manager if not provided
        self.poll_interval = poll_interval
        if not self.poll_interval:
            self.poll_interval = self.specs_manager.get_seconds_per_slot() // 2  # Poll at half slot time
        
        # Set specs manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
    
    def should_process_with_validator_scraper(self, slot: int) -> bool:
        """Check if this slot should be processed by the validator scraper."""
        validator_scrapers = [s for s in self.scrapers if isinstance(s, ValidatorScraper)]
        
        if not validator_scrapers:
            return False
            
        # For realtime, we use the instance method to check if slot should be processed
        return any(scraper.should_process_slot(slot) for scraper in validator_scrapers)
    
    async def start(self):
        """Start the realtime scraper."""
        await self.beacon_api.start()
        self.running = True
        
        # Get the last processed slot for each scraper
        for scraper in self.scrapers:
            last_slot = self.clickhouse.get_last_processed_slot(scraper.scraper_id)
            self.last_processed_slot = max(self.last_processed_slot, last_slot)
        
        logger.info(f"Starting realtime scraper from slot {self.last_processed_slot + 1}")
        
        # Pre-check one-time scrapers to avoid checking in every block (similar to parallel service)
        active_scrapers = []
        for scraper in self.scrapers:
            if scraper.one_time:
                should_run = await scraper.should_process()
                if should_run:
                    active_scrapers.append(scraper)
                    logger.info(f"One-time scraper {scraper.scraper_id} will be processed")
                else:
                    logger.info(f"One-time scraper {scraper.scraper_id} determined it should not run")
            else:
                # Regular scrapers always get included
                active_scrapers.append(scraper)
                logger.info(f"Regular scraper {scraper.scraper_id} will be processed")
        
        try:
            while self.running:
                # For each cycle, check if we need to remove completed one-time scrapers
                for scraper in list(active_scrapers):  # Create a copy to allow modification
                    if scraper.one_time:
                        last_processed = await scraper.get_last_processed_slot()
                        if last_processed > 0:
                            # This one-time scraper has completed, remove it
                            active_scrapers.remove(scraper)
                            logger.info(f"One-time scraper {scraper.scraper_id} completed and removed from active scrapers")
                            
                if not active_scrapers:
                    logger.warning("No active scrapers remaining. Exiting realtime scraper.")
                    self.running = False
                    break
                
                await self._check_new_blocks(active_scrapers)
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info("Realtime scraper cancelled")
            self.running = False
        except Exception as e:
            logger.error(f"Error in realtime scraper: {e}")
            self.running = False
            raise
    
    async def _check_new_blocks(self, active_scrapers: List[BaseScraper]):
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
                # Filter scrapers based on slot requirements
                slot_scrapers = []
                for scraper in active_scrapers:
                    # If it's a validator scraper, only include it if this is a target slot
                    if isinstance(scraper, ValidatorScraper):
                        if self.should_process_with_validator_scraper(slot):
                            slot_scrapers.append(scraper)
                            logger.debug(f"ValidatorScraper {scraper.scraper_id} will process slot {slot}")
                        else:
                            logger.debug(f"ValidatorScraper {scraper.scraper_id} skipping slot {slot}")
                    else:
                        # Non-validator scrapers process all slots
                        slot_scrapers.append(scraper)
                
                # Create processor functions for active scrapers for this slot
                processors = []
                for scraper in slot_scrapers:
                    processors.append(scraper.process)
                
                # Process the block once (only if we have processors)
                success = False
                if processors:
                    success = await block_processor.get_and_process_block(slot, processors)
                
                # Update the last processed slot even if no block was found (might be a skipped slot)
                self.last_processed_slot = slot
                
                # Update scraper state for all active scrapers
                for scraper in active_scrapers:
                    await scraper.update_scraper_state(
                        last_processed_slot=slot,
                        mode="realtime"
                    )
                
                if success:
                    logger.info(f"Processed block at slot {slot}")
                elif processors:
                    logger.info(f"Skipped slot {slot} (no block found)")
                else:
                    logger.debug(f"Skipped slot {slot} (no scrapers needed to process)")
        
        except Exception as e:
            logger.error(f"Error checking for new blocks: {e}")