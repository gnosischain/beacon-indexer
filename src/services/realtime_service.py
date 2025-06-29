import asyncio
import time
from typing import List, Dict, Optional

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.utils.block_processor import BlockProcessor
from src.utils.specs_manager import SpecsManager

class RealtimeService:
    """Service to scrape realtime data from the beacon chain with state tracking."""
    
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
        
        # Initialize state manager
        self.state_manager = StateManager(clickhouse)
        
        # Initialize poll interval from specs manager if not provided
        self.poll_interval = poll_interval
        if not self.poll_interval:
            self.poll_interval = self.specs_manager.get_seconds_per_slot() // 2  # Poll at half slot time
        
        # Set managers for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
            scraper.set_state_manager(self.state_manager)
        
        # Track last processed slot per scraper/table
        self.last_slots = {}
    
    def should_process_with_validator_scraper(self, slot: int) -> bool:
        """Check if this slot should be processed by the validator scraper."""
        validator_scrapers = [s for s in self.scrapers if isinstance(s, ValidatorScraper)]
        
        if not validator_scrapers:
            return False
            
        # For realtime, we use the instance method to check if slot should be processed
        return any(scraper.should_process_slot(slot) for scraper in validator_scrapers)
    
    async def start(self):
        """Start the realtime scraper with state tracking."""
        await self.beacon_api.start()
        self.running = True
        
        # Get the last processed slot from state manager
        await self._initialize_last_slots()
        
        logger.info(f"Starting realtime scraper from slot {self.last_processed_slot + 1}")
        
        # Pre-check one-time scrapers
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
                # Check for completed one-time scrapers
                for scraper in list(active_scrapers):
                    if scraper.one_time:
                        last_processed = await scraper.get_last_processed_slot()
                        if last_processed > 0:
                            active_scrapers.remove(scraper)
                            logger.info(f"One-time scraper {scraper.scraper_id} completed and removed")
                            
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
    
    async def _initialize_last_slots(self):
        """Initialize last processed slots from state manager."""
        max_slot = 0
        
        for scraper in self.scrapers:
            for table in scraper.get_tables_written():
                # Get sync position
                last_slot = self.state_manager.get_sync_position(scraper.scraper_id, table)
                
                if last_slot is None:
                    # Try to get from completed ranges
                    query = """
                    SELECT MAX(end_slot) as max_slot
                    FROM indexing_state
                    WHERE scraper_id = %(scraper_id)s
                      AND table_name = %(table_name)s
                      AND status = 'completed'
                    """
                    
                    result = self.clickhouse.execute(query, {
                        "scraper_id": scraper.scraper_id,
                        "table_name": table
                    })
                    
                    last_slot = result[0]['max_slot'] if result and result[0]['max_slot'] else 0
                
                self.last_slots[f"{scraper.scraper_id}:{table}"] = last_slot
                max_slot = max(max_slot, last_slot)
        
        self.last_processed_slot = max_slot
    
    async def _check_new_blocks(self, active_scrapers: List[BaseScraper]):
        """Check for new blocks and process them with state tracking."""
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
                
                if not slot_scrapers:
                    # Update last processed slot even if no scrapers needed
                    self.last_processed_slot = slot
                    continue
                
                # Reset row counts for all scrapers
                for scraper in slot_scrapers:
                    scraper.reset_row_counts()
                
                # Create processor functions
                processors = [scraper.process for scraper in slot_scrapers]
                
                # Process the block
                success = False
                try:
                    success = await block_processor.get_and_process_block(slot, processors)
                except Exception as e:
                    logger.error(f"Error processing slot {slot}: {e}")
                    # Continue with next slot
                
                # Update state for each scraper/table
                for scraper in slot_scrapers:
                    row_counts = scraper.get_row_counts()
                    
                    for table in scraper.get_tables_written():
                        key = f"{scraper.scraper_id}:{table}"
                        
                        # Only update if we actually processed this slot
                        if success:
                            # Update sync position
                            self.state_manager.update_sync_position(
                                scraper.scraper_id, table, slot
                            )
                            
                            # Check if we need to create a completed range
                            last_slot = self.last_slots.get(key, 0)
                            
                            # If there's a gap, create a range entry
                            if slot > last_slot + 1:
                                # Create a range for the gap
                                range_start = last_slot + 1
                                range_end = slot
                                
                                # Align to range boundaries for consistency
                                aligned_start = (range_start // self.state_manager.range_size) * self.state_manager.range_size
                                aligned_end = ((range_end // self.state_manager.range_size) + 1) * self.state_manager.range_size
                                aligned_end = min(aligned_end, slot + 1)
                                
                                rows = row_counts.get(table, 0)
                                
                                # Claim and complete the range
                                if self.state_manager.claim_range(
                                    scraper.scraper_id, table, aligned_start, aligned_end,
                                    f"realtime_{scraper.scraper_id}", force=True
                                ):
                                    self.state_manager.complete_range(
                                        scraper.scraper_id, table, aligned_start, aligned_end, rows
                                    )
                            
                            self.last_slots[key] = slot
                
                # Update the last processed slot
                self.last_processed_slot = slot
                
                # Update scraper state (old method for compatibility)
                for scraper in active_scrapers:
                    await scraper.update_scraper_state(slot, "realtime")
                
                if success:
                    logger.info(f"Processed block at slot {slot}")
                else:
                    logger.info(f"Skipped slot {slot} (no block found)")
        
        except Exception as e:
            logger.error(f"Error checking for new blocks: {e}")