import asyncio
from typing import List, Optional, Set, Dict
from datetime import datetime

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.scrapers.base_scraper import BaseScraper
from src.utils.block_processor import BlockProcessor
from src.utils.time_utils import get_relevant_validator_slots_in_range 
from src.scrapers.validator_scraper import ValidatorScraper 
from src.utils.specs_manager import SpecsManager

class HistoricalService:
    """Service to scrape historical data from the beacon chain with state tracking."""
    
    def __init__(
        self,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List[BaseScraper],
        specs_manager: SpecsManager, 
        start_slot: Optional[int] = None,
        end_slot: Optional[int] = None,
        batch_size: Optional[int] = None
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.specs_manager = specs_manager
        
        self.start_slot = start_slot if start_slot is not None else config.scraper.historical_start_slot
        self.end_slot = end_slot # Will be determined in start() if None
        self.batch_size = batch_size if batch_size is not None else config.scraper.batch_size
        
        # Initialize state manager
        self.state_manager = StateManager(clickhouse)
        
        # Set managers for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
            scraper.set_state_manager(self.state_manager)
    
    async def start(self):
        """Start the historical scraper with state tracking."""
        await self.beacon_api.start()
        
        if self.start_slot is None:
            logger.error("Historical start slot is not configured. Aborting.")
            return

        if self.end_slot is None:
            try:
                latest_header = await self.beacon_api.get_block_header("head")
                if latest_header and "header" in latest_header and "message" in latest_header["header"] and "slot" in latest_header["header"]["message"]:
                    self.end_slot = int(latest_header["header"]["message"]["slot"])
                    logger.info(f"Setting end slot to latest slot from chain: {self.end_slot}")
                else:
                    logger.error("Could not determine latest slot from beacon chain. End slot remains None. Aborting.")
                    return
            except Exception as e:
                logger.error(f"Error fetching latest block header: {e}. End slot remains None. Aborting.")
                return

        if self.end_slot is None: 
            logger.error("Historical end slot is not set after attempting to fetch from chain. Aborting.")
            return
            
        if self.start_slot > self.end_slot:
            logger.info(f"Start slot {self.start_slot} is after end slot {self.end_slot}. Nothing to process.")
            return

        logger.info(f"Starting historical run from slot {self.start_slot} to {self.end_slot}")
        
        # Reset any stale jobs before starting
        self.state_manager.reset_stale_ranges()

        # Filter active scrapers
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
                active_scrapers.append(scraper)
                logger.info(f"Regular scraper {scraper.scraper_id} will be processed")
        
        if not active_scrapers:
            logger.warning("No active scrapers to run. Exiting historical service.")
            return

        # Separate validator scrapers from other scrapers
        validator_scrapers = [s for s in active_scrapers if isinstance(s, ValidatorScraper)]
        other_scrapers = [s for s in active_scrapers if not isinstance(s, ValidatorScraper)]

        # Process validator scrapers with special handling
        if validator_scrapers:
            await self._process_validator_scrapers(validator_scrapers)
        
        # Process other scrapers with state tracking
        if other_scrapers:
            await self._process_other_scrapers(other_scrapers)

        logger.info(f"Historical scraping completed for range {self.start_slot} - {self.end_slot}")

    async def _process_validator_scrapers(self, validator_scrapers: List[ValidatorScraper]):
        """Process validator scrapers with their special slot requirements."""
        logger.info(f"Processing {len(validator_scrapers)} ValidatorScrapers")
        
        # Get time parameters
        genesis_time = self.clickhouse.get_genesis_time()
        seconds_per_slot = self.specs_manager.get_seconds_per_slot()
        
        if not genesis_time or not seconds_per_slot:
            logger.warning("Cannot determine slot timing parameters for validator scrapers")
            return
        
        # For each validator scraper
        for scraper in validator_scrapers:
            # Get all tables this scraper writes to
            tables = scraper.get_tables_written()
            
            for table in tables:
                # Process ranges for this table
                while True:
                    # Get next range to process
                    range_info = self.state_manager.get_next_range(
                        scraper.scraper_id, 
                        table, 
                        f"historical_{scraper.scraper_id}",
                        self.end_slot
                    )
                    
                    if not range_info:
                        break
                    
                    start_slot, end_slot = range_info
                    
                    # Get target slots for this range
                    target_slots = get_relevant_validator_slots_in_range(
                        start_slot, end_slot, genesis_time, seconds_per_slot
                    )
                    
                    if not target_slots:
                        # No target slots in this range, mark as completed
                        self.state_manager.complete_range(
                            scraper.scraper_id, table, start_slot, end_slot, 0
                        )
                        continue
                    
                    try:
                        # Process target slots
                        scraper.reset_row_counts()
                        
                        for slot in target_slots:
                            if slot < start_slot or slot >= end_slot:
                                continue
                                
                            mock_block_data = {"data": {"message": {"slot": slot}}}
                            await scraper.process(mock_block_data)
                        
                        # Get row counts and mark range as completed
                        row_counts = scraper.get_row_counts()
                        total_rows = row_counts.get(table, 0)
                        
                        self.state_manager.complete_range(
                            scraper.scraper_id, table, start_slot, end_slot, total_rows
                        )
                        
                        logger.info(f"Completed validator range {start_slot}-{end_slot} for {table}: {total_rows} rows")
                        
                    except Exception as e:
                        logger.error(f"Error processing validator range {start_slot}-{end_slot}: {e}")
                        self.state_manager.fail_range(
                            scraper.scraper_id, table, start_slot, end_slot, str(e)
                        )

    async def _process_other_scrapers(self, scrapers: List[BaseScraper]):
        """Process non-validator scrapers with state tracking."""
        logger.info(f"Processing {len(scrapers)} other scrapers")
        
        # Create a mapping of scrapers to their tables
        scraper_tables = {}
        for scraper in scrapers:
            tables = scraper.get_tables_written()
            scraper_tables[scraper.scraper_id] = tables
        
        # Process ranges
        while True:
            # Find the next range to process across all scrapers/tables
            next_range = None
            next_scraper = None
            next_table = None
            
            for scraper in scrapers:
                for table in scraper_tables[scraper.scraper_id]:
                    range_info = self.state_manager.get_next_range(
                        scraper.scraper_id,
                        table,
                        f"historical_{scraper.scraper_id}",
                        self.end_slot
                    )
                    
                    if range_info and (not next_range or range_info[0] < next_range[0]):
                        next_range = range_info
                        next_scraper = scraper
                        next_table = table
            
            if not next_range:
                break
            
            start_slot, end_slot = next_range
            
            # Process this range for all applicable scrapers
            await self._process_slot_range_with_state(
                start_slot, end_slot, scrapers
            )

    async def _process_slot_range_with_state(self, start_slot: int, end_slot: int, 
                                           scrapers: List[BaseScraper]):
        """Process a slot range with state tracking."""
        # Reset row counts for all scrapers
        for scraper in scrapers:
            scraper.reset_row_counts()
        
        # Get missing slots from the database
        missing_slots = self.clickhouse.get_missing_slots_in_range(start_slot, end_slot)
        
        if not missing_slots:
            # No missing slots, but we still need to mark ranges as complete
            for scraper in scrapers:
                for table in scraper.get_tables_written():
                    self.state_manager.complete_range(
                        scraper.scraper_id, table, start_slot, end_slot, 0
                    )
            return
        
        logger.info(f"Processing {len(missing_slots)} missing slots in range {start_slot}-{end_slot}")
        
        # Process slots
        semaphore = asyncio.Semaphore(config.scraper.max_concurrent_requests)
        block_processor = BlockProcessor(self.beacon_api)
        
        async def process_slot(slot: int):
            async with semaphore:
                try:
                    processors = [scraper.process for scraper in scrapers]
                    await block_processor.get_and_process_block(slot, processors)
                except Exception as e:
                    logger.error(f"Error processing slot {slot}: {e}")

        # Process all missing slots
        tasks = [process_slot(slot) for slot in missing_slots]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update state for all scrapers/tables
        for scraper in scrapers:
            row_counts = scraper.get_row_counts()
            for table in scraper.get_tables_written():
                total_rows = row_counts.get(table, 0)
                self.state_manager.complete_range(
                    scraper.scraper_id, table, start_slot, end_slot, total_rows
                )
                
        logger.info(f"Completed range {start_slot}-{end_slot}")