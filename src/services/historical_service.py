import asyncio
from typing import List, Optional, Set

from src.config import config
from src.utils.logger import logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.scrapers.base_scraper import BaseScraper
from src.utils.block_processor import BlockProcessor
from src.utils.time_utils import get_relevant_validator_slots_in_range 
from src.scrapers.validator_scraper import ValidatorScraper 
from src.utils.specs_manager import SpecsManager

class HistoricalService:
    """Service to scrape historical data from the beacon chain."""
    
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
        
        # Set specs manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
    
    async def start(self):
        """Start the historical scraper."""
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

        # Filter active scrapers (similar to parallel service)
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
        
        if not active_scrapers:
            logger.warning("No active scrapers to run. Exiting historical service.")
            return

        # Separate validator scrapers from other scrapers
        validator_scrapers = [s for s in active_scrapers if isinstance(s, ValidatorScraper)]
        other_scrapers = [s for s in active_scrapers if not isinstance(s, ValidatorScraper)]

        # --- ValidatorScraper specific processing ---
        if validator_scrapers:
            logger.info(f"Processing {len(validator_scrapers)} ValidatorScrapers: {[s.scraper_id for s in validator_scrapers]}")
            
            # Get time parameters from specs manager (similar to parallel service)
            genesis_time = self.clickhouse.get_genesis_time()
            seconds_per_slot = self.specs_manager.get_seconds_per_slot()
            
            if genesis_time and seconds_per_slot:
                all_validator_target_slots = get_relevant_validator_slots_in_range(
                    self.start_slot, self.end_slot, genesis_time, seconds_per_slot
                )
                logger.info(f"ValidatorScrapers: Identified {len(all_validator_target_slots)} target slots for range {self.start_slot}-{self.end_slot}")

                if all_validator_target_slots:
                    # Ensure semaphore limit is at least 1, even if list is shorter than max_concurrent_requests
                    semaphore_limit = min(config.scraper.max_concurrent_requests, len(all_validator_target_slots))
                    semaphore_validator = asyncio.Semaphore(max(1, semaphore_limit))
                    
                    # Filter slots to be strictly within the service's overall start/end range
                    slots_to_process_for_validator = sorted([
                        s for s in list(set(all_validator_target_slots)) 
                        if self.start_slot <= s <= self.end_slot
                    ])

                    logger.info(f"ValidatorScrapers: Will process {len(slots_to_process_for_validator)} slots")

                    # Process validator slots with proper error handling
                    async def process_validator_slot(slot: int, scraper_instances: List[ValidatorScraper]):
                        async with semaphore_validator:
                            logger.debug(f"ValidatorScrapers: Processing slot {slot}")
                            # Create mock block data with slot information
                            mock_block_data = {"data": {"message": {"slot": slot}}}
                            
                            for scraper in scraper_instances:
                                try:
                                    await scraper.process(mock_block_data)
                                except Exception as e:
                                    logger.error(f"ValidatorScraper {scraper.scraper_id}: Error processing slot {slot}: {e}")
                    
                    # Create tasks for all validator target slots
                    validator_tasks = []
                    for slot_to_process in slots_to_process_for_validator:
                        validator_tasks.append(process_validator_slot(slot_to_process, validator_scrapers))
                    
                    # Execute all validator tasks
                    if validator_tasks:
                        logger.info(f"Starting processing of {len(validator_tasks)} validator slots")
                        await asyncio.gather(*validator_tasks, return_exceptions=True)
                        logger.info(f"Completed processing validator slots")
                    
                    # Update state for all validator scrapers
                    processed_validator_slots_max = max(slots_to_process_for_validator) if slots_to_process_for_validator else self.start_slot - 1
                    for validator_scraper in validator_scrapers:
                        await validator_scraper.update_scraper_state(
                            last_processed_slot=max(self.start_slot - 1, processed_validator_slots_max),
                            mode="historical"
                        )
                        logger.info(f"Updated state for ValidatorScraper {validator_scraper.scraper_id} to slot {max(self.start_slot - 1, processed_validator_slots_max)}")
                else:
                    logger.info(f"ValidatorScrapers: No target slots identified in the given range")
                    # Update state to reflect that this range was considered, even if no slots were processed
                    for validator_scraper in validator_scrapers:
                        await validator_scraper.update_scraper_state(
                            last_processed_slot=self.end_slot, 
                            mode="historical"
                        )

                logger.info(f"ValidatorScrapers processing completed")
            else:
                logger.warning(f"ValidatorScrapers: Could not determine target slots due to missing time parameters (genesis_time: {genesis_time}, seconds_per_slot: {seconds_per_slot})")
        
        # --- Processing for other scrapers ---
        if other_scrapers:
            logger.info(f"Processing {len(other_scrapers)} other scrapers: {[s.scraper_id for s in other_scrapers]}")
            
            # Track which one-time scrapers are still pending
            pending_one_time_scrapers = []
            regular_scrapers = []
            
            for scraper in other_scrapers:
                if scraper.one_time:
                    # One-time scrapers are already filtered in active_scrapers, so they should all be pending
                    pending_one_time_scrapers.append(scraper)
                else:
                    regular_scrapers.append(scraper)

            logger.info(f"Regular scrapers: {[s.scraper_id for s in regular_scrapers]}")
            logger.info(f"Pending one-time scrapers: {[s.scraper_id for s in pending_one_time_scrapers]}")

            # Process in batches
            for batch_start_slot in range(self.start_slot, self.end_slot + 1, self.batch_size):
                current_batch_end_slot = min(batch_start_slot + self.batch_size - 1, self.end_slot)
                
                # Build active scrapers for this batch
                active_scrapers_for_this_batch = list(regular_scrapers)
                
                # Check which one-time scrapers should still process
                current_one_time_for_batch = []
                for ot_scraper in list(pending_one_time_scrapers):  # Create copy to allow modification
                     if await ot_scraper.should_process(): 
                        active_scrapers_for_this_batch.append(ot_scraper)
                        current_one_time_for_batch.append(ot_scraper)
                     else:
                        # This one-time scraper has completed, remove from pending
                        pending_one_time_scrapers.remove(ot_scraper)
                        logger.info(f"One-time scraper {ot_scraper.scraper_id} completed and removed from pending list")
                
                if not active_scrapers_for_this_batch:
                    logger.info(f"No active scrapers for batch {batch_start_slot}-{current_batch_end_slot}")
                else:
                    logger.info(f"Processing batch {batch_start_slot}-{current_batch_end_slot} for {len(active_scrapers_for_this_batch)} scrapers: {[s.scraper_id for s in active_scrapers_for_this_batch]}")
                    await self._process_slot_batch(batch_start_slot, current_batch_end_slot, active_scrapers_for_this_batch)
                
                # Update state for all other scrapers after each batch
                for scraper in other_scrapers: 
                    await scraper.update_scraper_state(
                        last_processed_slot=current_batch_end_slot,
                        mode="historical"
                    )
                
                logger.info(f"Batch {batch_start_slot}-{current_batch_end_slot} completed. Remaining pending one-time scrapers: {[s.scraper_id for s in pending_one_time_scrapers]}")
        else:
            logger.info("No other scrapers to process")

        logger.info(f"Historical scraping completed for range {self.start_slot} - {self.end_slot}")

    async def _process_slot_batch(self, start_slot: int, end_slot: int, scrapers_for_batch: List[BaseScraper]):
        """Process a batch of slots for the given scrapers."""
        if not scrapers_for_batch:
            logger.debug(f"Batch {start_slot}-{end_slot}: No scrapers provided for processing")
            return

        # Get missing slots from the database
        missing_slots_list = self.clickhouse.get_missing_slots_in_range(start_slot, end_slot)
        missing_slots: Set[int] = set(missing_slots_list)

        if not missing_slots:
            logger.info(f"Batch {start_slot}-{end_slot}: No missing slots found")
            return
        
        logger.info(f"Batch {start_slot}-{end_slot}: Processing {len(missing_slots)} missing slots with scrapers: {[s.scraper_id for s in scrapers_for_batch]}")
        
        # Ensure semaphore limit is at least 1
        semaphore_limit = min(config.scraper.max_concurrent_requests, len(missing_slots))
        semaphore = asyncio.Semaphore(max(1, semaphore_limit))
        
        block_processor = BlockProcessor(self.beacon_api)
        
        # Create processor functions for each scraper
        batch_processor_fns = [scraper.process for scraper in scrapers_for_batch]

        if not batch_processor_fns:
            logger.info(f"Batch {start_slot}-{end_slot}: No processor functions generated")
            return

        # Process slots with proper error handling
        async def process_slot_task(slot: int, current_processors: List):
            async with semaphore:
                try:
                    await block_processor.get_and_process_block(slot, current_processors)
                except Exception as e:
                    logger.error(f"Error processing slot {slot}: {e}")

        # Create tasks for all missing slots
        tasks = []
        for slot in sorted(list(missing_slots)): 
            tasks.append(process_slot_task(slot, batch_processor_fns))
        
        # Execute all tasks
        if tasks:
            logger.info(f"Starting processing of {len(tasks)} slots in batch {start_slot}-{end_slot}")
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info(f"Completed processing batch {start_slot}-{end_slot}")