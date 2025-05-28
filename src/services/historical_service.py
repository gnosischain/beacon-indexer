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
        
        self.start_slot = start_slot if start_slot is not None else config.scraper.historical_start_slot
        self.end_slot = end_slot # Will be determined in start() if None
        self.batch_size = batch_size if batch_size is not None else config.scraper.batch_size
        
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

        validator_scraper_instance: Optional[ValidatorScraper] = None
        other_scrapers: List[BaseScraper] = []
        for scr in self.scrapers:
            if isinstance(scr, ValidatorScraper):
                validator_scraper_instance = scr
            else:
                other_scrapers.append(scr)

        # --- ValidatorScraper specific processing ---
        if validator_scraper_instance:
            logger.info(f"Processing ValidatorScraper ({validator_scraper_instance.name}) specifically...")
            genesis_time = self.clickhouse.get_genesis_time()
            time_params = self.clickhouse.get_time_parameters()
            seconds_per_slot = time_params.get('seconds_per_slot') if time_params else None

            if genesis_time and seconds_per_slot:
                all_validator_target_slots = get_relevant_validator_slots_in_range(
                    self.start_slot, self.end_slot, genesis_time, seconds_per_slot
                )
                logger.info(f"ValidatorScraper: Identified {len(all_validator_target_slots)} target slots for range {self.start_slot}-{self.end_slot}.")

                if all_validator_target_slots:
                    # Ensure semaphore limit is at least 1, even if list is shorter than max_concurrent_requests
                    semaphore_limit = min(config.scraper.max_concurrent_requests, len(all_validator_target_slots))
                    semaphore_validator = asyncio.Semaphore(max(1, semaphore_limit))
                    
                    validator_tasks = []
                    # Filter slots to be strictly within the service's overall start/end range
                    slots_to_process_for_validator = sorted([
                        s for s in list(set(all_validator_target_slots)) 
                        if self.start_slot <= s <= self.end_slot
                    ])

                    for slot_to_process in slots_to_process_for_validator:
                        async def process_validator_slot(s, scraper_instance):
                            async with semaphore_validator:
                                logger.debug(f"ValidatorScraper: Attempting to process slot {s}")
                                mock_block_data = {"data": {"message": {"slot": str(s)}}}
                                try:
                                    await scraper_instance.process(mock_block_data)
                                except Exception as e:
                                    logger.error(f"ValidatorScraper: Error processing slot {s}: {e}")
                        
                        validator_tasks.append(process_validator_slot(slot_to_process, validator_scraper_instance))
                    
                    if validator_tasks:
                        await asyncio.gather(*validator_tasks)
                    
                    processed_validator_slots_max = max(slots_to_process_for_validator) if slots_to_process_for_validator else self.start_slot -1
                    await validator_scraper_instance.update_scraper_state(
                        last_processed_slot=max(self.start_slot -1, processed_validator_slots_max), # Ensure it's not negative if no slots
                        mode="historical"
                    )
                else:
                    logger.info(f"ValidatorScraper ({validator_scraper_instance.name}): No target slots identified in the given range or due to missing data.")
                    # Update state to reflect that this range was considered, even if no slots were processed.
                    await validator_scraper_instance.update_scraper_state(last_processed_slot=self.end_slot, mode="historical")


                logger.info(f"ValidatorScraper ({validator_scraper_instance.name}) specific processing finished.")
            else:
                logger.warning(f"ValidatorScraper ({validator_scraper_instance.name}): Could not determine target slots due to missing time parameters. It will not be processed with special logic.")
                # Optionally, add it to other_scrapers if you want it to run like a normal scraper in this case
                # other_scrapers.append(validator_scraper_instance)
        
        # --- Processing for other scrapers ---
        if other_scrapers:
            logger.info(f"Processing {len(other_scrapers)} other scrapers: {[s.name for s in other_scrapers]}")
            
            pending_one_time_scrapers = []
            for scraper in other_scrapers:
                if scraper.one_time:
                    if await scraper.should_process():
                        pending_one_time_scrapers.append(scraper)
                    else:
                        logger.info(f"One-time scraper {scraper.name} determined it should not run initially.")
            
            regular_other_scrapers = [s for s in other_scrapers if not s.one_time]

            for batch_start_slot in range(self.start_slot, self.end_slot + 1, self.batch_size):
                current_batch_end_slot = min(batch_start_slot + self.batch_size - 1, self.end_slot)
                
                active_scrapers_for_this_batch = list(regular_other_scrapers)
                
                current_one_time_for_batch = []
                for ot_scraper in pending_one_time_scrapers:
                     if await ot_scraper.should_process(): 
                        active_scrapers_for_this_batch.append(ot_scraper)
                        current_one_time_for_batch.append(ot_scraper)
                
                if not active_scrapers_for_this_batch:
                    logger.info(f"No 'other' scrapers active for batch {batch_start_slot}-{current_batch_end_slot}.")
                else:
                    logger.info(f"Processing batch {batch_start_slot}-{current_batch_end_slot} for {len(active_scrapers_for_this_batch)} scrapers: {[s.name for s in active_scrapers_for_this_batch]}")
                    await self._process_slot_batch(batch_start_slot, current_batch_end_slot, active_scrapers_for_this_batch)
                
                # Update state for all 'other' scrapers after each batch attempt
                for scraper in other_scrapers: 
                    await scraper.update_scraper_state(
                        last_processed_slot=current_batch_end_slot,
                        mode="historical"
                    )
                
                # Update list of pending_one_time_scrapers
                temp_pending_one_time = []
                for ot_scraper in pending_one_time_scrapers:
                    if await ot_scraper.should_process(): # Re-check after state update
                        temp_pending_one_time.append(ot_scraper)
                pending_one_time_scrapers = temp_pending_one_time
                logger.info(f"Other scrapers: Batch {batch_start_slot}-{current_batch_end_slot} processed. Pending one-time scrapers: {[s.name for s in pending_one_time_scrapers]}")
        else:
            logger.info("No 'other' scrapers to process.")

        logger.info(f"Historical scraping run fully completed for range {self.start_slot} - {self.end_slot}.")

    async def _process_slot_batch(self, start_slot: int, end_slot: int, scrapers_for_batch: List[BaseScraper]):
        """Process a batch of slots for the given scrapers."""
        if not scrapers_for_batch:
            logger.debug(f"Batch {start_slot}-{end_slot}: No scrapers provided for processing.")
            return

        missing_slots: Set[int] = self.clickhouse.get_missing_slots_in_range(start_slot, end_slot)

        if not missing_slots:
            logger.info(f"Batch {start_slot}-{end_slot}: No missing slots found for current set of scrapers.")
            return
        
        logger.info(f"Batch {start_slot}-{end_slot}: Attempting to process {len(missing_slots)} missing slots with scrapers: {[s.name for s in scrapers_for_batch]}")
        
        # Ensure semaphore limit is at least 1
        semaphore_limit = min(config.scraper.max_concurrent_requests, len(missing_slots))
        semaphore = asyncio.Semaphore(max(1, semaphore_limit))
        
        block_processor = BlockProcessor(self.beacon_api)
        
        batch_processor_fns = [scraper.process for scraper in scrapers_for_batch]

        if not batch_processor_fns:
            logger.info(f"Batch {start_slot}-{end_slot}: No processor functions generated for any scraper.")
            return

        tasks = []
        for slot in sorted(list(missing_slots)): 
            async def process_slot_task(s, current_processors):
                async with semaphore:
                    try:
                        await block_processor.get_and_process_block(s, current_processors)
                    except Exception as e:
                        logger.error(f"Error processing slot {s} via block_processor: {e}")
            
            tasks.append(process_slot_task(slot, batch_processor_fns))
        
        if tasks:
            await asyncio.gather(*tasks)