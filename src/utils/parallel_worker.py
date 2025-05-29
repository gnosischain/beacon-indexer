import asyncio
import os
import logging
import traceback
import time
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.bulk_insertion_service import BulkInsertionService
from src.utils.logger import logger, setup_logger
from src.utils.block_processor import BlockProcessor
from src.scrapers.validator_scraper import ValidatorScraper

class ParallelWorker:
    """Worker for processing a specific range of blocks in parallel."""
    
    def __init__(
        self, 
        worker_id: str,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List,
        start_slot: int,
        end_slot: int,
        batch_size: int = 1000,
        max_concurrent_requests: int = 100
    ):
        self.worker_id = worker_id
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.start_slot = start_slot
        self.end_slot = end_slot
        
        # Increase batch size for better throughput
        self.batch_size = min(10000, batch_size * 5)
        
        # Limit concurrent requests to avoid overwhelming the network
        self.max_concurrent_requests = max_concurrent_requests
        
        # Initialize a block processor for this worker
        self.block_processor = BlockProcessor(self.beacon_api)
        
        # Set up bulk insertion service for faster DB writes
        self.bulk_inserter = BulkInsertionService(
            clickhouse_service=clickhouse,
            max_parallel_inserts=5,
            batch_size=1000
        )
        
        # Use the standard logger - create a worker-specific logger
        self.logger = setup_logger(f"worker_{worker_id}", log_level=config.scraper.log_level)
        
        # Pre-calculate validator target slots for efficiency
        self.validator_target_slots = set()
        self._calculate_validator_target_slots()
        
        logger.info(f"Worker {worker_id} initialized for slots {start_slot}-{end_slot}, batch_size={self.batch_size}")
    
    def _calculate_validator_target_slots(self):
        """Pre-calculate which slots the validator scraper should process."""
        validator_scrapers = [s for s in self.scrapers if isinstance(s, ValidatorScraper)]
        
        if validator_scrapers:
            target_slots = ValidatorScraper.get_target_slots_in_range(
                self.start_slot, self.end_slot, self.clickhouse
            )
            self.validator_target_slots = set(target_slots)
            
            if self.validator_target_slots:
                self.logger.info(
                    f"Worker {self.worker_id}: Pre-calculated {len(self.validator_target_slots)} "
                    f"validator target slots in range {self.start_slot}-{self.end_slot}"
                )
            else:
                self.logger.info(f"Worker {self.worker_id}: No validator target slots in range")
    
    def should_process_with_validator_scraper(self, slot: int) -> bool:
        """Check if this slot should be processed by the validator scraper."""
        return slot in self.validator_target_slots
    
    async def process(self) -> bool:
        """Process the assigned block range."""
        self.logger.info(f"Worker {self.worker_id} starting to process slots {self.start_slot}-{self.end_slot}")
        
        try:
            # Check one-time scrapers at start (they might complete during processing)
            active_scrapers = []
            for scraper in self.scrapers:
                if scraper.one_time:
                    should_run = await scraper.should_process()
                    if should_run:
                        active_scrapers.append(scraper)
                else:
                    # Regular scrapers always active
                    active_scrapers.append(scraper)
            
            if not active_scrapers:
                self.logger.info(f"Worker {self.worker_id}: No active scrapers to run. Skipping range.")
                return True
            
            # Process slots in batches
            current_start = self.start_slot
            start_time = time.time()
            processed_slots = 0
            successful_slots = 0
            
            while current_start < self.end_slot:
                batch_end = min(current_start + self.batch_size, self.end_slot)
                
                # Check if one-time scrapers have completed during processing
                for scraper in list(active_scrapers):
                    if scraper.one_time:
                        last_processed = await scraper.get_last_processed_slot()
                        if last_processed > 0:
                            # This one-time scraper has completed, remove it
                            active_scrapers.remove(scraper)
                            self.logger.info(f"Worker {self.worker_id}: One-time scraper {scraper.scraper_id} completed and removed")
                
                if not active_scrapers:
                    self.logger.info(f"Worker {self.worker_id}: All scrapers have completed. Finishing early.")
                    break
                
                success_count = await self._process_batch(current_start, batch_end, active_scrapers)
                processed_slots += (batch_end - current_start)
                successful_slots += success_count
                
                # Calculate and log processing speed
                elapsed = time.time() - start_time
                if elapsed > 0:
                    slots_per_second = processed_slots / elapsed
                    successful_per_second = successful_slots / elapsed
                    remaining_slots = self.end_slot - batch_end
                    estimated_time = remaining_slots / slots_per_second if slots_per_second > 0 else 0
                    
                    self.logger.info(
                        f"Worker {self.worker_id} progress: {processed_slots}/{self.end_slot - self.start_slot} slots " +
                        f"({slots_per_second:.2f} slots/sec, {successful_per_second:.2f} successful/sec, " +
                        f"~{estimated_time/60:.2f} minutes remaining)"
                    )
                
                current_start = batch_end
                
                # Flush the bulk inserter periodically to ensure data gets written
                await self.bulk_inserter.flush_all_queues()
            
            # Final flush to ensure all data is written
            await self.bulk_inserter.flush_all_queues()
            
            elapsed = time.time() - start_time
            if elapsed > 0:
                final_rate = processed_slots / elapsed
                self.logger.info(
                    f"Worker {self.worker_id} completed processing {processed_slots} slots in {elapsed:.2f} seconds " +
                    f"({final_rate:.2f} slots/sec, {successful_slots} successful blocks)"
                )
            
            return True
        
        except Exception as e:
            self.logger.error(f"Worker {self.worker_id} error: {e}")
            self.logger.error(traceback.format_exc())
            
            # Try to flush any remaining data even on error
            try:
                await self.bulk_inserter.flush_all_queues()
            except:
                pass
                
            return False
    
    async def _process_batch(self, start_slot: int, end_slot: int, active_scrapers: List) -> int:
        """Process a batch of slots with optimized concurrent processing."""
        self.logger.info(f"Worker {self.worker_id} processing batch {start_slot}-{end_slot}")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def process_slot_with_semaphore(slot: int):
            async with semaphore:
                # Filter scrapers based on slot requirements
                slot_scrapers = []
                for scraper in active_scrapers:
                    # If it's a validator scraper, only include it if this is a target slot
                    if isinstance(scraper, ValidatorScraper):
                        if self.should_process_with_validator_scraper(slot):
                            slot_scrapers.append(scraper)
                        # else: skip validator scraper for this slot
                    else:
                        # Non-validator scrapers process all slots
                        slot_scrapers.append(scraper)
                
                # If no scrapers should process this slot, skip it
                if not slot_scrapers:
                    return False
                
                # Create processor functions for each scraper
                processors = []
                for scraper in slot_scrapers:
                    processors.append(scraper.process)
                
                # Try to process the slot
                try:
                    await self.block_processor.get_and_process_block(slot, processors)
                    return True
                except Exception as e:
                    if "404" in str(e):
                        # This is normal - not all slots have blocks
                        return False
                    else:
                        self.logger.warning(f"Worker {self.worker_id} error processing slot {slot}: {e}")
                        return False
        
        # Process slots in smaller sub-batches for better memory management
        success_count = 0
        sub_batch_size = 1000  # Process 1000 slots at a time
        
        for sub_start in range(start_slot, end_slot, sub_batch_size):
            sub_end = min(sub_start + sub_batch_size, end_slot)
            
            # Create tasks for each slot in the sub-batch
            tasks = [process_slot_with_semaphore(slot) for slot in range(sub_start, sub_end)]
            
            # Wait for all tasks to complete with timeout
            if tasks:
                try:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    success_count += sum(1 for r in results if r is True)
                except Exception as e:
                    self.logger.error(f"Worker {self.worker_id} sub-batch error: {e}")
                    # Continue processing other sub-batches
        
        return success_count