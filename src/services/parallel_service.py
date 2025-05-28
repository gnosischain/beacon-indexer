import asyncio
import os
import time
import json
from typing import Dict, List, Any, Tuple, Optional
import math

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger
from src.utils.parallel_worker import ParallelWorker
from src.utils.specs_manager import SpecsManager
from src.scrapers.base_scraper import BaseScraper

class ParallelService:
    """Service to manage parallel processing of beacon chain blocks."""
    
    def __init__(
        self,
        beacon_api: BeaconAPIService,
        clickhouse: ClickHouseService,
        scrapers: List[BaseScraper],
        specs_manager: SpecsManager,
        start_slot: Optional[int] = None,
        end_slot: Optional[int] = None,
        num_workers: int = 4,
        worker_batch_size: int = 100
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.scrapers = scrapers
        self.specs_manager = specs_manager
        self.start_slot = start_slot
        self.end_slot = end_slot
        self.num_workers = num_workers
        self.worker_batch_size = worker_batch_size
        
        # Read max_concurrent_requests from config
        self.max_concurrent_requests = config.scraper.max_concurrent_requests
        
        # Calculate optimal range size based on total range and number of workers
        self.range_size = self._calculate_optimal_range_size()
        
        logger.info(f"ParallelService initialized with {num_workers} workers")
        logger.info(f"Range size: {self.range_size}, Batch size: {worker_batch_size}, Max concurrent requests: {self.max_concurrent_requests}")
        
        # In-memory state tracking
        self.completed_ranges = []
        self.active_ranges = []
        self.next_slot = self.start_slot or 0
        self.last_processed_slot = self.start_slot or 0
        
        # Set specs manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
    
    def _calculate_optimal_range_size(self) -> int:
        """Calculate the optimal range size based on total slots and workers."""
        if self.start_slot is not None and self.end_slot is not None:
            total_slots = self.end_slot - self.start_slot
            # Much larger ranges for better efficiency - aim for just 2 ranges per worker
            optimal_size = max(10000, total_slots // (self.num_workers * 2))
            # No upper cap - let the workers handle large ranges effectively
            return optimal_size
        return 10000
    
    def _get_next_slot_range(self, end_slot: int) -> Optional[Tuple[int, int]]:
        """Get the next slot range to process."""
        # Skip if we've reached the end
        if self.next_slot >= end_slot:
            return None
        
        # Calculate end slot for this range
        range_end = min(self.next_slot + self.range_size, end_slot)
        
        # Make sure this range doesn't overlap with active ranges
        for start, end in self.active_ranges:
            if (start <= self.next_slot < end) or (start < range_end <= end):
                # Range overlap, try finding the next free range
                self.next_slot = max(end, self.next_slot)
                if self.next_slot >= end_slot:
                    return None
                range_end = min(self.next_slot + self.range_size, end_slot)
        
        # Update next slot
        next_slot = self.next_slot
        self.next_slot = range_end
        
        # Add to active ranges
        self.active_ranges.append((next_slot, range_end))
        
        return (next_slot, range_end)
    
    def _mark_range_complete(self, start_slot: int, end_slot: int) -> None:
        """Mark a range as complete in the in-memory state."""
        # Remove from active ranges
        if (start_slot, end_slot) in self.active_ranges:
            self.active_ranges.remove((start_slot, end_slot))
        
        # Add to completed ranges
        self.completed_ranges.append((start_slot, end_slot))
        
        # Sort completed ranges
        self.completed_ranges.sort()
        
        # Merge adjacent ranges
        merged_ranges = []
        for range_item in self.completed_ranges:
            if not merged_ranges or range_item[0] > merged_ranges[-1][1]:
                merged_ranges.append(range_item)
            else:
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], range_item[1]))
        
        self.completed_ranges = merged_ranges
        
        # Update last_processed_slot more aggressively
        if merged_ranges:
            # Find the highest contiguous range starting from our initial start point
            current_slot = self.start_slot or 0
            for range_start, range_end in merged_ranges:
                if range_start <= current_slot <= range_end:
                    current_slot = range_end
                elif range_start > current_slot:
                    break
            self.last_processed_slot = max(self.last_processed_slot, current_slot)
        
        # Also update scraper_state in database for each scraper
        for scraper in self.scrapers:
            self.clickhouse.update_scraper_state(
                scraper_id=f"parallel_{scraper.scraper_id}",
                last_processed_slot=self.last_processed_slot,
                mode="parallel"
            )
        
        logger.info(f"Marked range {start_slot}-{end_slot} as complete. Last processed: {self.last_processed_slot}")
    
    async def start(self) -> None:
        """Start the parallel processing service."""
        await self.beacon_api.start()
        
        # Filter one-time scrapers that should run
        active_scrapers = []
        for scraper in self.scrapers:
            if scraper.one_time:
                should_run = await scraper.should_process()
                if should_run:
                    active_scrapers.append(scraper)
            else:
                # Regular scrapers always get included
                active_scrapers.append(scraper)
        
        if not active_scrapers:
            logger.warning("No active scrapers to run. Exiting parallel service.")
            return
        
        # If end_slot is not provided, get the latest slot
        if self.end_slot is None:
            try:
                latest_header = await self.beacon_api.get_block_header("head")
                self.end_slot = int(latest_header["header"]["message"]["slot"])
                logger.info(f"Using latest slot as end_slot: {self.end_slot}")
            except Exception as e:
                logger.error(f"Error getting latest slot: {e}")
                return
        
        # If start_slot is not provided, check for existing progress
        if self.start_slot is None:
            # Get the minimum last processed slot across all scrapers
            min_last_processed = float('inf')
            for scraper in active_scrapers:
                last_processed = await scraper.get_last_processed_slot()
                if last_processed < min_last_processed:
                    min_last_processed = last_processed
            
            if min_last_processed < float('inf'):
                self.next_slot = min_last_processed
                self.last_processed_slot = min_last_processed
                logger.info(f"Resuming from last processed slot: {self.last_processed_slot}")
            else:
                self.next_slot = 0
                self.last_processed_slot = 0
                logger.info("Starting from slot 0 (no previous progress found)")
        
        logger.info(f"Starting parallel processing from slot {self.next_slot} to {self.end_slot}")
        
        active_workers = {}
        
        while self.next_slot < self.end_slot or active_workers:
            # Clean up completed workers
            for worker_id in list(active_workers.keys()):
                if active_workers[worker_id]["task"].done():
                    task = active_workers[worker_id]["task"]
                    start_slot = active_workers[worker_id]["start_slot"]
                    end_slot = active_workers[worker_id]["end_slot"]
                    
                    try:
                        result = task.result()
                        if result:
                            logger.info(f"Worker {worker_id} completed successfully")
                            self._mark_range_complete(start_slot, end_slot)
                        else:
                            logger.error(f"Worker {worker_id} failed")
                    except Exception as e:
                        logger.error(f"Worker {worker_id} error: {e}")
                    
                    del active_workers[worker_id]
            
            # Start new workers if we have capacity
            while len(active_workers) < self.num_workers and self.next_slot < self.end_slot:
                # Get the next slot range
                range_info = self._get_next_slot_range(self.end_slot)
                
                if not range_info:
                    break
                
                start_slot, end_slot = range_info
                worker_id = f"worker_{len(active_workers)}"
                
                logger.info(f"Starting {worker_id} for slots {start_slot}-{end_slot}")
                
                # Create a new beacon API service for this worker
                worker_beacon_api = BeaconAPIService(self.beacon_api.base_url)
                await worker_beacon_api.start()
                
                # Create the worker
                worker = ParallelWorker(
                    worker_id=worker_id,
                    beacon_api=worker_beacon_api,
                    clickhouse=self.clickhouse,
                    scrapers=active_scrapers,  # Use only active scrapers
                    start_slot=start_slot,
                    end_slot=end_slot,
                    batch_size=self.worker_batch_size,
                    max_concurrent_requests=self.max_concurrent_requests
                )
                
                # Start the worker
                task = asyncio.create_task(worker.process())
                
                # Add to active workers
                active_workers[worker_id] = {
                    "task": task,
                    "start_slot": start_slot,
                    "end_slot": end_slot
                }
                
                # Small delay to prevent overloading
                await asyncio.sleep(0.5)
            
            # Wait a bit before checking again
            await asyncio.sleep(1)
            
            # Log progress periodically
            if active_workers:
                total_slots = self.end_slot - (self.start_slot or 0)
                processed_slots = self.last_processed_slot - (self.start_slot or 0)
                if total_slots > 0:
                    progress = (processed_slots / total_slots) * 100
                    logger.info(f"Progress: {progress:.2f}% ({processed_slots}/{total_slots} slots). Active workers: {len(active_workers)}")
        
        logger.info(f"Parallel processing completed. Last processed slot: {self.last_processed_slot}")