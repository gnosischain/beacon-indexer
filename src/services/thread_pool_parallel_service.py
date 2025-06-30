"""
Fixed thread pool-based parallel service with proper event loop handling.
"""
import asyncio
import time
from typing import List, Optional, Dict
from datetime import datetime

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.services.worker_pool_service import WorkerPoolService, WorkResult
from src.utils.logger import logger
from src.scrapers.base_scraper import BaseScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.utils.specs_manager import SpecsManager


class ThreadPoolParallelService:
    """
    Fixed implementation with proper event loop and state management.
    """
    
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
        self.start_slot = start_slot or 0
        self.end_slot = end_slot
        self.num_workers = num_workers
        self.worker_batch_size = worker_batch_size
        
        # State manager
        self.state_manager = StateManager(clickhouse)
        
        # Set managers for scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
            if hasattr(scraper, 'set_state_manager'):
                scraper.set_state_manager(self.state_manager)
            
        # Worker pool
        self.worker_pool = None
        
        # Progress tracking
        self.progress = {
            'total_ranges': 0,
            'completed_ranges': 0,
            'failed_ranges': 0,
            'total_rows': 0,
            'start_time': None
        }
        
    async def start(self) -> None:
        """Start parallel processing using thread pool."""
        await self.beacon_api.start()
        self.progress['start_time'] = time.time()
        
        # Get end slot if not provided
        if self.end_slot is None:
            try:
                latest = await self.beacon_api.get_block_header("head")
                self.end_slot = int(latest["header"]["message"]["slot"])
                logger.info(f"Set end slot to latest: {self.end_slot}")
            except Exception as e:
                logger.error(f"Could not determine end slot: {e}")
                return
                
        logger.info(
            f"Starting thread pool parallel processing: "
            f"slots {self.start_slot}-{self.end_slot}, "
            f"workers={self.num_workers}, batch_size={self.worker_batch_size}"
        )
        
        # Reset stale ranges
        self.state_manager.reset_stale_ranges()
        
        # Filter active scrapers
        active_scrapers = []
        for scraper in self.scrapers:
            if scraper.one_time:
                # Check if this one-time scraper should run
                should_run = await scraper.should_process()
                if not should_run:
                    logger.info(f"Skipping one-time scraper {scraper.scraper_id} (already completed)")
                    continue
            active_scrapers.append(scraper)
            
        if not active_scrapers:
            logger.warning("No active scrapers to run")
            return
            
        # Log active scrapers with their IDs
        logger.info(f"Active scrapers: {[s.scraper_id for s in active_scrapers]}")
            
        # Convert scraper instances to a dictionary of scraper classes
        scraper_classes = {}
        for scraper in active_scrapers:
            # Use the scraper's actual ID as the key
            scraper_classes[scraper.scraper_id] = type(scraper)
            logger.debug(f"Registered scraper class: {scraper.scraper_id} -> {type(scraper).__name__}")
            
        # Create initial ranges in the database
        logger.info("Creating initial ranges in database...")
        created_ranges = self.state_manager.bulk_create_ranges(
            active_scrapers,  # Pass scraper instances
            self.start_slot, 
            self.end_slot
        )
        
        if created_ranges > 0:
            logger.info(f"Created {created_ranges} new ranges")
        else:
            logger.info("Using existing ranges from database")
            
        # Create worker pool with fixed implementation
        self.worker_pool = WorkerPoolService(
            beacon_url=self.beacon_api.base_url,
            existing_clickhouse=self.clickhouse,  # Pass existing connection
            scraper_classes=scraper_classes,
            specs_manager=self.specs_manager,
            num_workers=self.num_workers
        )
        
        try:
            # Process the full range
            await self.worker_pool.process_range(self.start_slot, self.end_slot)
            
            # Log final statistics
            elapsed_time = time.time() - self.progress['start_time']
            logger.info(
                f"Parallel processing completed in {elapsed_time:.1f}s. "
                f"Total rows indexed: {self.worker_pool.stats['total_rows']}"
            )
            
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            raise
        finally:
            # Ensure beacon API is properly closed
            if self.beacon_api:
                await self.beacon_api.stop()