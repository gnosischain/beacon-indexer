"""
Thread pool-based parallel service that replaces the async parallel service.
Uses the worker pool for efficient processing.
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
    Parallel processing service using thread-based worker pool.
    Replaces the async-based parallel service for better performance.
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
        
        # Reset stale ranges (fixed method name)
        self.state_manager.reset_stale_ranges()
        
        # Filter active scrapers
        active_scrapers = []
        for scraper in self.scrapers:
            if scraper.one_time and await scraper.get_last_processed_slot() > 0:
                logger.info(f"Skipping one-time scraper {scraper.scraper_id} (already completed)")
                continue
            active_scrapers.append(scraper)
            
        if not active_scrapers:
            logger.warning("No active scrapers to run")
            return
            
        # Convert scraper instances to a dictionary of scraper classes
        scraper_classes = {}
        for scraper in active_scrapers:
            scraper_classes[scraper.scraper_id] = type(scraper)
            
        # Create initial ranges in the database
        logger.info("Creating initial ranges in database...")
        created_ranges = self.state_manager.bulk_create_ranges(
            scraper_classes, 
            self.start_slot, 
            self.end_slot
        )
        
        if created_ranges > 0:
            logger.info(f"Created {created_ranges} new ranges")
        else:
            logger.info("Using existing ranges from database")
            
        # Create worker pool with correct parameters
        self.worker_pool = WorkerPoolService(
            beacon_url=self.beacon_api.base_url,
            clickhouse_config={
                'host': self.clickhouse.host,
                'port': self.clickhouse.port,
                'user': self.clickhouse.user,
                'password': self.clickhouse.password,
                'database': self.clickhouse.database,
                'secure': self.clickhouse.secure,
                'verify': self.clickhouse.verify
            },
            scraper_classes=scraper_classes,
            num_workers=self.num_workers,
            existing_clickhouse=self.clickhouse
        )
        
        # Set the specs manager and state manager on the worker pool
        self.worker_pool.set_specs_manager(self.specs_manager)
        self.worker_pool.set_state_manager(self.state_manager)
        
        try:
            # Process the entire range using the worker pool
            await self.worker_pool.process_range(self.start_slot, self.end_slot)
            
            logger.info("All work completed successfully")
                
        except Exception as e:
            logger.error(f"Error during parallel processing: {e}")
            raise
        finally:
            # Final summary
            self._print_summary()
            
    def _print_summary(self):
        """Print final processing summary."""
        elapsed = time.time() - self.progress['start_time']
        
        # Get final stats from state manager
        stats_query = """
        SELECT 
            countIf(status = 'completed') as completed,
            countIf(status = 'failed') as failed,
            countIf(status = 'pending') as pending,
            countIf(status = 'processing') as processing,
            sum(rows_indexed) as total_rows
        FROM indexing_state
        WHERE start_slot >= %(start_slot)s AND end_slot <= %(end_slot)s
        """
        
        results = self.state_manager.db.execute(stats_query, {
            "start_slot": self.start_slot,
            "end_slot": self.end_slot
        })
        
        if results:
            stats = results[0]
            completed = stats['completed']
            failed = stats['failed'] 
            pending = stats['pending']
            processing = stats['processing']
            total_rows = stats['total_rows'] or 0
        else:
            completed = failed = pending = processing = total_rows = 0
        
        total_ranges = completed + failed + pending + processing
        
        logger.info("=" * 80)
        logger.info("PARALLEL PROCESSING SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total time: {elapsed/60:.2f} minutes")
        logger.info(f"Total ranges: {total_ranges}")
        logger.info(f"Completed ranges: {completed}")
        logger.info(f"Failed ranges: {failed}")
        logger.info(f"Pending ranges: {pending}")
        logger.info(f"Processing ranges: {processing}")
        logger.info(f"Total rows processed: {total_rows:,}")
        
        if elapsed > 0:
            logger.info(f"Average rate: {total_rows/elapsed:.0f} rows/second")
            
        logger.info("=" * 80)