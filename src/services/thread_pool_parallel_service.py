"""
Fixed thread pool parallel service that works with the existing codebase.
This is a drop-in replacement for the existing thread_pool_parallel_service.py
"""
import asyncio
import time
from typing import List, Optional, Dict, Set
from datetime import datetime

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.services.worker_pool_service import WorkerPoolService
from src.utils.logger import logger
from src.scrapers.base_scraper import BaseScraper
from src.utils.specs_manager import SpecsManager
from src.core.datasets import DatasetRegistry


class ThreadPoolParallelService:
    """
    Fixed implementation that properly handles beacon API and worker distribution.
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
        
        # Dataset registry
        self.dataset_registry = DatasetRegistry()
        
        # Track enabled scrapers
        self.enabled_scraper_ids = [s.scraper_id for s in scrapers]
        logger.info(f"Enabled scrapers: {self.enabled_scraper_ids}")
        
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
        """Start parallel processing with proper state management."""
        self.progress['start_time'] = time.time()
        
        logger.info(f"Starting ThreadPoolParallelService for slots {self.start_slot}-{self.end_slot}")
        logger.info(f"Workers: {self.num_workers}, Batch size: {self.worker_batch_size}")
        
        # Note: Beacon API should already be started in main.py
        # No need to check or start it here
            
        # Get end slot if not provided
        if self.end_slot is None:
            await self._determine_end_slot()
            
        # Clean up old state entries (optional - already done in main.py)
        # await self._cleanup_old_state_entries()
        
        # Initialize state ranges for enabled scrapers only
        created_ranges = await self._initialize_state_ranges()
        
        if created_ranges == 0:
            logger.info("No new ranges created. Using existing ranges or checking for work...")
            
        # Create scraper classes dict for worker pool
        scraper_classes = self._create_scraper_classes_dict()
        
        # Calculate how many workers per scraper
        workers_per_scraper = self._calculate_workers_per_scraper()
        logger.info(f"Worker distribution plan: {workers_per_scraper}")
        
        # Create worker pool with better configuration
        self.worker_pool = WorkerPoolService(
            beacon_url=self.beacon_api.base_url,
            existing_clickhouse=self.clickhouse,
            scraper_classes=scraper_classes,
            specs_manager=self.specs_manager,
            num_workers=self.num_workers
        )
        
        # If the worker pool doesn't support enabled_scrapers parameter,
        # we'll work with what we have
        if hasattr(self.worker_pool, 'enabled_scrapers'):
            self.worker_pool.enabled_scrapers = self.enabled_scraper_ids
        
        try:
            # Process the range
            await self.worker_pool.process_range(self.start_slot, self.end_slot)
            
            # Log final statistics
            elapsed_time = time.time() - self.progress['start_time']
            logger.info(
                f"Parallel processing completed in {elapsed_time:.1f}s. "
                f"Check worker pool statistics for details."
            )
            
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            raise
        finally:
            # Note: Don't close beacon API here, it's managed by main.py
            pass
                
    async def _determine_end_slot(self):
        """Determine end slot from chain head."""
        try:
            latest_header = await self.beacon_api.get_block_header("head")
            self.end_slot = int(latest_header["header"]["message"]["slot"])
            logger.info(f"Set end slot to chain head: {self.end_slot}")
        except Exception as e:
            raise RuntimeError(f"Could not determine end slot: {e}")
            
    def _calculate_workers_per_scraper(self) -> Dict[str, int]:
        """Calculate how many workers each scraper should get."""
        num_scrapers = len(self.enabled_scraper_ids)
        if num_scrapers == 0:
            return {}
            
        # Equal distribution with remainder handling
        base_workers = self.num_workers // num_scrapers
        remainder = self.num_workers % num_scrapers
        
        workers_per_scraper = {}
        for i, scraper_id in enumerate(self.enabled_scraper_ids):
            # Give extra worker to first scrapers if there's remainder
            workers_per_scraper[scraper_id] = base_workers + (1 if i < remainder else 0)
            
        return workers_per_scraper
            
    async def _cleanup_old_state_entries(self):
        """Clean up state entries for non-enabled scrapers."""
        logger.info("Cleaning up old state entries...")
        
        # Get all unique datasets in indexing_state
        query = """
        SELECT DISTINCT dataset
        FROM indexing_state FINAL
        WHERE mode = 'historical'
        """
        
        results = self.clickhouse.execute(query)
        existing_datasets = {row['dataset'] for row in results}
        
        # Get valid datasets for enabled scrapers
        valid_datasets = set()
        for scraper_id in self.enabled_scraper_ids:
            datasets = self.dataset_registry.get_datasets_for_scraper(scraper_id)
            for dataset in datasets:
                valid_datasets.add(dataset.name)
                
        # Find datasets to remove
        datasets_to_remove = existing_datasets - valid_datasets
        
        if datasets_to_remove:
            logger.info(f"Removing state entries for disabled datasets: {datasets_to_remove}")
            
            for dataset in datasets_to_remove:
                delete_query = """
                ALTER TABLE indexing_state
                DELETE WHERE mode = 'historical' AND dataset = %(dataset)s
                """
                self.clickhouse.execute(delete_query, {'dataset': dataset})
                
            logger.info(f"Cleaned up {len(datasets_to_remove)} disabled datasets")
            
    async def _initialize_state_ranges(self) -> int:
        """Initialize state ranges for enabled scrapers only."""
        logger.info("Initializing state ranges for enabled scrapers...")
        
        # Use state manager's bulk create with our scrapers
        created_ranges = self.state_manager.bulk_create_ranges(
            self.scrapers,  # Pass actual scraper instances
            self.start_slot,
            self.end_slot
        )
        
        if created_ranges > 0:
            logger.info(f"Created {created_ranges} new state ranges")
            
        # Log state summary
        await self._log_state_summary()
        
        return created_ranges
        
    async def _log_state_summary(self):
        """Log summary of state entries."""
        query = """
        SELECT 
            dataset,
            countIf(status = 'pending') as pending,
            countIf(status = 'processing') as processing,
            countIf(status = 'completed') as completed,
            countIf(status = 'failed') as failed,
            count() as total
        FROM indexing_state FINAL
        WHERE mode = 'historical'
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
        GROUP BY dataset
        ORDER BY dataset
        """
        
        results = self.clickhouse.execute(query, {
            'start_slot': self.start_slot,
            'end_slot': self.end_slot
        })
        
        logger.info("=== State Summary ===")
        for row in results:
            logger.info(
                f"{row['dataset']}: "
                f"Total: {row['total']}, "
                f"Pending: {row['pending']}, "
                f"Processing: {row['processing']}, "
                f"Completed: {row['completed']}, "
                f"Failed: {row['failed']}"
            )
            
    def _create_scraper_classes_dict(self) -> Dict[str, type]:
        """Create dictionary of scraper classes from instances."""
        scraper_classes = {}
        
        for scraper in self.scrapers:
            scraper_classes[scraper.scraper_id] = type(scraper)
            
        return scraper_classes