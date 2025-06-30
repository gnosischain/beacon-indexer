"""Enhanced historical operation mode."""
import asyncio
import time
from typing import Dict, Any, List, Optional, Set
from datetime import datetime

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.core.state import StateStatus
from src.utils.logger import logger
from src.utils.block_processor import BlockProcessor
from src.scrapers.validator_scraper import ValidatorScraper
from src.services.worker_pool_service import WorkerPoolService


class HistoricalOperation(OperationMode):
    """Enhanced historical indexing with resume capability."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.start_slot = self.config.get('start_slot', 0)
        self.end_slot = self.config.get('end_slot')
        self.batch_size = self.config.get('batch_size', 1000)
        self.num_workers = self.config.get('num_workers', 1)
        self.force = self.config.get('force', False)
        self.datasets = self.config.get('datasets', [])
        
    def validate_config(self) -> None:
        """Validate historical mode configuration."""
        if self.start_slot < 0:
            raise ValueError("start_slot must be non-negative")
            
        if self.end_slot is not None and self.end_slot <= self.start_slot:
            raise ValueError("end_slot must be greater than start_slot")
            
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
            
        if self.num_workers < 1:
            raise ValueError("num_workers must be at least 1")
            
    async def execute(self) -> Dict[str, Any]:
        """Execute historical indexing."""
        start_time = time.time()
        
        # Get end slot if not specified
        if self.end_slot is None:
            await self._determine_end_slot()
            
        logger.info(
            f"Starting historical indexing: "
            f"slots {self.start_slot}-{self.end_slot}, "
            f"workers={self.num_workers}, batch_size={self.batch_size}, "
            f"force={self.force}"
        )
        
        # Get datasets to process
        datasets_to_process = self._get_datasets_to_process()
        
        if not datasets_to_process:
            logger.warning("No datasets to process")
            return {
                "operation": "historical",
                "duration": 0,
                "ranges_processed": 0,
                "total_rows": 0
            }
            
        # Create initial ranges
        created = await self._create_initial_ranges(datasets_to_process)
        logger.info(f"Created {created} new ranges to process")
        
        # Reset any stale processing jobs
        self.state_manager.reset_stale_jobs()
        
        # Use worker pool for parallel processing
        if self.num_workers > 1:
            result = await self._process_parallel(datasets_to_process)
        else:
            result = await self._process_sequential(datasets_to_process)
            
        result["operation"] = "historical"
        result["duration"] = time.time() - start_time
        result["start_slot"] = self.start_slot
        result["end_slot"] = self.end_slot
        
        return result
        
    async def _determine_end_slot(self) -> None:
        """Determine end slot from chain head."""
        try:
            latest_header = await self.beacon_api.get_block_header("head")
            self.end_slot = int(latest_header["header"]["message"]["slot"])
            logger.info(f"Set end slot to chain head: {self.end_slot}")
        except Exception as e:
            raise RuntimeError(f"Could not determine end slot: {e}")
            
    def _get_datasets_to_process(self) -> List[str]:
        """Get list of datasets to process."""
        if self.datasets:
            # Validate requested datasets
            registry = self.dataset_registry
            valid_datasets = []
            
            for dataset_name in self.datasets:
                dataset = registry.get_dataset(dataset_name)
                if dataset:
                    valid_datasets.append(dataset_name)
                else:
                    logger.warning(f"Unknown dataset: {dataset_name}")
                    
            return valid_datasets
        else:
            # Process only datasets for enabled scrapers
            registry = self.dataset_registry
            valid_datasets = []
            
            # Get scraper IDs from enabled scrapers
            scraper_ids = [scraper.scraper_id for scraper in self.scrapers]
            logger.info(f"Processing datasets for scrapers: {scraper_ids}")
            
            # Get datasets for each enabled scraper
            for scraper_id in scraper_ids:
                datasets = registry.get_datasets_for_scraper(scraper_id)
                for dataset in datasets:
                    if dataset.is_continuous and dataset.name not in valid_datasets:
                        valid_datasets.append(dataset.name)
                        
            logger.info(f"Selected datasets to process: {valid_datasets}")
            return valid_datasets
            
    async def _create_initial_ranges(self, datasets: List[str]) -> int:
        """Create initial ranges for processing using bulk operations."""
        total_created = 0
        
        for dataset_name in datasets:
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            # Special handling for sparse datasets
            if dataset.is_sparse and dataset.scraper_id == "validator_scraper":
                # Create ranges only where target slots exist
                created = await self._create_validator_ranges_bulk(dataset_name)
                total_created += created
            else:
                # Use bulk create for regular ranges
                if hasattr(self.state_manager, 'bulk_create_ranges_for_dataset'):
                    created = self.state_manager.bulk_create_ranges_for_dataset(
                        "historical", dataset_name, self.start_slot, self.end_slot
                    )
                else:
                    # Fall back to old method if bulk method not available
                    created = self.state_manager.create_ranges_for_dataset(
                        "historical", dataset_name, self.start_slot, self.end_slot
                    )
                total_created += created
                
        return total_created
    
    async def _create_validator_ranges_bulk(self, dataset_name: str) -> int:
        """Create ranges for validator dataset with target slots using bulk operations."""
        # Get validator scraper
        validator_scraper = next(
            (s for s in self.scrapers if isinstance(s, ValidatorScraper)), 
            None
        )
        
        if not validator_scraper:
            logger.warning("Validator scraper not found")
            return 0
        
        # Use bulk method if available
        if hasattr(self.state_manager, 'bulk_create_validator_ranges'):
            return await self.state_manager.bulk_create_validator_ranges(
                "historical", dataset_name, self.start_slot, self.end_slot,
                validator_scraper
            )
        else:
            # Fall back to old method
            return await self._create_validator_ranges(dataset_name)
            
    async def _create_validator_ranges(self, dataset_name: str) -> int:
        """Create ranges for validator dataset with target slots (fallback method)."""
        created = 0
        range_size = self.state_manager.range_size
        
        # Get validator scraper
        validator_scraper = next(
            (s for s in self.scrapers if isinstance(s, ValidatorScraper)), 
            None
        )
        
        if not validator_scraper:
            logger.warning("Validator scraper not found")
            return 0
            
        # Process in chunks to avoid memory issues
        current = self.start_slot
        while current < self.end_slot:
            chunk_end = min(current + range_size * 100, self.end_slot)
            
            # Get target slots in this chunk
            target_slots = validator_scraper.get_target_slots_in_range(
                current, chunk_end, self.clickhouse
            )
            
            # Create ranges that contain target slots
            for slot in target_slots:
                range_start = (slot // range_size) * range_size
                range_end = range_start + range_size
                
                # Check if range already exists
                status = self.state_manager.get_range_status(
                    "historical", dataset_name, range_start, range_end
                )
                
                if status is None or (self.force and status != StateStatus.COMPLETED):
                    if self.state_manager.create_range(
                        "historical", dataset_name, range_start, range_end
                    ):
                        created += 1
                        
            current = chunk_end
            
        return created
        
    async def _process_parallel(self, datasets: List[str]) -> Dict[str, Any]:
        """Process using worker pool."""
        # Convert scrapers to class mapping for worker pool
        scraper_classes = {}
        for scraper in self.scrapers:
            scraper_classes[scraper.scraper_id] = type(scraper)
            
        # Create worker pool
        worker_pool = WorkerPoolService(
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
        
        # Set managers
        worker_pool.set_specs_manager(self.scrapers[0].specs_manager)
        worker_pool.set_state_manager(self.state_manager)
        
        # Process range
        await worker_pool.process_range(self.start_slot, self.end_slot)
        
        # Get results
        progress = self.state_manager.get_progress_summary("historical")
        
        total_ranges = sum(p['completed_ranges'] for p in progress.values())
        total_rows = sum(p['total_rows_indexed'] for p in progress.values())
        
        return {
            "ranges_processed": total_ranges,
            "total_rows": total_rows
        }
        
    async def _process_sequential(self, datasets: List[str]) -> Dict[str, Any]:
        """Process sequentially with single worker."""
        block_processor = BlockProcessor(self.beacon_api)
        
        ranges_processed = 0
        total_rows = 0
        
        while True:
            # Get next work item
            work_item = None
            
            for dataset_name in datasets:
                dataset = self.dataset_registry.get_dataset(dataset_name)
                if not dataset:
                    continue
                    
                # Get next pending range
                range_info = self.state_manager.get_next_pending_range(
                    "historical", dataset_name, self.end_slot
                )
                
                if range_info:
                    work_item = (dataset_name, range_info[0], range_info[1])
                    break
                    
            if not work_item:
                logger.info("No more ranges to process")
                break
                
            dataset_name, start_slot, end_slot = work_item
            
            # Claim range
            if not self.state_manager.claim_range(
                "historical", dataset_name, start_slot, end_slot, 
                "sequential-worker", f"batch-{ranges_processed}"
            ):
                continue
                
            # Process range
            try:
                rows = await self._process_single_range(
                    dataset_name, start_slot, end_slot
                )
                
                self.state_manager.complete_range(
                    "historical", dataset_name, start_slot, end_slot, rows
                )
                
                ranges_processed += 1
                total_rows += rows
                
                if ranges_processed % 10 == 0:
                    logger.info(
                        f"Progress: {ranges_processed} ranges, "
                        f"{total_rows} rows indexed"
                    )
                    
            except Exception as e:
                logger.error(f"Error processing range: {e}")
                self.state_manager.fail_range(
                    "historical", dataset_name, start_slot, end_slot, str(e)
                )
                
        return {
            "ranges_processed": ranges_processed,
            "total_rows": total_rows
        }
        
    async def _process_single_range(self, dataset_name: str, 
                                  start_slot: int, end_slot: int) -> int:
        """Process a single range of slots."""
        dataset = self.dataset_registry.get_dataset(dataset_name)
        if not dataset:
            return 0
            
        # Get appropriate scraper
        scraper = next(
            (s for s in self.scrapers if s.scraper_id == dataset.scraper_id),
            None
        )
        
        if not scraper:
            logger.error(f"Scraper {dataset.scraper_id} not found")
            return 0
            
        # Process based on dataset type
        if dataset.is_sparse and isinstance(scraper, ValidatorScraper):
            # Get target slots in range
            target_slots = scraper.get_target_slots_in_range(
                start_slot, end_slot, self.clickhouse
            )
            
            if not target_slots:
                return 0
                
            # Process only target slots
            rows = 0
            for slot in target_slots:
                try:
                    block = await self.beacon_api.get_block(slot)
                    if block:
                        await scraper.process(block)
                        rows += 1
                except Exception as e:
                    logger.error(f"Error processing slot {slot}: {e}")
                    
            return rows
        else:
            # Process all slots in range
            return await self._process_slot_range(
                scraper, start_slot, end_slot
            )
            
    async def _process_slot_range(self, scraper, start_slot: int, 
                                 end_slot: int) -> int:
        """Process a range of slots with a scraper."""
        block_processor = BlockProcessor(self.beacon_api)
        
        rows = 0
        current = start_slot
        
        while current < end_slot:
            batch_end = min(current + self.batch_size, end_slot)
            
            # Process batch
            blocks = await block_processor.get_blocks_range(
                current, batch_end
            )
            
            for block in blocks:
                if block:
                    await scraper.process(block)
                    rows += 1
                    
            current = batch_end
            
        return rows