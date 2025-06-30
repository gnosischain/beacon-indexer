"""Process failed ranges operation."""
import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.utils.logger import logger
from src.services.worker_pool_service import WorkerPoolService


class ProcessFailedOperation(OperationMode):
    """Operation to process only failed ranges."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.datasets = self.config.get('datasets', [])
        self.max_attempts = self.config.get('max_retries', 3)
        self.num_workers = self.config.get('num_workers', 1)
        self.delete_before_retry = self.config.get('delete_before_retry', True)
        
    def validate_config(self) -> None:
        """Validate configuration."""
        if self.max_attempts < 1:
            raise ValueError("max_retries must be at least 1")
            
        if self.num_workers < 1:
            raise ValueError("num_workers must be at least 1")
            
    async def execute(self) -> Dict[str, Any]:
        """Execute failed range processing."""
        start_time = time.time()
        
        logger.info(
            f"Starting failed range processing: "
            f"max_attempts={self.max_attempts}, "
            f"delete_before_retry={self.delete_before_retry}"
        )
        
        # Get datasets to process
        datasets_to_process = self._get_datasets_to_process()
        
        # Find all failed ranges
        failed_ranges = await self._find_all_failed_ranges(datasets_to_process)
        
        total_failed = sum(len(ranges) for ranges in failed_ranges.values())
        
        if total_failed == 0:
            logger.info("No failed ranges found to process")
            return {
                "operation": "process_failed",
                "duration": time.time() - start_time,
                "failed_ranges_found": 0,
                "ranges_processed": 0,
                "ranges_succeeded": 0,
                "ranges_failed_again": 0
            }
            
        logger.info(f"Found {total_failed} failed ranges across {len(failed_ranges)} datasets")
        
        # Delete existing data if configured
        if self.delete_before_retry:
            await self._delete_failed_data(failed_ranges)
            
        # Process failed ranges
        if self.num_workers > 1:
            result = await self._process_parallel(failed_ranges)
        else:
            result = await self._process_sequential(failed_ranges)
            
        result["operation"] = "process_failed"
        result["duration"] = time.time() - start_time
        result["failed_ranges_found"] = total_failed
        
        return result
        
    def _get_datasets_to_process(self) -> List[str]:
        """Get list of datasets to process."""
        if self.datasets:
            return self.datasets
        else:
            # Process all continuous datasets
            datasets = self.dataset_registry.get_all_datasets()
            return [d.name for d in datasets if d.is_continuous]
            
    async def _find_all_failed_ranges(
        self, 
        datasets: List[str]
    ) -> Dict[str, List[Tuple[int, int, int, str]]]:
        """Find all failed ranges for datasets."""
        failed_ranges = {}
        
        for dataset_name in datasets:
            ranges = self.state_manager.get_failed_ranges(
                "historical", dataset_name,
                0, 999999999,  # All slots
                self.max_attempts
            )
            
            if ranges:
                failed_ranges[dataset_name] = ranges
                logger.info(
                    f"Found {len(ranges)} failed ranges in {dataset_name} "
                    f"with < {self.max_attempts} attempts"
                )
                
                # Log some examples
                for i, (start, end, attempts, error) in enumerate(ranges[:3]):
                    logger.info(
                        f"  {dataset_name} {start}-{end}: "
                        f"attempts={attempts}, error={error[:50]}..."
                    )
                    
                if len(ranges) > 3:
                    logger.info(f"  ... and {len(ranges)-3} more")
                    
        return failed_ranges
        
    async def _delete_failed_data(self, failed_ranges: Dict[str, List]) -> None:
        """Delete existing data for failed ranges before retry."""
        logger.info("Deleting existing data for failed ranges...")
        
        for dataset_name, ranges in failed_ranges.items():
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            for start_slot, end_slot, _, _ in ranges:
                # Delete from each table
                for table in dataset.tables:
                    try:
                        delete_query = f"""
                        ALTER TABLE {self.clickhouse.database}.{table}
                        DELETE WHERE slot >= {start_slot} AND slot < {end_slot}
                        """
                        
                        self.clickhouse.execute(delete_query)
                        
                    except Exception as e:
                        logger.error(
                            f"Error deleting data from {table} for "
                            f"{start_slot}-{end_slot}: {e}"
                        )
                        
        # Wait for deletions to propagate
        await asyncio.sleep(2)
        
    async def _process_parallel(self, failed_ranges: Dict[str, List]) -> Dict[str, Any]:
        """Process failed ranges using worker pool."""
        # Reset failed ranges to pending
        for dataset_name, ranges in failed_ranges.items():
            for start_slot, end_slot, attempts, _ in ranges:
                # Mark as pending for workers to pick up
                self.state_manager.reset_failed_range(
                    "historical", dataset_name, start_slot, end_slot
                )
                
        # Get min and max slots
        all_slots = []
        for ranges in failed_ranges.values():
            for start, end, _, _ in ranges:
                all_slots.extend([start, end])
                
        min_slot = min(all_slots) if all_slots else 0
        max_slot = max(all_slots) if all_slots else 0
        
        # Convert scrapers to class mapping
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
        await worker_pool.process_range(min_slot, max_slot)
        
        # Count results
        ranges_succeeded = 0
        ranges_failed_again = 0
        
        for dataset_name, ranges in failed_ranges.items():
            for start_slot, end_slot, _, _ in ranges:
                status = self.state_manager.get_range_status(
                    "historical", dataset_name, start_slot, end_slot
                )
                
                if status == 'completed':
                    ranges_succeeded += 1
                elif status == 'failed':
                    ranges_failed_again += 1
                    
        return {
            "ranges_processed": len(all_slots) // 2,
            "ranges_succeeded": ranges_succeeded,
            "ranges_failed_again": ranges_failed_again
        }
        
    async def _process_sequential(self, failed_ranges: Dict[str, List]) -> Dict[str, Any]:
        """Process failed ranges sequentially."""
        ranges_processed = 0
        ranges_succeeded = 0
        ranges_failed_again = 0
        
        for dataset_name, ranges in failed_ranges.items():
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            for start_slot, end_slot, attempts, error in ranges:
                logger.info(
                    f"Retrying {dataset_name} {start_slot}-{end_slot} "
                    f"(attempt {attempts + 1}/{self.max_attempts})"
                )
                
                # Reset to pending
                self.state_manager.reset_failed_range(
                    "historical", dataset_name, start_slot, end_slot
                )
                
                # Process using historical operation
                from .historical_operation import HistoricalOperation
                
                hist_op = HistoricalOperation(
                    self.beacon_api,
                    self.clickhouse,
                    self.state_manager,
                    self.scrapers,
                    {'force': True}
                )
                
                rows = await hist_op._process_single_range(
                    dataset_name, start_slot, end_slot
                )
                
                ranges_processed += 1
                
                if rows >= 0:
                    ranges_succeeded += 1
                    logger.info(
                        f"Successfully processed {dataset_name} {start_slot}-{end_slot} "
                        f"on retry ({rows} rows)"
                    )
                else:
                    ranges_failed_again += 1
                    logger.error(
                        f"Failed again: {dataset_name} {start_slot}-{end_slot}"
                    )
                    
        return {
            "ranges_processed": ranges_processed,
            "ranges_succeeded": ranges_succeeded,
            "ranges_failed_again": ranges_failed_again
        }