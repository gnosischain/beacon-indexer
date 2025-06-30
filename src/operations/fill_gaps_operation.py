"""Fill gaps operation implementation."""
import asyncio
import time
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.core.state import StateStatus
from src.utils.logger import logger
from src.services.worker_pool_service import WorkerPoolService


class FillGapsOperation(OperationMode):
    """Operation to find and fill gaps in indexed data."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.start_slot = self.config.get('start_slot', 0)
        self.end_slot = self.config.get('end_slot')
        self.datasets = self.config.get('datasets', [])
        self.num_workers = self.config.get('num_workers', 1)
        self.handle_failed = self.config.get('handle_failed_ranges', True)
        self.max_attempts = self.config.get('max_retries', 3)
        self.min_gap_size = self.config.get('min_gap_size', 1)
        
    def validate_config(self) -> None:
        """Validate configuration."""
        if self.start_slot < 0:
            raise ValueError("start_slot must be non-negative")
            
        if self.end_slot is not None and self.end_slot <= self.start_slot:
            raise ValueError("end_slot must be greater than start_slot")
            
        if self.num_workers < 1:
            raise ValueError("num_workers must be at least 1")
            
        if self.max_attempts < 1:
            raise ValueError("max_retries must be at least 1")
            
    async def execute(self) -> Dict[str, Any]:
        """Execute gap filling operation."""
        start_time = time.time()
        
        # Determine end slot if not specified
        if self.end_slot is None:
            await self._determine_end_slot()
            
        logger.info(
            f"Starting gap detection and filling: "
            f"slots {self.start_slot}-{self.end_slot}, "
            f"handle_failed={self.handle_failed}"
        )
        
        # Get datasets to check
        datasets_to_check = self._get_datasets_to_check()
        
        # Find all gaps
        all_gaps = await self._find_all_gaps(datasets_to_check)
        
        # Find failed ranges if requested
        failed_ranges = {}
        if self.handle_failed:
            failed_ranges = await self._find_failed_ranges(datasets_to_check)
            
        # Log summary
        total_gaps = sum(len(gaps) for gaps in all_gaps.values())
        total_failed = sum(len(ranges) for ranges in failed_ranges.values())
        
        logger.info(
            f"Found {total_gaps} gaps and {total_failed} failed ranges "
            f"across {len(datasets_to_check)} datasets"
        )
        
        if total_gaps == 0 and total_failed == 0:
            return {
                "operation": "fill_gaps",
                "duration": time.time() - start_time,
                "gaps_found": 0,
                "gaps_filled": 0,
                "failed_ranges_processed": 0,
                "total_rows": 0
            }
            
        # Create ranges for gaps
        created_ranges = await self._create_gap_ranges(all_gaps)
        
        # Process all ranges (gaps + failed)
        if self.num_workers > 1:
            result = await self._process_parallel()
        else:
            result = await self._process_sequential(all_gaps, failed_ranges)
            
        result["operation"] = "fill_gaps"
        result["duration"] = time.time() - start_time
        result["gaps_found"] = total_gaps
        result["failed_ranges_found"] = total_failed
        
        return result
        
    async def _determine_end_slot(self) -> None:
        """Determine end slot from existing data."""
        # Use the highest slot from completed ranges
        progress = self.state_manager.get_progress_summary("historical")
        
        if progress:
            max_slot = max(p.get('highest_slot', 0) for p in progress.values())
            if max_slot > 0:
                self.end_slot = max_slot
                logger.info(f"Set end slot to highest indexed: {self.end_slot}")
                return
                
        # Fall back to chain head
        try:
            latest_header = await self.beacon_api.get_block_header("head")
            self.end_slot = int(latest_header["header"]["message"]["slot"])
            logger.info(f"Set end slot to chain head: {self.end_slot}")
        except Exception as e:
            raise RuntimeError(f"Could not determine end slot: {e}")
            
    def _get_datasets_to_check(self) -> List[str]:
        """Get list of datasets to check for gaps."""
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
            # Check all continuous datasets
            datasets = self.dataset_registry.get_all_datasets()
            return [d.name for d in datasets if d.is_continuous]
            
    async def _find_all_gaps(self, datasets: List[str]) -> Dict[str, List[Tuple[int, int]]]:
        """Find gaps in all datasets."""
        all_gaps = {}
        
        for dataset_name in datasets:
            logger.info(f"Checking gaps in {dataset_name}...")
            
            gaps = self.state_manager.find_gaps(
                "historical", dataset_name, 
                self.start_slot, self.end_slot,
                self.min_gap_size
            )
            
            if gaps:
                all_gaps[dataset_name] = gaps
                logger.info(f"Found {len(gaps)} gaps in {dataset_name}")
                
                # Log first few gaps
                for i, (start, end) in enumerate(gaps[:5]):
                    logger.info(f"  Gap {i+1}: slots {start}-{end} ({end-start} slots)")
                    
                if len(gaps) > 5:
                    logger.info(f"  ... and {len(gaps)-5} more gaps")
                    
        return all_gaps
        
    async def _find_failed_ranges(self, datasets: List[str]) -> Dict[str, List[Tuple[int, int, int, str]]]:
        """Find failed ranges that can be retried."""
        failed_ranges = {}
        
        for dataset_name in datasets:
            ranges = self.state_manager.get_failed_ranges(
                "historical", dataset_name,
                self.start_slot, self.end_slot,
                self.max_attempts
            )
            
            if ranges:
                failed_ranges[dataset_name] = ranges
                logger.info(
                    f"Found {len(ranges)} failed ranges in {dataset_name} "
                    f"with < {self.max_attempts} attempts"
                )
                
        return failed_ranges
        
    async def _create_gap_ranges(self, gaps: Dict[str, List[Tuple[int, int]]]) -> int:
        """Create pending ranges for all gaps."""
        total_created = 0
        
        for dataset_name, dataset_gaps in gaps.items():
            for start_slot, end_slot in dataset_gaps:
                # Create range entry
                if self.state_manager.create_range(
                    "historical", dataset_name, start_slot, end_slot,
                    batch_id="gap_fill"
                ):
                    total_created += 1
                    
        logger.info(f"Created {total_created} ranges for gap filling")
        return total_created
        
    async def _process_parallel(self) -> Dict[str, Any]:
        """Process gaps using worker pool."""
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
        
        # Process range (workers will pick up gap ranges)
        await worker_pool.process_range(self.start_slot, self.end_slot)
        
        # Get results
        progress = self.state_manager.get_progress_summary("historical")
        
        gaps_filled = 0
        failed_processed = 0
        total_rows = 0
        
        for dataset_name, stats in progress.items():
            # Count gap fills (ranges with batch_id='gap_fill' that are completed)
            gap_count = self.state_manager.count_completed_by_batch(
                "historical", dataset_name, "gap_fill"
            )
            gaps_filled += gap_count
            
            total_rows += stats['total_rows_indexed']
            
        return {
            "gaps_filled": gaps_filled,
            "failed_ranges_processed": failed_processed,
            "total_rows": total_rows
        }
        
    async def _process_sequential(
        self, 
        gaps: Dict[str, List[Tuple[int, int]]],
        failed_ranges: Dict[str, List[Tuple[int, int, int, str]]]
    ) -> Dict[str, Any]:
        """Process gaps and failed ranges sequentially."""
        gaps_filled = 0
        failed_processed = 0
        total_rows = 0
        
        # Process gaps first
        for dataset_name, dataset_gaps in gaps.items():
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            for start_slot, end_slot in dataset_gaps:
                rows = await self._process_single_gap(dataset_name, start_slot, end_slot)
                if rows >= 0:
                    gaps_filled += 1
                    total_rows += rows
                    
        # Process failed ranges
        if self.handle_failed:
            for dataset_name, ranges in failed_ranges.items():
                for start_slot, end_slot, attempts, error in ranges:
                    logger.info(
                        f"Retrying failed range {dataset_name} {start_slot}-{end_slot} "
                        f"(attempt {attempts + 1}/{self.max_attempts})"
                    )
                    
                    rows = await self._process_single_gap(
                        dataset_name, start_slot, end_slot, force=True
                    )
                    
                    if rows >= 0:
                        failed_processed += 1
                        total_rows += rows
                        
        return {
            "gaps_filled": gaps_filled,
            "failed_ranges_processed": failed_processed,
            "total_rows": total_rows
        }
        
    async def _process_single_gap(
        self, 
        dataset_name: str, 
        start_slot: int, 
        end_slot: int,
        force: bool = False
    ) -> int:
        """Process a single gap."""
        try:
            # Use historical operation's range processor
            from .historical_operation import HistoricalOperation
            
            # Create a temporary historical operation
            hist_op = HistoricalOperation(
                self.beacon_api,
                self.clickhouse,
                self.state_manager,
                self.scrapers,
                {'force': force}
            )
            
            return await hist_op._process_single_range(dataset_name, start_slot, end_slot)
            
        except Exception as e:
            logger.error(f"Error processing gap {dataset_name} {start_slot}-{end_slot}: {e}")
            return -1