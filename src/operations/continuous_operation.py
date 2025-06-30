"""Enhanced continuous operation mode."""
import asyncio
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.core.state import StateStatus
from src.utils.logger import logger
from src.utils.block_processor import BlockProcessor
from src.scrapers.validator_scraper import ValidatorScraper


class ContinuousOperation(OperationMode):
    """Enhanced continuous indexing following chain tip."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.confirmation_blocks = self.config.get('confirmation_blocks', 12)
        self.poll_interval = self.config.get('poll_interval', 10)
        self.running = False
        self.last_processed_slot = 0
        self.checkpoint_interval = 1000  # Create checkpoint every N slots
        self.last_checkpoint_slot = 0
        
    def validate_config(self) -> None:
        """Validate continuous mode configuration."""
        if self.confirmation_blocks < 0:
            raise ValueError("confirmation_blocks must be non-negative")
        if self.poll_interval < 1:
            raise ValueError("poll_interval must be at least 1 second")
            
    async def execute(self) -> Dict[str, Any]:
        """Execute continuous indexing."""
        self.running = True
        start_time = time.time()
        slots_processed = 0
        errors = 0
        
        try:
            # Initialize from last checkpoint
            await self._initialize_from_checkpoint()
            
            # Start monitoring tasks
            gap_monitor = asyncio.create_task(self._monitor_gaps())
            health_monitor = asyncio.create_task(self._monitor_health())
            
            logger.info(f"Starting continuous indexing from slot {self.last_processed_slot + 1}")
            
            while self.running:
                try:
                    processed = await self._process_new_slots()
                    slots_processed += processed
                    
                    # Create checkpoint periodically
                    if self.last_processed_slot - self.last_checkpoint_slot >= self.checkpoint_interval:
                        await self._create_checkpoint()
                        
                except Exception as e:
                    logger.error(f"Error in continuous processing: {e}")
                    errors += 1
                    await asyncio.sleep(self.poll_interval * 2)  # Back off on error
                    
                await asyncio.sleep(self.poll_interval)
                
        finally:
            # Cancel monitoring tasks
            gap_monitor.cancel()
            health_monitor.cancel()
            
            # Final checkpoint
            await self._create_checkpoint()
            
        return {
            "operation": "continuous",
            "duration": time.time() - start_time,
            "slots_processed": slots_processed,
            "last_slot": self.last_processed_slot,
            "errors": errors
        }
        
    async def _initialize_from_checkpoint(self) -> None:
        """Initialize state from last checkpoint."""
        # Get last synced positions for all datasets
        datasets = self.dataset_registry.get_all_datasets()
        dataset_names = [d.name for d in datasets if d.is_continuous]
        
        last_slot = self.state_manager.get_last_synced_slot("continuous", dataset_names)
        
        if last_slot > 0:
            self.last_processed_slot = last_slot
            self.last_checkpoint_slot = last_slot
            logger.info(f"Resuming from checkpoint at slot {last_slot}")
        else:
            # No checkpoint, start from latest completed ranges
            progress = self.state_manager.get_progress_summary("continuous")
            if progress:
                min_slot = min(p.get('highest_slot', 0) for p in progress.values())
                self.last_processed_slot = min_slot
                
    async def _process_new_slots(self) -> int:
        """Process new slots from the chain."""
        # Get chain head
        latest_header = await self.beacon_api.get_block_header("head")
        head_slot = int(latest_header["header"]["message"]["slot"])
        
        # Apply confirmation blocks
        target_slot = head_slot - self.confirmation_blocks
        
        if target_slot <= self.last_processed_slot:
            logger.debug(f"No new confirmed slots. Head: {head_slot}, Target: {target_slot}, Last: {self.last_processed_slot}")
            return 0
            
        # Process slots in ranges
        range_size = self.state_manager.range_size
        slots_processed = 0
        
        current = self.last_processed_slot + 1
        while current <= target_slot:
            range_end = min(current + range_size, target_slot + 1)
            
            # Process this range for all datasets
            success = await self._process_range(current, range_end)
            
            if success:
                slots_processed += (range_end - current)
                self.last_processed_slot = range_end - 1
                current = range_end
            else:
                # Failed to process range, retry next iteration
                break
                
        return slots_processed
        
    async def _process_range(self, start_slot: int, end_slot: int) -> bool:
        """Process a range of slots for all datasets."""
        try:
            # Get datasets in priority order
            datasets = self.dataset_registry.get_datasets_by_priority()
            
            # Group by scraper for efficiency
            scraper_ranges = {}
            for dataset in datasets:
                if not dataset.is_continuous:
                    continue
                    
                # Check if already processed
                status = self.state_manager.get_range_status(
                    "continuous", dataset.name, start_slot, end_slot
                )
                
                if status == StateStatus.COMPLETED:
                    continue
                    
                # Special handling for validator dataset
                if dataset.is_sparse and dataset.scraper_id == "validator_scraper":
                    # Check if this range contains any target slots
                    validator_scraper = next(
                        (s for s in self.scrapers if isinstance(s, ValidatorScraper)), 
                        None
                    )
                    if validator_scraper:
                        target_slots = validator_scraper.get_target_slots_in_range(
                            start_slot, end_slot, self.clickhouse
                        )
                        if not target_slots:
                            # No target slots, mark as completed with 0 rows
                            self.state_manager.complete_range(
                                "continuous", dataset.name, start_slot, end_slot, 0
                            )
                            continue
                            
                # Claim range
                if not self.state_manager.claim_range(
                    "continuous", dataset.name, start_slot, end_slot,
                    "continuous_worker", "continuous_batch"
                ):
                    logger.warning(f"Could not claim range for {dataset.name}")
                    continue
                    
                # Add to scraper work
                if dataset.scraper_id not in scraper_ranges:
                    scraper_ranges[dataset.scraper_id] = []
                scraper_ranges[dataset.scraper_id].append(dataset)
                
            # Process by scraper
            block_processor = BlockProcessor(self.beacon_api)
            
            for slot in range(start_slot, end_slot):
                # Get scrapers that need this slot
                slot_scrapers = []
                for scraper in self.scrapers:
                    scraper_datasets = scraper_ranges.get(scraper.scraper_id, [])
                    if scraper_datasets:
                        # Check if scraper should process this slot
                        if isinstance(scraper, ValidatorScraper):
                            if scraper.should_process_slot(slot):
                                slot_scrapers.append(scraper)
                        else:
                            slot_scrapers.append(scraper)
                            
                if slot_scrapers:
                    # Reset row counts
                    for scraper in slot_scrapers:
                        scraper.reset_row_counts()
                        
                    # Process slot
                    processors = [s.process for s in slot_scrapers]
                    success = await block_processor.get_and_process_block(slot, processors)
                    
                    if not success:
                        logger.debug(f"No block at slot {slot}")
                        
            # Complete ranges for all datasets
            for scraper_id, datasets in scraper_ranges.items():
                scraper = next((s for s in self.scrapers if s.scraper_id == scraper_id), None)
                if scraper:
                    row_counts = scraper.get_row_counts()
                    for dataset in datasets:
                        rows = sum(row_counts.get(table, 0) for table in dataset.tables)
                        self.state_manager.complete_range(
                            "continuous", dataset.name, start_slot, end_slot, rows
                        )
                        
            # Update sync positions
            for dataset in datasets:
                if dataset.is_continuous:
                    self.state_manager.update_sync_position(
                        "continuous", dataset.name, end_slot - 1
                    )
                    
            return True
            
        except Exception as e:
            logger.error(f"Error processing range {start_slot}-{end_slot}: {e}")
            
            # Mark failed for claimed datasets
            for scraper_id, datasets in scraper_ranges.items():
                for dataset in datasets:
                    self.state_manager.fail_range(
                        "continuous", dataset.name, start_slot, end_slot, str(e)
                    )
                    
            return False
            
    async def _create_checkpoint(self) -> None:
        """Create a checkpoint of current state."""
        try:
            # Update sync positions for all datasets
            datasets = self.dataset_registry.get_all_datasets()
            for dataset in datasets:
                if dataset.is_continuous:
                    self.state_manager.update_sync_position(
                        "continuous", dataset.name, self.last_processed_slot
                    )
                    
            self.last_checkpoint_slot = self.last_processed_slot
            logger.info(f"Created checkpoint at slot {self.last_processed_slot}")
            
        except Exception as e:
            logger.error(f"Error creating checkpoint: {e}")
            
    async def _monitor_gaps(self) -> None:
        """Monitor and fill gaps in continuous mode."""
        while self.running:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                # Find gaps in recent data
                datasets = self.dataset_registry.get_all_datasets()
                lookback_slots = 10000  # Look back ~1.4 hours
                
                start_slot = max(0, self.last_processed_slot - lookback_slots)
                end_slot = self.last_processed_slot
                
                for dataset in datasets:
                    if not dataset.is_continuous:
                        continue
                        
                    gaps = self.state_manager.find_gaps(
                        "continuous", dataset.name, start_slot, end_slot
                    )
                    
                    if gaps:
                        logger.warning(f"Found {len(gaps)} gaps in {dataset.name}")
                        # Gaps will be filled in next iteration
                        
            except Exception as e:
                logger.error(f"Error in gap monitoring: {e}")
                
    async def _monitor_health(self) -> None:
        """Monitor system health."""
        while self.running:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Check for stale jobs
                stale_count = self.state_manager.reset_stale_jobs()
                if stale_count > 0:
                    logger.info(f"Reset {stale_count} stale jobs")
                    
                # Log progress
                progress = self.state_manager.get_progress_summary("continuous")
                for dataset, stats in progress.items():
                    if stats['processing_ranges'] > 0:
                        logger.info(
                            f"{dataset}: {stats['completed_ranges']} completed, "
                            f"{stats['processing_ranges']} processing"
                        )
                        
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")