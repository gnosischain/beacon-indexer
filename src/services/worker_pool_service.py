"""
Fixed worker pool service with proper event loop management.
This implementation correctly handles asyncio in multiprocessing workers.
"""
import asyncio
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass
from queue import Empty
from typing import Dict, List, Optional, Type, Any

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.utils.logger import logger, setup_logger
from src.scrapers.base_scraper import BaseScraper
from src.utils.specs_manager import SpecsManager


@dataclass
class WorkItem:
    """Represents a unit of work for the pool."""
    scraper_id: str
    start_slot: int
    end_slot: int
    worker_batch_size: int
    priority: int = 0


@dataclass
class WorkResult:
    """Result of processing a work item."""
    scraper_id: str
    start_slot: int
    end_slot: int
    success: bool
    rows_indexed: int = 0
    error_message: Optional[str] = None
    duration: float = 0.0


@dataclass
class WorkerContext:
    """Context for a worker process."""
    worker_id: str
    beacon_api: BeaconAPIService
    clickhouse: ClickHouseService
    state_manager: StateManager
    specs_manager: Optional[SpecsManager]
    scrapers: Dict[str, BaseScraper]


class WorkerPoolService:
    """
    Fixed implementation with proper event loop handling.
    Uses multiprocessing with asyncio for parallel beacon chain scraping.
    """
    
    def __init__(
        self,
        beacon_url: str,
        clickhouse_config: Optional[Dict] = None,
        existing_clickhouse: Optional[ClickHouseService] = None,
        scraper_classes: Dict[str, Type[BaseScraper]] = None,
        specs_manager: Optional[SpecsManager] = None,
        num_workers: int = 4
    ):
        self.beacon_url = beacon_url
        self.clickhouse_config = clickhouse_config
        self.existing_clickhouse = existing_clickhouse
        self.scraper_classes = scraper_classes or {}
        self.specs_manager = specs_manager
        self.num_workers = num_workers
        
        # Multiprocessing components
        self.work_queue = mp.Queue()
        self.result_queue = mp.Queue()
        self.stop_event = mp.Event()
        
        # Worker processes
        self.workers = []
        
        # Statistics
        self.stats = {
            'work_distributed': 0,
            'work_completed': 0,
            'work_failed': 0,
            'total_rows': 0
        }
        
        # Progress tracking
        self.progress_log_interval = 100
        
    async def process_range(self, start_slot: int, end_slot: int):
        """Process a range of slots using the worker pool."""
        logger.info(f"Starting worker pool processing for slots {start_slot}-{end_slot}")
        
        # Start workers
        self._start_workers()
        
        # Start monitoring tasks
        monitor_task = asyncio.create_task(self._monitor_progress())
        distribute_task = asyncio.create_task(self._distribute_work(start_slot, end_slot))
        collect_task = asyncio.create_task(self._collect_results())
        
        try:
            # Wait for distribution to complete
            await distribute_task
            
            # Wait for all work to be processed
            await self._wait_for_completion()
            
            # Stop the workers
            self.stop_event.set()
            
            # Cancel monitoring tasks
            monitor_task.cancel()
            collect_task.cancel()
            
            # Wait for workers to finish
            for worker in self.workers:
                worker.join(timeout=10)
                if worker.is_alive():
                    logger.warning(f"Worker {worker.name} did not stop gracefully")
                    worker.terminate()
                    
        except Exception as e:
            logger.error(f"Error in worker pool processing: {e}")
            self.stop_event.set()
            raise
        finally:
            # Log final statistics
            logger.info(
                f"Worker pool completed: "
                f"{self.stats['work_completed']} successful, "
                f"{self.stats['work_failed']} failed, "
                f"{self.stats['total_rows']} total rows"
            )
            
    def _start_workers(self):
        """Start worker processes."""
        for i in range(self.num_workers):
            worker_id = f"worker-{i+1}"
            worker = mp.Process(
                target=self._worker_process,
                args=(worker_id,),
                name=worker_id
            )
            worker.start()
            self.workers.append(worker)
            logger.info(f"{worker_id} started")
            
    def _worker_process(self, worker_id: str):
        """
        Worker process main loop.
        FIXED: Properly creates and manages its own event loop.
        """
        # Set up worker-specific logger
        worker_logger = setup_logger(worker_id, log_level="INFO")
        
        # Create new event loop for this process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run the async worker loop
            loop.run_until_complete(self._async_worker_loop(worker_id, worker_logger))
        except Exception as e:
            worker_logger.error(f"Fatal error in worker: {e}")
            worker_logger.error(traceback.format_exc())
        finally:
            # Clean up the event loop
            try:
                loop.close()
            except:
                pass
            worker_logger.info(f"{worker_id} shutting down")
            
    async def _async_worker_loop(self, worker_id: str, worker_logger):
        """
        Async worker loop that runs in each worker process.
        """
        # Create worker context with proper async initialization
        context = await self._create_worker_context_async(worker_id)
        
        # Process work items
        while not self.stop_event.is_set():
            try:
                # Get work item with timeout (non-blocking check)
                try:
                    work_item = self.work_queue.get_nowait()
                except Empty:
                    # No work available, wait a bit
                    await asyncio.sleep(0.1)
                    continue
                
                # Process the work item
                result = await self._process_work_item(context, work_item)
                
                # Put result in queue
                self.result_queue.put(result)
                
            except Exception as e:
                worker_logger.error(f"Error processing work item: {e}")
                worker_logger.error(traceback.format_exc())
                # Create error result
                if 'work_item' in locals():
                    error_result = WorkResult(
                        scraper_id=work_item.scraper_id,
                        start_slot=work_item.start_slot,
                        end_slot=work_item.end_slot,
                        success=False,
                        error_message=str(e)
                    )
                    self.result_queue.put(error_result)
                    
        # Clean up resources
        if context.beacon_api:
            await context.beacon_api.stop()
            
    async def _create_worker_context_async(self, worker_id: str) -> WorkerContext:
        """
        Create worker context with proper async initialization.
        FIXED: Properly initializes BeaconAPIService with await.
        """
        # Create new beacon API instance for this worker
        beacon_api = BeaconAPIService(self.beacon_url)
        await beacon_api.start()  # Properly await the start
        
        if self.existing_clickhouse:
            # Use thread-safe client from existing connection
            clickhouse = self.existing_clickhouse.get_threadsafe_client()
        else:
            clickhouse = ClickHouseService(**self.clickhouse_config)
            
        state_manager = StateManager(clickhouse)
        
        # Create scraper instances with correct IDs
        scrapers = {}
        for scraper_id, scraper_class in self.scraper_classes.items():
            scraper = scraper_class(beacon_api, clickhouse)
            # Ensure the scraper has the correct ID
            scraper.scraper_id = scraper_id
            if self.specs_manager:
                scraper.set_specs_manager(self.specs_manager)
            if hasattr(scraper, 'set_state_manager'):
                scraper.set_state_manager(state_manager)
            scrapers[scraper_id] = scraper
            
        return WorkerContext(
            worker_id=worker_id,
            beacon_api=beacon_api,
            clickhouse=clickhouse,
            state_manager=state_manager,
            specs_manager=self.specs_manager,
            scrapers=scrapers
        )
        
    async def _process_work_item(self, context: WorkerContext, work_item: WorkItem) -> WorkResult:
        """Process a single work item."""
        start_time = time.time()
        
        try:
            # Get the scraper
            scraper = context.scrapers.get(work_item.scraper_id)
            if not scraper:
                raise ValueError(f"Unknown scraper ID: {work_item.scraper_id}")
                
            # Process the slot range
            logger.info(
                f"{context.worker_id} processing {work_item.scraper_id} "
                f"slots {work_item.start_slot}-{work_item.end_slot}"
            )
            
            rows_indexed = 0
            
            # Process in batches
            for batch_start in range(work_item.start_slot, work_item.end_slot, work_item.worker_batch_size):
                batch_end = min(batch_start + work_item.worker_batch_size, work_item.end_slot)
                
                # Claim the range for processing
                claimed = context.state_manager.claim_range(
                    mode='historical',  # or work_item.mode if available
                    dataset=work_item.scraper_id,
                    start_slot=batch_start,
                    end_slot=batch_end,
                    worker_id=context.worker_id,
                    batch_id=str(batch_start)
                )
                
                if not claimed:
                    logger.warning(f"Could not claim range {batch_start}-{batch_end}")
                    continue
                
                # Process the batch
                batch_rows = await self._process_batch(
                    context,
                    scraper,
                    batch_start,
                    batch_end
                )
                
                rows_indexed += batch_rows
                
                # Mark batch as completed
                context.state_manager.complete_range(
                    mode='historical',
                    dataset=work_item.scraper_id,
                    start_slot=batch_start,
                    end_slot=batch_end,
                    rows_indexed=batch_rows
                )
                
            # Mark the entire work item as completed
            context.state_manager.complete_range(
                mode='historical',
                dataset=work_item.scraper_id,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                rows_indexed=rows_indexed
            )
            
            duration = time.time() - start_time
            
            return WorkResult(
                scraper_id=work_item.scraper_id,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                success=True,
                rows_indexed=rows_indexed,
                duration=duration
            )
            
        except Exception as e:
            logger.error(f"Error processing work item: {e}")
            logger.error(traceback.format_exc())
            
            # Mark as failed in state manager
            context.state_manager.fail_range(
                mode='historical',
                dataset=work_item.scraper_id,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                error_message=str(e)
            )
            
            return WorkResult(
                scraper_id=work_item.scraper_id,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                success=False,
                error_message=str(e),
                duration=time.time() - start_time
            )
            
    async def _process_batch(
        self,
        context: WorkerContext,
        scraper: BaseScraper,
        start_slot: int,
        end_slot: int
    ) -> int:
        """Process a batch of slots."""
        rows_indexed = 0
        
        # Use BlockProcessor for proper block fetching and processing
        from src.utils.block_processor import BlockProcessor
        block_processor = BlockProcessor(context.beacon_api)
        
        # Create processor function list
        processors = [scraper.process]
        
        # Process slots
        for slot in range(start_slot, end_slot):
            try:
                # Use block processor which handles the block fetching properly
                success = await block_processor.get_and_process_block(slot, processors)
                if success:
                    rows_indexed += 1
                    
            except Exception as e:
                logger.error(f"Error processing slot {slot}: {e}")
                # Continue with next slot
                
        return rows_indexed
        
    async def _distribute_work(self, start_slot: int, end_slot: int):
        """Distribute work items to the queue."""
        logger.info(f"Distributing work for slots {start_slot}-{end_slot}")
        
        # Calculate work items per scraper
        for scraper_id in self.scraper_classes.keys():
            # Create work items for this scraper
            slot_range = end_slot - start_slot
            items_per_scraper = max(1, slot_range // (self.num_workers * 2))
            
            for work_start in range(start_slot, end_slot, items_per_scraper):
                work_end = min(work_start + items_per_scraper, end_slot)
                
                work_item = WorkItem(
                    scraper_id=scraper_id,
                    start_slot=work_start,
                    end_slot=work_end,
                    worker_batch_size=100,
                    priority=self._calculate_priority(work_start)
                )
                
                self.work_queue.put(work_item)
                self.stats['work_distributed'] += 1
                
        logger.info(f"Distributed {self.stats['work_distributed']} work items")
        
    def _calculate_priority(self, start_slot: int) -> int:
        """Calculate priority for work item."""
        # Simple priority: earlier slots have higher priority
        return start_slot
        
    async def _collect_results(self):
        """Collect results from workers."""
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                # Check for results (non-blocking)
                try:
                    result = self.result_queue.get_nowait()
                except Empty:
                    await asyncio.sleep(0.1)
                    continue
                
                # Update statistics
                if result.success:
                    self.stats['work_completed'] += 1
                    self.stats['total_rows'] += result.rows_indexed
                else:
                    self.stats['work_failed'] += 1
                    
                # Log progress if needed
                if (self.stats['work_completed'] + self.stats['work_failed']) % self.progress_log_interval == 0:
                    await self._log_progress()
                    
            except Exception as e:
                logger.error(f"Error collecting results: {e}")
                
    async def _wait_for_completion(self):
        """Wait for all work to be completed."""
        logger.info("Waiting for all work to complete...")
        
        while True:
            # Check if all work is done
            total_processed = self.stats['work_completed'] + self.stats['work_failed']
            
            if total_processed >= self.stats['work_distributed']:
                logger.info("All work items processed")
                break
                
            # Check if workers are still alive
            alive_workers = sum(1 for w in self.workers if w.is_alive())
            if alive_workers == 0:
                logger.error("All workers have died!")
                break
                
            await asyncio.sleep(1)
            
    async def _monitor_progress(self):
        """Monitor and log progress periodically."""
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(30)  # Log every 30 seconds
                await self._log_progress()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in progress monitor: {e}")
                
    async def _log_progress(self):
        """Log current progress."""
        total = self.stats['work_distributed']
        completed = self.stats['work_completed']
        failed = self.stats['work_failed']
        
        if total > 0:
            progress = ((completed + failed) / total) * 100
            logger.info(
                f"Progress: {progress:.1f}% "
                f"({completed} completed, {failed} failed, "
                f"{total - completed - failed} pending), "
                f"Total rows: {self.stats['total_rows']}"
            )