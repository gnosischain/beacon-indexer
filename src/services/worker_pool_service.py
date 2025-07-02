"""
Enhanced worker pool service with fair distribution across scrapers.
This implementation ensures all enabled scrapers get workers assigned.
"""
import asyncio
import multiprocessing as mp
import time
import traceback
from dataclasses import dataclass, field
from queue import Empty, PriorityQueue
from typing import Dict, List, Optional, Type, Any, Set
import heapq
from collections import defaultdict

from src.config import config
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.utils.logger import logger, setup_logger
from src.scrapers.base_scraper import BaseScraper
from src.utils.specs_manager import SpecsManager


@dataclass
class WorkItem:
    """Enhanced work item with scraper assignment."""
    scraper_id: str
    dataset: str  # Add dataset tracking
    start_slot: int
    end_slot: int
    worker_batch_size: int
    priority: int = 0
    assigned_worker: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    
    def __lt__(self, other):
        # Higher priority = processed first
        # Within same priority, older items processed first
        if self.priority != other.priority:
            return self.priority > other.priority
        return self.created_at < other.created_at


@dataclass
class WorkResult:
    """Result of processing a work item."""
    scraper_id: str
    dataset: str
    start_slot: int
    end_slot: int
    success: bool
    rows_indexed: int = 0
    error_message: Optional[str] = None
    duration: float = 0.0
    worker_id: Optional[str] = None


@dataclass
class ScraperWorkQueue:
    """Dedicated work queue for a scraper."""
    scraper_id: str
    queue: PriorityQueue = field(default_factory=PriorityQueue)
    assigned_workers: Set[str] = field(default_factory=set)
    completed_items: int = 0
    failed_items: int = 0
    total_rows: int = 0
    

class WorkerPoolService:
    """
    Enhanced implementation with fair scraper distribution.
    """
    
    def __init__(
        self,
        beacon_url: str,
        clickhouse_config: Optional[Dict] = None,
        existing_clickhouse: Optional[ClickHouseService] = None,
        scraper_classes: Dict[str, Type[BaseScraper]] = None,
        specs_manager: Optional[SpecsManager] = None,
        num_workers: int = 4,
        enabled_scrapers: List[str] = None  # Add enabled scrapers
    ):
        self.beacon_url = beacon_url
        self.clickhouse_config = clickhouse_config
        self.existing_clickhouse = existing_clickhouse
        self.scraper_classes = scraper_classes or {}
        self.specs_manager = specs_manager
        self.num_workers = num_workers
        self.enabled_scrapers = enabled_scrapers or []
        
        # Filter scraper classes to only enabled ones
        if self.enabled_scrapers:
            self.scraper_classes = {
                sid: cls for sid, cls in self.scraper_classes.items()
                if any(sid.startswith(es) or es in sid for es in self.enabled_scrapers)
            }
            logger.info(f"Filtered to {len(self.scraper_classes)} enabled scrapers: {list(self.scraper_classes.keys())}")
        
        # Create per-scraper work queues
        self.scraper_queues: Dict[str, ScraperWorkQueue] = {}
        for scraper_id in self.scraper_classes:
            self.scraper_queues[scraper_id] = ScraperWorkQueue(scraper_id)
        
        # Global result queue
        self.result_queue = mp.Queue()
        self.stop_event = mp.Event()
        
        # Worker processes
        self.workers = []
        self.worker_assignments: Dict[str, str] = {}  # worker_id -> scraper_id
        
        # Statistics
        self.stats = defaultdict(lambda: {
            'work_distributed': 0,
            'work_completed': 0,
            'work_failed': 0,
            'total_rows': 0,
            'active_workers': set()
        })
        
        # Work distribution strategy
        self.distribution_strategy = 'round_robin'  # or 'load_balanced'
        
    def _calculate_workers_per_scraper(self) -> Dict[str, int]:
        """Calculate how many workers each scraper should get."""
        num_scrapers = len(self.scraper_classes)
        if num_scrapers == 0:
            return {}
            
        # Equal distribution with remainder handling
        base_workers = self.num_workers // num_scrapers
        remainder = self.num_workers % num_scrapers
        
        workers_per_scraper = {}
        for i, scraper_id in enumerate(self.scraper_classes.keys()):
            # Give extra worker to first scrapers if there's remainder
            workers_per_scraper[scraper_id] = base_workers + (1 if i < remainder else 0)
            
        logger.info(f"Worker distribution: {workers_per_scraper}")
        return workers_per_scraper
        
    async def process_range(self, start_slot: int, end_slot: int):
        """Process a range with fair distribution across scrapers."""
        logger.info(f"Starting worker pool for slots {start_slot}-{end_slot}")
        logger.info(f"Active scrapers: {list(self.scraper_classes.keys())}")
        
        # Calculate worker distribution
        workers_per_scraper = self._calculate_workers_per_scraper()
        
        # Start workers with scraper assignments
        self._start_workers_with_assignments(workers_per_scraper)
        
        # Start monitoring tasks
        monitor_task = asyncio.create_task(self._monitor_progress())
        distribute_task = asyncio.create_task(
            self._distribute_work_fairly(start_slot, end_slot)
        )
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
            # Log final statistics per scraper
            self._log_final_statistics()
            
    def _start_workers_with_assignments(self, workers_per_scraper: Dict[str, int]):
        """Start workers with pre-assigned scrapers."""
        worker_id_counter = 0
        
        for scraper_id, num_workers in workers_per_scraper.items():
            for _ in range(num_workers):
                worker_id = f"worker-{worker_id_counter}"
                worker_id_counter += 1
                
                # Assign worker to scraper
                self.worker_assignments[worker_id] = scraper_id
                self.scraper_queues[scraper_id].assigned_workers.add(worker_id)
                
                # Start worker process
                worker = mp.Process(
                    target=self._worker_process_with_assignment,
                    args=(worker_id, scraper_id),
                    name=worker_id
                )
                worker.start()
                self.workers.append(worker)
                
                logger.info(f"{worker_id} started and assigned to {scraper_id}")
                
    def _worker_process_with_assignment(self, worker_id: str, assigned_scraper: str):
        """Worker process that handles specific scraper."""
        try:
            # Create new event loop for this process
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Run the async worker
            loop.run_until_complete(
                self._async_worker_with_assignment(worker_id, assigned_scraper)
            )
        except Exception as e:
            logger.error(f"{worker_id} fatal error: {e}")
            logger.error(traceback.format_exc())
        finally:
            loop.close()
            
    async def _async_worker_with_assignment(self, worker_id: str, assigned_scraper: str):
        """Async worker that processes work for assigned scraper."""
        worker_logger = setup_logger(worker_id, log_level=config.scraper.log_level)
        worker_logger.info(f"Worker started for scraper: {assigned_scraper}")
        
        # Create worker context
        context = await self._create_worker_context(worker_id)
        if not context:
            worker_logger.error("Failed to create worker context")
            return
            
        # Get the scraper instance for this worker
        scraper_class = self.scraper_classes.get(assigned_scraper)
        if not scraper_class:
            worker_logger.error(f"Scraper class not found: {assigned_scraper}")
            return
            
        scraper = scraper_class(context.beacon_api, context.clickhouse)
        scraper.scraper_id = assigned_scraper
        
        if self.specs_manager:
            scraper.set_specs_manager(self.specs_manager)
        if hasattr(scraper, 'set_state_manager'):
            scraper.set_state_manager(context.state_manager)
            
        # Process work items for assigned scraper
        while not self.stop_event.is_set():
            work_item = self._get_work_for_scraper(assigned_scraper)
            
            if work_item is None:
                await asyncio.sleep(0.1)
                continue
                
            start_time = time.time()
            try:
                worker_logger.info(
                    f"Processing {work_item.dataset} "
                    f"slots {work_item.start_slot}-{work_item.end_slot}"
                )
                
                # Process the work item
                rows_indexed = await self._process_work_item(
                    context, scraper, work_item
                )
                
                # Report success
                result = WorkResult(
                    scraper_id=assigned_scraper,
                    dataset=work_item.dataset,
                    start_slot=work_item.start_slot,
                    end_slot=work_item.end_slot,
                    success=True,
                    rows_indexed=rows_indexed,
                    duration=time.time() - start_time,
                    worker_id=worker_id
                )
                
                self.result_queue.put(result)
                worker_logger.info(
                    f"Completed {work_item.dataset}: {rows_indexed} rows in {result.duration:.2f}s"
                )
                
            except Exception as e:
                worker_logger.error(f"Error processing work item: {e}")
                worker_logger.error(traceback.format_exc())
                
                # Report failure
                result = WorkResult(
                    scraper_id=assigned_scraper,
                    dataset=work_item.dataset,
                    start_slot=work_item.start_slot,
                    end_slot=work_item.end_slot,
                    success=False,
                    error_message=str(e),
                    duration=time.time() - start_time,
                    worker_id=worker_id
                )
                
                self.result_queue.put(result)
                
        worker_logger.info("Worker shutting down")
        
    def _get_work_for_scraper(self, scraper_id: str) -> Optional[WorkItem]:
        """Get next work item for specific scraper."""
        queue = self.scraper_queues.get(scraper_id)
        if not queue:
            return None
            
        try:
            # Non-blocking get
            work_item = queue.queue.get_nowait()
            return work_item
        except Empty:
            return None
            
    async def _distribute_work_fairly(self, start_slot: int, end_slot: int):
        """Distribute work fairly across all scrapers."""
        logger.info(f"Distributing work for slots {start_slot}-{end_slot}")
        
        total_slots = end_slot - start_slot
        slots_per_scraper = total_slots // len(self.scraper_classes)
        
        # Get datasets from state manager
        state_manager = StateManager(self.existing_clickhouse)
        
        for scraper_id in self.scraper_classes:
            # Get datasets for this scraper
            from src.core.datasets import DatasetRegistry
            registry = DatasetRegistry()
            datasets = registry.get_datasets_for_scraper(scraper_id)
            
            if not datasets:
                logger.warning(f"No datasets found for scraper {scraper_id}")
                continue
                
            # Create work items for each dataset
            work_items_created = 0
            
            for dataset in datasets:
                if not dataset.is_continuous:
                    continue
                    
                # Calculate ranges for this dataset
                dataset_start = start_slot
                dataset_end = end_slot
                
                # Create smaller work items for better distribution
                work_item_size = max(100, slots_per_scraper // 10)  # At least 10 items per scraper
                
                current = dataset_start
                while current < dataset_end:
                    work_end = min(current + work_item_size, dataset_end)
                    
                    work_item = WorkItem(
                        scraper_id=scraper_id,
                        dataset=dataset.name,
                        start_slot=current,
                        end_slot=work_end,
                        worker_batch_size=min(100, work_item_size),
                        priority=self._calculate_priority(current, scraper_id)
                    )
                    
                    # Add to scraper's queue
                    self.scraper_queues[scraper_id].queue.put(work_item)
                    work_items_created += 1
                    
                    current = work_end
                    
            logger.info(f"Created {work_items_created} work items for {scraper_id}")
            self.stats[scraper_id]['work_distributed'] = work_items_created
            
    def _calculate_priority(self, start_slot: int, scraper_id: str) -> int:
        """Calculate priority with scraper balancing."""
        # Base priority on slot number (earlier = higher priority)
        base_priority = 1000000 - start_slot
        
        # Adjust based on scraper load
        queue = self.scraper_queues.get(scraper_id)
        if queue:
            # Boost priority for scrapers with fewer items
            load_adjustment = 1000 * (10 - min(queue.queue.qsize(), 10))
            return base_priority + load_adjustment
            
        return base_priority
        
    async def _process_work_item(self, context, scraper, work_item: WorkItem) -> int:
        """Process a single work item."""
        rows_indexed = 0
        
        from src.utils.block_processor import BlockProcessor
        block_processor = BlockProcessor(context.beacon_api)
        
        # Process slots in batches
        current = work_item.start_slot
        batch_size = work_item.worker_batch_size
        
        while current < work_item.end_slot:
            batch_end = min(current + batch_size, work_item.end_slot)
            
            # For validator scraper, check if we need special handling
            if hasattr(scraper, 'get_target_slots_in_range'):
                # Get target slots for validator scraper
                target_slots = scraper.get_target_slots_in_range(
                    current, batch_end, context.clickhouse
                )
                
                # Process only target slots
                for slot in target_slots:
                    try:
                        success = await block_processor.get_and_process_block(
                            slot, [scraper.process]
                        )
                        if success:
                            rows_indexed += 1
                    except Exception as e:
                        logger.error(f"Error processing slot {slot}: {e}")
            else:
                # Regular processing for all slots
                for slot in range(current, batch_end):
                    try:
                        success = await block_processor.get_and_process_block(
                            slot, [scraper.process]
                        )
                        if success:
                            rows_indexed += 1
                    except Exception as e:
                        logger.error(f"Error processing slot {slot}: {e}")
                        
            current = batch_end
            
        return rows_indexed
        
    async def _collect_results(self):
        """Collect and process results from workers."""
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                result = self.result_queue.get(timeout=1)
                
                # Update statistics
                scraper_stats = self.stats[result.scraper_id]
                
                if result.success:
                    scraper_stats['work_completed'] += 1
                    scraper_stats['total_rows'] += result.rows_indexed
                    
                    # Update scraper queue stats
                    queue = self.scraper_queues.get(result.scraper_id)
                    if queue:
                        queue.completed_items += 1
                        queue.total_rows += result.rows_indexed
                else:
                    scraper_stats['work_failed'] += 1
                    
                    queue = self.scraper_queues.get(result.scraper_id)
                    if queue:
                        queue.failed_items += 1
                        
                # Log progress
                if scraper_stats['work_completed'] % 10 == 0:
                    logger.info(
                        f"{result.scraper_id}: {scraper_stats['work_completed']} completed, "
                        f"{scraper_stats['work_failed']} failed, "
                        f"{scraper_stats['total_rows']} rows"
                    )
                    
            except Empty:
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error collecting result: {e}")
                
    async def _monitor_progress(self):
        """Monitor and log progress for all scrapers."""
        while not self.stop_event.is_set():
            await asyncio.sleep(30)  # Log every 30 seconds
            
            logger.info("=== Worker Pool Progress ===")
            
            for scraper_id, queue in self.scraper_queues.items():
                stats = self.stats[scraper_id]
                pending = queue.queue.qsize()
                
                logger.info(
                    f"{scraper_id}: "
                    f"Workers: {len(queue.assigned_workers)}, "
                    f"Pending: {pending}, "
                    f"Completed: {stats['work_completed']}, "
                    f"Failed: {stats['work_failed']}, "
                    f"Rows: {stats['total_rows']}"
                )
                
    async def _wait_for_completion(self):
        """Wait for all work items to be processed."""
        logger.info("Waiting for all work to complete...")
        
        while True:
            all_complete = True
            
            for scraper_id, queue in self.scraper_queues.items():
                if not queue.queue.empty():
                    all_complete = False
                    break
                    
                # Check if there are any pending results
                stats = self.stats[scraper_id]
                distributed = stats['work_distributed']
                completed = stats['work_completed'] + stats['work_failed']
                
                if completed < distributed:
                    all_complete = False
                    break
                    
            if all_complete:
                logger.info("All work items completed")
                break
                
            await asyncio.sleep(1)
            
    def _log_final_statistics(self):
        """Log final statistics for all scrapers."""
        logger.info("=== Final Worker Pool Statistics ===")
        
        total_completed = 0
        total_failed = 0
        total_rows = 0
        
        for scraper_id in self.scraper_classes:
            stats = self.stats[scraper_id]
            queue = self.scraper_queues[scraper_id]
            
            logger.info(
                f"{scraper_id}: "
                f"Distributed: {stats['work_distributed']}, "
                f"Completed: {stats['work_completed']}, "
                f"Failed: {stats['work_failed']}, "
                f"Rows: {stats['total_rows']}, "
                f"Workers: {len(queue.assigned_workers)}"
            )
            
            total_completed += stats['work_completed']
            total_failed += stats['work_failed']
            total_rows += stats['total_rows']
            
        logger.info(
            f"TOTAL: Completed: {total_completed}, "
            f"Failed: {total_failed}, Rows: {total_rows}"
        )
        
    async def _create_worker_context(self, worker_id: str):
        """Create context for a worker process."""
        try:
            # Create beacon API connection
            beacon_api = BeaconAPIService(self.beacon_url)
            await beacon_api.start()
            
            # Use existing clickhouse or create new
            if self.existing_clickhouse:
                clickhouse = self.existing_clickhouse
            else:
                clickhouse = ClickHouseService()
                
            # Create state manager
            state_manager = StateManager(clickhouse)
            
            return type('WorkerContext', (), {
                'worker_id': worker_id,
                'beacon_api': beacon_api,
                'clickhouse': clickhouse,
                'state_manager': state_manager,
                'specs_manager': self.specs_manager
            })()
            
        except Exception as e:
            logger.error(f"Failed to create worker context: {e}")
            return None