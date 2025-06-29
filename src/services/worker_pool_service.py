"""
Fixed worker pool service with proper parallel processing and work distribution.
"""
import os 
import asyncio
import time
import uuid
from typing import Dict, Type, List, Optional, Any, Tuple
from dataclasses import dataclass
from queue import Queue, Empty, PriorityQueue
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import threading
from collections import defaultdict

from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.services.bulk_insertion_service import BulkInsertionService
from src.utils.logger import logger
from src.scrapers.base_scraper import BaseScraper
from src.utils.specs_manager import SpecsManager


@dataclass
class WorkItem:
    """Represents a unit of work for the pool."""
    range_id: str
    scraper_id: str
    table_name: str
    start_slot: int
    end_slot: int
    priority: int = 0
    attempt: int = 0
    
    def __lt__(self, other):
        """For priority queue ordering."""
        return self.priority > other.priority  # Higher priority first


@dataclass  
class WorkResult:
    """Result from processing a work item."""
    range_id: str
    scraper_id: str
    table_name: str
    start_slot: int
    end_slot: int
    success: bool
    rows_indexed: int = 0
    error: Optional[str] = None
    duration: float = 0.0


class WorkerContext:
    """Context for each worker thread with its own connections."""
    
    def __init__(self, worker_id: str, beacon_url: str, clickhouse_config: dict):
        self.worker_id = worker_id
        self.beacon_url = beacon_url
        self.clickhouse_config = clickhouse_config
        self._beacon_api = None
        self._clickhouse = None
        self._bulk_inserter = None
        self._state_manager = None
        self._loop = None
        self._connection_retry_count = 3
        self._connection_retry_delay = 2  # seconds
        
    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get or create event loop for this thread."""
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop
        
    @property
    def beacon_api(self) -> BeaconAPIService:
        """Get thread-local beacon API instance."""
        if self._beacon_api is None:
            self._beacon_api = BeaconAPIService(self.beacon_url)
            # Start the beacon API in this thread's event loop
            self.loop.run_until_complete(self._beacon_api.start())
        return self._beacon_api
        
    @property
    def clickhouse(self) -> ClickHouseService:
        """Get thread-local ClickHouse instance with retry logic."""
        if self._clickhouse is None:
            for attempt in range(self._connection_retry_count):
                try:
                    self._clickhouse = ClickHouseService(
                        host=self.clickhouse_config['host'],
                        port=self.clickhouse_config['port'],
                        user=self.clickhouse_config['user'],
                        password=self.clickhouse_config['password'],
                        database=self.clickhouse_config['database'],
                        secure=self.clickhouse_config['secure'],
                        verify=self.clickhouse_config['verify']
                    )
                    logger.info(f"{self.worker_id} successfully connected to ClickHouse")
                    break
                except Exception as e:
                    if attempt < self._connection_retry_count - 1:
                        logger.warning(f"{self.worker_id} ClickHouse connection attempt {attempt + 1} failed, retrying...")
                        time.sleep(self._connection_retry_delay)
                    else:
                        logger.error(f"{self.worker_id} failed to connect to ClickHouse after {self._connection_retry_count} attempts")
                        raise
        return self._clickhouse
        
    @property
    def bulk_inserter(self) -> BulkInsertionService:
        """Get thread-local bulk inserter."""
        if self._bulk_inserter is None:
            self._bulk_inserter = BulkInsertionService(
                clickhouse_service=self.clickhouse,
                max_parallel_inserts=2,
                batch_size=500  # Smaller batch size for more frequent flushes
            )
        return self._bulk_inserter
        
    @property
    def state_manager(self) -> StateManager:
        """Get thread-local state manager."""
        if self._state_manager is None:
            self._state_manager = StateManager(self.clickhouse)
        return self._state_manager
        
    def cleanup(self):
        """Clean up resources."""
        if self._bulk_inserter:
            self._bulk_inserter.flush_all()  # Final flush
            self._bulk_inserter.stop()
        if self._loop:
            self._loop.close()


class WorkerPoolService:
    """
    Thread-based worker pool for parallel processing of beacon chain data.
    """
    
    def __init__(
        self, 
        beacon_url: str,
        clickhouse_config: dict,
        scraper_classes: Dict[str, Type[BaseScraper]],
        num_workers: int = 4,
        existing_clickhouse: Optional[ClickHouseService] = None
    ):
        self.beacon_url = beacon_url
        self.clickhouse_config = clickhouse_config
        self.scraper_classes = scraper_classes
        self.num_workers = num_workers
        
        # Work management
        self.work_queue = PriorityQueue()
        self.results_queue = Queue()
        self.completed_work = set()
        self.active_work = {}
        
        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        self.worker_futures = []
        
        # Control flags
        self.running = False
        self.lock = threading.Lock()
        
        # Monitoring
        self.stats = {
            'work_distributed': 0,
            'work_started': 0,
            'work_completed': 0,
            'work_failed': 0,
            'total_rows': 0,
            'worker_errors': 0
        }
        
        # Managers that are set later
        self.specs_manager = None
        self.state_manager = StateManager(existing_clickhouse) if existing_clickhouse else None
        
    def set_specs_manager(self, specs_manager: SpecsManager):
        """Set the specs manager."""
        self.specs_manager = specs_manager
        
    def set_state_manager(self, state_manager: StateManager):
        """Set the state manager."""
        self.state_manager = state_manager
        
    async def process_range(self, start_slot: int, end_slot: int):
        """Process a range of slots using the worker pool."""
        logger.info(f"Starting worker pool processing for slots {start_slot}-{end_slot}")
        
        self.running = True
        
        # Start worker threads
        for i in range(self.num_workers):
            worker_id = f"worker-{i+1}"
            future = self.executor.submit(self._worker_thread, worker_id)
            self.worker_futures.append(future)
            
        try:
            # Start monitoring task
            monitor_task = asyncio.create_task(self._monitor_progress())
            
            # Distribute work
            await self._distribute_work(end_slot)
            
            # Wait for completion
            await self._wait_for_completion(end_slot)
            
        finally:
            # Stop workers
            self.running = False
            
            # Wait for workers to finish
            for future in as_completed(self.worker_futures, timeout=30):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Worker error: {e}")
                    
            # Cancel monitoring
            monitor_task.cancel()
            
            # Shutdown executor
            self.executor.shutdown(wait=True)
            
            logger.info("Worker pool shut down")
            
    async def _wait_for_completion(self, end_slot: int):
        """Wait for all work to be completed."""
        # Reuse the main StateManager's connection for monitoring
        while self.running:
            # Check if all work is done using the state manager
            progress = self.state_manager.db.execute("""
                SELECT 
                    countIf(status = 'completed') as completed,
                    countIf(status = 'processing') as processing,
                    countIf(status = 'pending') as pending,
                    countIf(status = 'failed') as failed
                FROM indexing_state FINAL
                WHERE start_slot < %(end_slot)s
            """, {"end_slot": end_slot})
            
            if progress:
                stats = progress[0]
                if stats['pending'] == 0 and stats['processing'] == 0:
                    logger.info("All work completed!")
                    break
            
            await asyncio.sleep(5)
    
    async def _distribute_work(self, end_slot: int):
        """Continuously distribute work to workers."""
        distributed_count = 0
        seen_ranges = set()
        
        logger.info("Starting work distribution...")
        
        # Pre-calculate validator ranges if validator scraper is enabled
        validator_ranges = set()
        if "validator_scraper" in self.scraper_classes:
            # Get all possible ranges
            all_ranges = []
            range_size = int(os.getenv("STATE_RANGE_SIZE", "10000"))
            current = 0
            while current < end_slot:
                range_end = min(current + range_size - 1, end_slot)
                all_ranges.append((current, range_end))
                current = range_end + 1
            
            # Filter to only those with validator slots
            from src.scrapers.validator_scraper import ValidatorScraper
            validator_ranges_list = []
            for start, end in all_ranges:
                target_slots = ValidatorScraper.get_target_slots_in_range(
                    start, end, self.state_manager.db
                )
                if target_slots:
                    validator_ranges.add((start, end))
                    validator_ranges_list.append((start, end))
            
            logger.info(f"Found {len(validator_ranges)} ranges containing validator target slots")
        
        while self.running:
            work_found = False
            
            # Check each scraper/table combination for work
            for scraper_id, scraper_class in self.scraper_classes.items():
                # Create temporary instance to get tables
                temp_scraper = scraper_class(None, None)
                tables = temp_scraper.get_tables_written()
                
                for table_name in tables:
                    # For validator scraper, only check ranges with target slots
                    if scraper_id == "validator_scraper" and table_name == "validators":
                        # Only get ranges that contain validator slots
                        for start_slot, range_end in validator_ranges:
                            range_key = f"{scraper_id}:{table_name}:{start_slot}:{range_end}"
                            
                            if range_key in seen_ranges:
                                continue
                            
                            # Check if this range needs processing
                            status = self.state_manager._get_latest_status(
                                scraper_id, table_name, start_slot, range_end
                            )
                            
                            if not status or status['status'] in ['pending', 'failed']:
                                seen_ranges.add(range_key)
                                
                                work_item = WorkItem(
                                    range_id=range_key,
                                    scraper_id=scraper_id,
                                    table_name=table_name,
                                    start_slot=start_slot,
                                    end_slot=range_end,
                                    priority=self._calculate_priority(scraper_id, start_slot)
                                )
                                
                                self.work_queue.put(work_item)
                                distributed_count += 1
                                work_found = True
                                
                                with self.lock:
                                    self.stats['work_distributed'] += 1
                    else:
                        # Regular scraper - process all ranges
                        range_info = self.state_manager.get_next_range(
                            scraper_id, table_name, 
                            f"pool-coordinator", 
                            end_slot
                        )
                        
                        if range_info:
                            start_slot, range_end = range_info
                            range_key = f"{scraper_id}:{table_name}:{start_slot}:{range_end}"
                            
                            if range_key not in seen_ranges:
                                seen_ranges.add(range_key)
                                
                                work_item = WorkItem(
                                    range_id=range_key,
                                    scraper_id=scraper_id,
                                    table_name=table_name,
                                    start_slot=start_slot,
                                    end_slot=range_end,
                                    priority=self._calculate_priority(scraper_id, start_slot)
                                )
                                
                                self.work_queue.put(work_item)
                                distributed_count += 1
                                work_found = True
                                
                                with self.lock:
                                    self.stats['work_distributed'] += 1
                        
            if not work_found:
                # No more work found, wait a bit before checking again
                await asyncio.sleep(2)
                
        logger.info(f"Work distribution complete. Distributed {distributed_count} items")
    
    def _calculate_priority(self, scraper_id: str, start_slot: int) -> int:
        """Calculate priority for a work item."""
        # Higher priority for core scrapers and lower slots
        priority = 1000000 - start_slot  # Lower slots first
        
        if scraper_id == "core_block_scraper":
            priority += 1000000  # Highest priority
        elif scraper_id == "block_scraper":
            priority += 500000
            
        return priority
    
    def _worker_thread(self, worker_id: str):
        """Worker thread that processes work items."""
        logger.info(f"{worker_id} started")
        
        # Create worker context
        context = WorkerContext(worker_id, self.beacon_url, self.clickhouse_config)
        
        try:
            while self.running:
                try:
                    # Get work item with timeout
                    work_item = self.work_queue.get(timeout=5)
                    
                    with self.lock:
                        self.active_work[worker_id] = work_item
                        self.stats['work_started'] += 1
                    
                    # Process the work
                    result = self._process_work_item(work_item, context)
                    
                    # Store result
                    self.results_queue.put(result)
                    
                    with self.lock:
                        if worker_id in self.active_work:
                            del self.active_work[worker_id]
                        self.completed_work.add(work_item.range_id)
                        
                        if result.success:
                            self.stats['work_completed'] += 1
                            self.stats['total_rows'] += result.rows_indexed
                        else:
                            self.stats['work_failed'] += 1
                    
                except Empty:
                    continue
                except Exception as e:
                    logger.error(f"{worker_id} error: {e}")
                    with self.lock:
                        self.stats['worker_errors'] += 1
        finally:
            context.cleanup()
            logger.info(f"{worker_id} stopped")
    
    def _process_work_item(self, work_item: WorkItem, context: WorkerContext) -> WorkResult:
        """Process a single work item."""
        start_time = time.time()
        
        try:
            # Create scraper instance
            scraper_class = self.scraper_classes[work_item.scraper_id]
            scraper = scraper_class(context.beacon_api, context.clickhouse)
            
            # Set managers
            if self.specs_manager:
                scraper.set_specs_manager(self.specs_manager)
            if self.state_manager:
                scraper.set_state_manager(self.state_manager)
            
            # Inject bulk inserter
            scraper._bulk_inserter = context.bulk_inserter
            
            # Reset row counts
            scraper.reset_row_counts()
            
            # Special handling for validator scraper
            if work_item.scraper_id == "validator_scraper" and work_item.table_name == "validators":
                # Check if this range has any validator target slots
                if hasattr(scraper, 'get_target_slots_in_range'):
                    target_slots = scraper.get_target_slots_in_range(
                        work_item.start_slot, work_item.end_slot, context.clickhouse
                    )
                    if not target_slots:
                        # No target slots in this range, mark as completed with 0 rows
                        logger.info(f"{context.worker_id} skipping {work_item.range_id} - no validator target slots")
                        
                        # Claim and immediately complete the range
                        if context.state_manager.claim_range(
                            work_item.scraper_id, work_item.table_name,
                            work_item.start_slot, work_item.end_slot,
                            context.worker_id
                        ):
                            context.state_manager.complete_range(
                                work_item.scraper_id, work_item.table_name,
                                work_item.start_slot, work_item.end_slot,
                                0  # 0 rows
                            )
                        
                        return WorkResult(
                            range_id=work_item.range_id,
                            scraper_id=work_item.scraper_id,
                            table_name=work_item.table_name,
                            start_slot=work_item.start_slot,
                            end_slot=work_item.end_slot,
                            success=True,
                            rows_indexed=0,
                            duration=time.time() - start_time
                        )
            
            # Process slots in the range
            batch_size = int(os.getenv("WORKER_BATCH_SIZE", "100"))
            current = work_item.start_slot
            
            logger.info(f"{context.worker_id} processing {work_item.range_id}")
            
            # Try to claim the range for the specific table in the work item
            if not context.state_manager.claim_range(
                work_item.scraper_id, work_item.table_name,
                work_item.start_slot, work_item.end_slot,
                context.worker_id
            ):
                # Already being processed
                logger.warning(f"{context.worker_id} failed to claim {work_item.range_id} - already claimed")
                return WorkResult(
                    range_id=work_item.range_id,
                    scraper_id=work_item.scraper_id,
                    table_name=work_item.table_name,
                    start_slot=work_item.start_slot,
                    end_slot=work_item.end_slot,
                    success=False,
                    error="Range already claimed",
                    duration=time.time() - start_time
                )
            
            # Process the range
            slots_processed = 0
            blocks_found = 0
            
            while current < work_item.end_slot:
                batch_end = min(current + batch_size, work_item.end_slot)
                
                # Process batch of slots
                for slot in range(current, batch_end):
                    try:
                        # Fetch actual block data
                        block_data = context.loop.run_until_complete(
                            context.beacon_api.get_block(str(slot))
                        )
                        
                        # Process with scraper - it will write to all its tables
                        context.loop.run_until_complete(scraper.process(block_data))
                        blocks_found += 1
                        
                    except Exception as e:
                        if "404" not in str(e):  # Log non-404 errors
                            logger.debug(f"Error processing slot {slot}: {e}")
                    
                    slots_processed += 1
                    
                    # Periodic flush every 100 slots
                    if slots_processed % 100 == 0:
                        context.bulk_inserter.flush_all()
                        logger.debug(f"{context.worker_id} flushed after {slots_processed} slots")
                
                # Flush after each batch
                context.bulk_inserter.flush_all()
                current = batch_end
            
            # Final flush to ensure all data is written
            context.bulk_inserter.flush_all()
            
            # Log queue sizes for debugging
            queue_sizes = context.bulk_inserter.get_queue_sizes()
            if any(size > 0 for size in queue_sizes.values()):
                logger.warning(f"{context.worker_id} has unflushed data: {queue_sizes}")
                # Force another flush
                context.bulk_inserter.flush_all()
            
            # Get final row counts for the specific table we're tracking
            row_counts = scraper.get_row_counts()
            table_rows = row_counts.get(work_item.table_name, 0)
            
            # Mark range as completed only for the table we claimed
            context.state_manager.complete_range(
                work_item.scraper_id, work_item.table_name,
                work_item.start_slot, work_item.end_slot,
                table_rows
            )
            
            duration = time.time() - start_time
            logger.info(
                f"{context.worker_id} completed {work_item.range_id} in {duration:.2f}s "
                f"({table_rows} rows, {blocks_found} blocks found, {slots_processed} slots processed)"
            )
            
            return WorkResult(
                range_id=work_item.range_id,
                scraper_id=work_item.scraper_id,
                table_name=work_item.table_name,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                success=True,
                rows_indexed=table_rows,
                duration=duration
            )
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"{context.worker_id} failed {work_item.range_id}: {error_msg}")
            
            # Mark range as failed for the table we were processing
            context.state_manager.fail_range(
                work_item.scraper_id, work_item.table_name,
                work_item.start_slot, work_item.end_slot,
                error_msg
            )
            
            return WorkResult(
                range_id=work_item.range_id,
                scraper_id=work_item.scraper_id,
                table_name=work_item.table_name,
                start_slot=work_item.start_slot,
                end_slot=work_item.end_slot,
                success=False,
                error=error_msg,
                duration=time.time() - start_time
            )
    
    async def _monitor_progress(self):
        """Monitor and log progress periodically."""
        log_interval = int(os.getenv("WORKER_PROGRESS_LOG_INTERVAL", "30"))
        
        while self.running:
            await asyncio.sleep(log_interval)
            
            with self.lock:
                stats = self.stats.copy()
                active = len(self.active_work)
                
            # Query database for overall progress
            db_stats = self.state_manager.db.execute("""
                SELECT 
                    countIf(status = 'completed') as completed,
                    countIf(status = 'processing') as processing,
                    countIf(status = 'pending') as pending,
                    countIf(status = 'failed') as failed,
                    sum(rows_indexed) as total_rows
                FROM indexing_state FINAL
            """)
            
            if db_stats:
                db_stats = db_stats[0]
                logger.info(
                    f"Progress: {db_stats['completed']} completed, "
                    f"{db_stats['processing']} processing, "
                    f"{db_stats['pending']} pending, "
                    f"{db_stats['failed']} failed, "
                    f"{active} active workers, "
                    f"{db_stats['total_rows']:,} total rows"
                )
            
            # Log worker stats
            logger.info(
                f"Worker stats: {stats['work_distributed']} distributed, "
                f"{stats['work_started']} started, "
                f"{stats['work_completed']} completed, "
                f"{stats['work_failed']} failed"
            )