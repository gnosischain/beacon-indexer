"""
Worker pool service for parallel processing with lazy range creation.
"""
import asyncio
import time
import queue
import threading
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime

from src.utils.logger import logger, setup_logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.core.datasets import DatasetRegistry
from src.utils.specs_manager import SpecsManager


@dataclass
class WorkItem:
    """Represents a unit of work."""
    scraper_id: str
    dataset: str
    start_slot: int
    end_slot: int
    priority: int = 0


@dataclass 
class WorkResult:
    """Result from processing a work item."""
    scraper_id: str
    dataset: str
    start_slot: int
    end_slot: int
    success: bool
    rows_indexed: int = 0
    error_message: str = ""
    duration: float = 0.0
    worker_id: str = ""


class WorkerContext:
    """Context for a worker thread."""
    def __init__(self, worker_id: str, config: Dict[str, Any]):
        self.worker_id = worker_id
        self.beacon_api = None
        self.clickhouse = None
        self.state_manager = None
        self.scrapers = {}
        self.config = config


class WorkerPoolService:
    """Manages a pool of workers for parallel processing."""
    
    def __init__(
        self,
        beacon_url: str,
        clickhouse_config: Dict[str, Any] = None,
        scraper_classes: Dict[str, type] = None,
        num_workers: int = 4,
        existing_clickhouse: ClickHouseService = None,
        specs_manager: SpecsManager = None
    ):
        self.beacon_url = beacon_url
        self.clickhouse_config = clickhouse_config
        self.scraper_classes = scraper_classes or {}
        self.num_workers = num_workers
        self.existing_clickhouse = existing_clickhouse
        self.specs_manager = specs_manager
        
        # State manager
        self.state_manager = None
        
        # Dataset registry
        self.dataset_registry = DatasetRegistry()
        
        # Worker management
        self.workers = []
        self.stop_event = threading.Event()
        
        # Work distribution
        self.scraper_queues = {}
        self.result_queue = queue.Queue()
        
        # Initialize queues for each scraper
        for scraper_id in self.scraper_classes:
            self.scraper_queues[scraper_id] = queue.Queue(maxsize=num_workers * 2)
            
        # Statistics
        self.stats = defaultdict(lambda: {
            'completed': 0,
            'failed': 0,
            'total_rows': 0,
            'total_duration': 0.0
        })
        
    def set_state_manager(self, state_manager: StateManager):
        """Set the state manager instance."""
        self.state_manager = state_manager
        
    def set_specs_manager(self, specs_manager: SpecsManager):
        """Set the specs manager instance."""
        self.specs_manager = specs_manager
        
    async def process_range(self, start_slot: int, end_slot: int):
        """Process a range using lazy range creation."""
        logger.info(f"Starting worker pool for slots {start_slot}-{end_slot}")
        logger.info(f"Active scrapers: {list(self.scraper_classes.keys())}")
        
        # Save range for workers
        self.start_slot = start_slot
        self.end_slot = end_slot
        
        # Start workers
        self._start_workers()
        
        # Start monitoring tasks
        monitor_task = asyncio.create_task(self._monitor_progress())
        distribute_task = asyncio.create_task(
            self._distribute_work_lazy(start_slot, end_slot)
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
                
        except Exception as e:
            logger.error(f"Error in worker pool: {e}")
            self.stop_event.set()
            raise
        finally:
            self._log_final_statistics()
            
    def _start_workers(self):
        """Start worker threads."""
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker_thread,
                args=(i,),
                name=f"Worker-{i}"
            )
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
            
        logger.info(f"Started {self.num_workers} worker threads")
        
    def _worker_thread(self, worker_num: int):
        """Worker thread main loop."""
        worker_id = f"worker_{worker_num}"
        logger.info(f"Worker {worker_num} thread started")
        
        # Create an event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        context = WorkerContext(worker_id, self.clickhouse_config or {})
        
        try:
            # Initialize connections in async context
            loop.run_until_complete(self._initialize_worker_async(context, worker_num))
            
            logger.info(f"Worker {worker_num} entering main loop")
            
            # Process work items
            while not self.stop_event.is_set():
                try:
                    # Get work from any queue
                    work_item = self._get_next_work_item()
                    
                    if work_item is None:
                        time.sleep(0.1)
                        continue
                    
                    logger.info(f"Worker {worker_num} got work: {work_item.dataset} {work_item.start_slot}-{work_item.end_slot}")
                    
                    # Process the work
                    start_time = time.time()
                    
                    try:
                        scraper = context.scrapers.get(work_item.scraper_id)
                        if not scraper:
                            raise ValueError(f"Unknown scraper: {work_item.scraper_id}")
                        
                        # Process the range
                        rows = loop.run_until_complete(
                            self._process_range_async(scraper, work_item.start_slot, work_item.end_slot)
                        )
                        
                        # Mark as completed using thread's own state manager
                        context.state_manager.complete_range(
                            "historical", work_item.dataset, 
                            work_item.start_slot, work_item.end_slot, rows
                        )
                        
                        # Report success
                        result = WorkResult(
                            scraper_id=work_item.scraper_id,
                            dataset=work_item.dataset,
                            start_slot=work_item.start_slot,
                            end_slot=work_item.end_slot,
                            success=True,
                            rows_indexed=rows,
                            duration=time.time() - start_time,
                            worker_id=worker_id
                        )
                        
                        self.result_queue.put(result)
                        logger.info(f"Worker {worker_num} completed {work_item.dataset} {work_item.start_slot}-{work_item.end_slot}: {rows} rows")
                        
                    except Exception as e:
                        logger.error(f"Worker {worker_num} error: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        
                        # Mark as failed using thread's own state manager
                        try:
                            context.state_manager.fail_range(
                                "historical", work_item.dataset,
                                work_item.start_slot, work_item.end_slot, str(e)[:500]
                            )
                        except Exception as fail_error:
                            logger.error(f"Error marking range as failed: {fail_error}")
                        
                        # Report failure
                        result = WorkResult(
                            scraper_id=work_item.scraper_id,
                            dataset=work_item.dataset,
                            start_slot=work_item.start_slot,
                            end_slot=work_item.end_slot,
                            success=False,
                            error_message=str(e),
                            duration=time.time() - start_time,
                            worker_id=worker_id
                        )
                        
                        self.result_queue.put(result)
                        
                except Exception as e:
                    logger.error(f"Worker {worker_num} unexpected error: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"Worker {worker_num} failed during initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Cleanup
            try:
                if hasattr(context, 'beacon_api') and context.beacon_api:
                    if hasattr(context.beacon_api, 'session') and context.beacon_api.session:
                        loop.run_until_complete(context.beacon_api.session.close())
            except:
                pass
            
            loop.close()
            logger.info(f"Worker {worker_num} shutting down")

    async def _initialize_worker_async(self, context: WorkerContext, worker_num: int):
        """Initialize worker connections in async context."""
        # Create and start beacon API
        context.beacon_api = BeaconAPIService(base_url=self.beacon_url)
        await context.beacon_api.start()
        
        # Create ClickHouse connection
        if self.clickhouse_config:
            context.clickhouse = ClickHouseService(
                host=self.clickhouse_config['host'],
                port=self.clickhouse_config['port'],
                user=self.clickhouse_config['user'],
                password=self.clickhouse_config['password'],
                database=self.clickhouse_config['database'],
                secure=self.clickhouse_config.get('secure', True),
                verify=self.clickhouse_config.get('verify', False)
            )
        else:
            context.clickhouse = ClickHouseService()
        
        # Create thread-specific state manager
        context.state_manager = StateManager(context.clickhouse)
        
        # Initialize scrapers
        for scraper_id, scraper_class in self.scraper_classes.items():
            scraper = scraper_class(context.beacon_api, context.clickhouse)
            if self.specs_manager:
                scraper.set_specs_manager(self.specs_manager)
            context.scrapers[scraper_id] = scraper
        
        logger.info(f"Worker {worker_num} initialized with own connections")

    async def _process_range_async(self, scraper, start_slot: int, end_slot: int) -> int:
        """Process a range with a scraper."""
        rows = 0
        
        logger.info(f"Processing range {start_slot}-{end_slot} for scraper {scraper.scraper_id}")
        
        # Get blocks in batches
        batch_size = 100
        current = start_slot
        
        while current < end_slot:
            batch_end = min(current + batch_size, end_slot)
            
            # Get blocks one by one
            for slot in range(current, batch_end):
                try:
                    # Get block data
                    block = await scraper.beacon_api.get_block(str(slot))
                    
                    if block:
                        # Process with scraper
                        await scraper.process(block)
                        rows += 1
                        
                except Exception as e:
                    # Some slots might be empty, especially in early epochs
                    if "404" not in str(e) and "not found" not in str(e).lower():
                        logger.debug(f"Error processing slot {slot}: {e}")
                        
            current = batch_end
        
        logger.info(f"Completed processing {start_slot}-{end_slot}: {rows} rows")
        return rows
        
    def _get_next_work_item(self) -> Optional[WorkItem]:
        """Get next work item from any queue."""
        # Try each scraper queue in round-robin fashion
        for scraper_id, queue in self.scraper_queues.items():
            try:
                return queue.get_nowait()
            except:
                continue
        return None
        
    async def _distribute_work_lazy(self, start_slot: int, end_slot: int):
        """Distribute work using lazy range creation - simplified version."""
        logger.info(f"Starting lazy work distribution for slots {start_slot}-{end_slot}")
        
        # First, check what pending ranges we have
        try:
            query = """
            SELECT dataset, start_slot, end_slot
            FROM indexing_state
            WHERE mode = 'historical' AND status = 'pending'
            ORDER BY dataset, start_slot
            """
            result = self.state_manager.db.execute(query)
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
            
            logger.info(f"Found {len(result)} pending ranges to process")
            for row in result[:5]:  # Show first 5
                logger.info(f"  Pending: {row['dataset']} {row['start_slot']}-{row['end_slot']}")
        except Exception as e:
            logger.error(f"Error checking pending ranges: {e}")
        
        # Map dataset names to scrapers - MUST match exactly what bootstrap created
        dataset_to_scraper = {
            'blocks': 'core_block_scraper',
            'execution_payloads': 'core_block_scraper',
            'deposits': 'operational_events_scraper',
            'withdrawals': 'operational_events_scraper',
            'voluntary_exits': 'operational_events_scraper',
            'bls_to_execution_changes': 'operational_events_scraper',
            'validators': 'validator_scraper'
        }
        
        completed_datasets = set()
        iterations = 0
        
        while not self.stop_event.is_set():
            iterations += 1
            work_created = False
            
            # Process each dataset directly
            for dataset_name, scraper_id in dataset_to_scraper.items():
                if dataset_name in completed_datasets:
                    continue
                    
                # Check if this scraper is available
                if scraper_id not in self.scraper_classes:
                    continue
                    
                # Check queue
                queue = self.scraper_queues.get(scraper_id)
                if not queue:
                    logger.error(f"No queue for scraper {scraper_id}")
                    continue
                    
                if queue.qsize() >= self.num_workers:
                    continue  # Queue full
                    
                # Try to claim a pending range for this dataset
                try:
                    # Directly try to claim the pending range
                    claim_query = """
                    SELECT start_slot, end_slot
                    FROM indexing_state
                    WHERE mode = 'historical' 
                      AND dataset = %(dataset)s
                      AND status = 'pending'
                    ORDER BY start_slot
                    LIMIT 1
                    """
                    
                    result = self.state_manager.db.execute(claim_query, {
                        'dataset': dataset_name
                    })
                    
                    if hasattr(result, '__iter__') and not isinstance(result, list):
                        result = list(result)
                        
                    if result:
                        start = result[0]['start_slot']
                        end = result[0]['end_slot']
                        
                        # Claim it
                        if self.state_manager.claim_range('historical', dataset_name, start, end, 'distributor'):
                            # Create work item
                            work_item = WorkItem(
                                scraper_id=scraper_id,
                                dataset=dataset_name,
                                start_slot=start,
                                end_slot=end
                            )
                            
                            queue.put(work_item)
                            work_created = True
                            logger.info(f"Distributed work: {dataset_name} {start}-{end} to {scraper_id}")
                    else:
                        # No pending ranges, try to create next one
                        create_query = """
                        SELECT MAX(end_slot) as max_end
                        FROM indexing_state
                        WHERE mode = 'historical' 
                          AND dataset = %(dataset)s
                          AND status = 'completed'
                        """
                        
                        result = self.state_manager.db.execute(create_query, {
                            'dataset': dataset_name
                        })
                        
                        if hasattr(result, '__iter__') and not isinstance(result, list):
                            result = list(result)
                            
                        max_end = result[0]['max_end'] if result and result[0]['max_end'] else self.start_slot
                        
                        if max_end < end_slot:
                            # Create next range
                            next_start = max_end
                            next_end = min(next_start + self.state_manager.range_size, end_slot)
                            
                            if self.state_manager.create_range('historical', dataset_name, next_start, next_end):
                                logger.info(f"Created new range for {dataset_name}: {next_start}-{next_end}")
                                # Will be picked up in next iteration
                        else:
                            # Dataset complete
                            completed_datasets.add(dataset_name)
                            logger.info(f"Dataset {dataset_name} completed")
                            
                except Exception as e:
                    logger.error(f"Error processing dataset {dataset_name}: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            # Check if all done
            if len(completed_datasets) == len(dataset_to_scraper):
                logger.info("All datasets completed")
                break
                
            if not work_created:
                if iterations % 10 == 0:
                    logger.info(f"No work created in iteration {iterations}")
                await asyncio.sleep(1)
            else:
                await asyncio.sleep(0.1)
        
        logger.info(f"Work distribution finished after {iterations} iterations")
        
    async def _monitor_progress(self):
        """Monitor and log progress."""
        last_log = time.time()
        log_interval = 30  # seconds
        
        while not self.stop_event.is_set():
            try:
                now = time.time()
                if now - last_log >= log_interval:
                    self._log_progress()
                    last_log = now
                    
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in progress monitor: {e}")
                
    async def _collect_results(self):
        """Collect and process results."""
        while not self.stop_event.is_set():
            try:
                # Get result with timeout
                try:
                    result = self.result_queue.get(timeout=1)
                except:
                    continue
                    
                # Update statistics
                stats = self.stats[result.scraper_id]
                
                if result.success:
                    stats['completed'] += 1
                    stats['total_rows'] += result.rows_indexed
                else:
                    stats['failed'] += 1
                    
                stats['total_duration'] += result.duration
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting results: {e}")
                
    async def _wait_for_completion(self):
        """Wait for all work to complete."""
        check_interval = 5
        no_activity_count = 0
        max_no_activity = 12  # 1 minute
        
        while not self.stop_event.is_set():
            # Check if all queues are empty
            all_empty = all(
                q.empty() for q in self.scraper_queues.values()
            )
            
            # Check if any workers are processing
            processing = self._count_processing_ranges()
            
            if all_empty and processing == 0:
                no_activity_count += 1
                
                if no_activity_count >= max_no_activity:
                    logger.info("No activity detected, assuming completion")
                    break
            else:
                no_activity_count = 0
                
            await asyncio.sleep(check_interval)
            
    def _count_processing_ranges(self) -> int:
        """Count ranges currently being processed."""
        try:
            query = """
            SELECT COUNT(*) as count
            FROM indexing_state
            WHERE mode = 'historical' AND status = 'processing'
            """
            result = self.state_manager.db.execute(query)
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
            return result[0]['count'] if result else 0
        except:
            return 0
            
    def _log_progress(self):
        """Log current progress."""
        try:
            summary = self.state_manager.get_progress_summary("historical")
            
            for dataset, stats in summary.items():
                total = stats['total_ranges']
                completed = stats['completed_ranges']
                processing = stats['processing_ranges']
                failed = stats['failed_ranges']
                
                if total > 0:
                    pct = (completed / total) * 100
                    logger.info(
                        f"{dataset}: {completed}/{total} ({pct:.1f}%) "
                        f"[processing: {processing}, failed: {failed}]"
                    )
                    
        except Exception as e:
            logger.error(f"Error logging progress: {e}")
            
    def _log_final_statistics(self):
        """Log final statistics."""
        logger.info("Worker pool statistics:")
        
        total_completed = 0
        total_failed = 0
        total_rows = 0
        
        for scraper_id, stats in self.stats.items():
            completed = stats['completed']
            failed = stats['failed']
            rows = stats['total_rows']
            
            total_completed += completed
            total_failed += failed
            total_rows += rows
            
            if completed > 0:
                avg_duration = stats['total_duration'] / completed
                logger.info(
                    f"  {scraper_id}: {completed} completed, {failed} failed, "
                    f"{rows:,} rows, {avg_duration:.2f}s avg"
                )
                
        logger.info(
            f"Total: {total_completed} completed, {total_failed} failed, "
            f"{total_rows:,} rows"
        )