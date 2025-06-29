import asyncio
from typing import Dict, List, Any
import time
import threading
from src.utils.logger import logger

class BulkInsertionService:
    """
    Service for optimizing ClickHouse insertions by batching and parallelizing inserts.
    Thread-safe implementation for use in worker threads.
    """
    
    def __init__(self, clickhouse_service, max_parallel_inserts=5, batch_size=1000):
        self.clickhouse = clickhouse_service
        self.max_parallel_inserts = max_parallel_inserts
        self.batch_size = batch_size
        self.insertion_queues = {}  # One queue per table
        self._lock = threading.Lock()  # Thread safety
        self._stop_event = threading.Event()
        self._flush_interval = 2.0  # Flush every 2 seconds
        self._last_flush = {}  # Track last flush time per table
    
    def queue_for_insertion(self, table_name: str, data: Dict[str, Any]):
        """Queue a record for bulk insertion (thread-safe)."""
        with self._lock:
            if table_name not in self.insertion_queues:
                self.insertion_queues[table_name] = []
                self._last_flush[table_name] = time.time()
            
            self.insertion_queues[table_name].append(data)
            
            # Check if we should flush based on size or time
            should_flush = (
                len(self.insertion_queues[table_name]) >= self.batch_size or
                (time.time() - self._last_flush[table_name]) > self._flush_interval
            )
            
            if should_flush:
                self._flush_table(table_name)
    
    def _flush_table(self, table_name: str):
        """Flush a specific table's queue (must be called with lock held)."""
        if table_name not in self.insertion_queues or not self.insertion_queues[table_name]:
            return
            
        batch = self.insertion_queues[table_name]
        self.insertion_queues[table_name] = []
        self._last_flush[table_name] = time.time()
        
        # Release lock during actual insertion to avoid blocking other threads
        self._lock.release()
        try:
            # Use the ClickHouseService's bulk_insert method
            self.clickhouse.bulk_insert(table_name, batch, batch_size=len(batch))
            logger.debug(f"Flushed {len(batch)} records to {table_name}")
        except Exception as e:
            logger.error(f"Error flushing batch to {table_name}: {e}")
            # Re-acquire lock before modifying queue
            self._lock.acquire()
            # On error, put the data back in the queue
            self.insertion_queues[table_name].extend(batch)
            raise
        finally:
            # Re-acquire lock if we don't have it
            if not self._lock.locked():
                self._lock.acquire()
    
    def flush_all(self):
        """Synchronously flush all pending insertions (thread-safe)."""
        with self._lock:
            tables_to_flush = list(self.insertion_queues.keys())
            
            for table_name in tables_to_flush:
                if self.insertion_queues[table_name]:
                    self._flush_table(table_name)
    
    def get_queue_sizes(self) -> Dict[str, int]:
        """Get current queue sizes for monitoring."""
        with self._lock:
            return {table: len(queue) for table, queue in self.insertion_queues.items()}
    
    def stop(self):
        """Stop the service and flush remaining data."""
        self._stop_event.set()
        self.flush_all()
        logger.info("Bulk insertion service stopped")