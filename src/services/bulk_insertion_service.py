import asyncio
from typing import Dict, List, Any
import time
import random
from src.utils.logger import logger

class BulkInsertionService:
    """
    Service for optimizing ClickHouse insertions by batching and parallelizing inserts.
    """
    
    def __init__(self, clickhouse_service, max_parallel_inserts=5, batch_size=1000):
        self.clickhouse = clickhouse_service
        self.max_parallel_inserts = max_parallel_inserts
        self.batch_size = batch_size
        self.insert_semaphore = asyncio.Semaphore(max_parallel_inserts)
        self.insertion_queues = {}  # One queue per table
        self.processing_tasks = {}  # Tasks for processing queues
    
    def queue_for_insertion(self, table_name: str, data: Dict[str, Any]):
        """Queue a record for bulk insertion."""
        if table_name not in self.insertion_queues:
            self.insertion_queues[table_name] = []
            # Start a processor for this queue
            self.processing_tasks[table_name] = asyncio.create_task(
                self._process_insertion_queue(table_name)
            )
        
        self.insertion_queues[table_name].append(data)
        
        # If we've reached the batch size, signal the processor
        if len(self.insertion_queues[table_name]) >= self.batch_size:
            logger.debug(f"Queue for {table_name} has reached batch size")
    
    async def _process_insertion_queue(self, table_name: str):
        """Process the insertion queue for a specific table."""
        logger.info(f"Started insertion queue processor for table {table_name}")
        
        while True:
            # Check if we have enough items to insert
            if len(self.insertion_queues[table_name]) >= self.batch_size:
                # Extract a batch from the queue
                batch = self.insertion_queues[table_name][:self.batch_size]
                self.insertion_queues[table_name] = self.insertion_queues[table_name][self.batch_size:]
                
                # Insert the batch
                await self._insert_batch(table_name, batch)
            elif self.insertion_queues[table_name]:
                # We have some items but not a full batch, wait a bit before inserting
                await asyncio.sleep(0.5)
                
                # Insert whatever we have
                batch = self.insertion_queues[table_name]
                self.insertion_queues[table_name] = []
                
                await self._insert_batch(table_name, batch)
            else:
                # Empty queue, wait before checking again
                await asyncio.sleep(0.1)
    
    async def _insert_batch(self, table_name: str, batch: List[Dict[str, Any]]):
        """Insert a batch of records into ClickHouse."""
        async with self.insert_semaphore:
            try:
                start_time = time.time()
                
                # Use execute_many for efficiency
                self.clickhouse.execute_many(f"INSERT INTO {table_name}", batch)
                
                elapsed = time.time() - start_time
                logger.info(f"Inserted {len(batch)} records into {table_name} in {elapsed:.2f}s ({len(batch)/elapsed:.2f} records/s)")
            except Exception as e:
                logger.error(f"Error inserting batch into {table_name}: {e}")
                
                # Retry with smaller batches if needed
                if len(batch) > 1:
                    mid = len(batch) // 2
                    logger.info(f"Retrying with smaller batches of {mid} and {len(batch) - mid} records")
                    await self._insert_batch(table_name, batch[:mid])
                    await self._insert_batch(table_name, batch[mid:])
    
    async def flush_all_queues(self):
        """Flush all insertion queues."""
        for table_name in list(self.insertion_queues.keys()):
            if self.insertion_queues[table_name]:
                batch = self.insertion_queues[table_name]
                self.insertion_queues[table_name] = []
                await self._insert_batch(table_name, batch)
        
        # Wait for all processing tasks to complete
        for task in self.processing_tasks.values():
            task.cancel()
        
        # Wait for all tasks to be properly cancelled
        for task in self.processing_tasks.values():
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        logger.info("All insertion queues flushed")