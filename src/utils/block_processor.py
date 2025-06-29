from typing import Dict, List, Any, Optional
import asyncio
import random

from src.services.beacon_api_service import BeaconAPIService
from src.utils.logger import logger

class BlockProcessor:
    """
    Coordinates block retrieval and distribution to multiple scrapers.
    This reduces redundant API calls by fetching each block once.
    With optimizations for high-throughput batch processing and improved retry logic.
    """
    
    def __init__(self, beacon_api: BeaconAPIService):
        self.beacon_api = beacon_api
        self._block_cache = {}  # Cache for recently processed blocks
        self._cache_limit = 1000  # Maximum number of blocks to keep in cache
        self._block_semaphore = asyncio.Semaphore(100)  # Limit concurrent block fetches
    
    async def get_and_process_block(self, slot: int, processors: List[callable], max_retries: int = 3) -> bool:
        """
        Get a block once and distribute to all processor functions with retry logic.
        
        Args:
            slot: The slot number to process
            processors: List of processing functions to call with the block data
            max_retries: Maximum number of retries for failed requests
            
        Returns:
            bool: True if block was found and processed, False otherwise
        """
        # Check cache first
        if slot in self._block_cache:
            block_data = self._block_cache[slot]
        else:
            block_data = None
            last_error = None
            
            # Retry logic for fetching the block
            for attempt in range(max_retries + 1):
                try:
                    # Use a semaphore to limit concurrent fetches
                    async with self._block_semaphore:
                        # Fetch the block from the API
                        block_data = await self.beacon_api.get_block(str(slot))
                    
                    # Update cache (with LRU-style management)
                    if len(self._block_cache) >= self._cache_limit:
                        # Remove oldest item
                        oldest_slot = min(self._block_cache.keys())
                        del self._block_cache[oldest_slot]
                    
                    self._block_cache[slot] = block_data
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    last_error = e
                    
                    if "404" in str(e):
                        # No block at this slot, just skip it (don't retry 404s)
                        logger.debug(f"No block found at slot {slot} (404)")
                        return False
                    
                    # Check if this is a retryable error
                    is_retryable = any(error_type in str(e).lower() for error_type in [
                        "server disconnected", "connection", "timeout", "network", 
                        "temporary", "rate limit", "503", "502", "500"
                    ])
                    
                    if not is_retryable:
                        logger.error(f"Non-retryable error fetching block for slot {slot}: {e}")
                        return False
                    
                    if attempt < max_retries:
                        # Calculate backoff delay with jitter
                        delay = min(2 ** attempt + random.uniform(0, 1), 30)
                        logger.warning(f"Error fetching block for slot {slot} (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {delay:.1f}s")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Failed to fetch block for slot {slot} after {max_retries + 1} attempts: {last_error}")
                        return False
        
        # If we still don't have block data, return False
        if not block_data:
            return False
        
        # Process with all processor functions
        tasks = []
        for processor in processors:
            try:
                # Create a task for each processor
                task = asyncio.create_task(processor(block_data))
                tasks.append(task)
            except Exception as e:
                # Get the name of the processor function for better error reporting
                processor_name = getattr(processor, '__self__', None)
                if processor_name:
                    processor_name = processor_name.scraper_id
                else:
                    processor_name = processor.__name__
                
                logger.error(f"Error in processor {processor_name} for slot {slot}: {e}")
        
        # Wait for all processing tasks to complete
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log any processor errors
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processor_name = getattr(processors[i], '__self__', processors[i]).__class__.__name__
                    logger.error(f"Processor {processor_name} failed for slot {slot}: {result}")
        
        return True
    
    async def get_and_process_blocks_batch(self, slots: List[int], processors: List[callable]) -> int:
        """
        Process a batch of slots with optimized parallel fetching.
        
        Args:
            slots: List of slot numbers to process
            processors: List of processing functions to apply
            
        Returns:
            int: Number of slots successfully processed
        """
        # Create tasks for each slot
        tasks = [self.get_and_process_block(slot, processors) for slot in slots]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful slots
        success_count = sum(1 for r in results if r is True)
        
        return success_count
    
    def clear_cache(self) -> None:
        """Clear the block cache."""
        self._block_cache.clear()