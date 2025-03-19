from typing import Dict, List, Any, Optional
import asyncio

from src.services.beacon_api_service import BeaconAPIService
from src.utils.logger import logger

class BlockProcessor:
    """
    Coordinates block retrieval and distribution to multiple scrapers.
    This reduces redundant API calls by fetching each block once.
    """
    
    def __init__(self, beacon_api: BeaconAPIService):
        self.beacon_api = beacon_api
        self._block_cache = {}  # Cache for recently processed blocks
        self._cache_limit = 100  # Maximum number of blocks to keep in cache
    
    async def get_and_process_block(self, slot: int, processors: List[callable]) -> bool:
        """
        Get a block once and distribute to all processor functions.
        
        Args:
            slot: The slot number to process
            processors: List of processing functions to call with the block data
            
        Returns:
            bool: True if block was found and processed, False otherwise
        """
        # Check cache first
        if slot in self._block_cache:
            block_data = self._block_cache[slot]
        else:
            try:
                # Fetch the block from the API
                block_data = await self.beacon_api.get_block(str(slot))
                
                # Update cache (with LRU-style management)
                if len(self._block_cache) >= self._cache_limit:
                    # Remove oldest item
                    oldest_slot = min(self._block_cache.keys())
                    del self._block_cache[oldest_slot]
                
                self._block_cache[slot] = block_data
                
            except Exception as e:
                if "404" in str(e):
                    logger.warning(f"No block found for slot {slot} (404)")
                else:
                    logger.error(f"Error fetching block for slot {slot}: {e}")
                return False
        
        # Process with all processor functions
        for processor in processors:
            try:
                await processor(block_data)
            except Exception as e:
                # Get the name of the processor function for better error reporting
                processor_name = getattr(processor, '__self__', None)
                if processor_name:
                    processor_name = processor_name.scraper_id
                else:
                    processor_name = processor.__name__
                
                logger.error(f"Error in processor {processor_name} for slot {slot}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        return True
    
    def clear_cache(self) -> None:
        """Clear the block cache."""
        self._block_cache.clear()