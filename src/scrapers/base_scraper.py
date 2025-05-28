from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, scraper_id: str, beacon_api, clickhouse, one_time: bool = False):
        self.scraper_id = scraper_id
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.one_time = one_time
        self.specs_manager = None
    
    def set_specs_manager(self, specs_manager) -> None:
        """Set the specs manager instance."""
        self.specs_manager = specs_manager
    
    def get_slots_per_epoch(self) -> int:
        """Get slots per epoch from specs."""
        if self.specs_manager:
            return self.specs_manager.get_slots_per_epoch()
        else:
            # Fallback to getting from clickhouse directly
            time_params = self.clickhouse.get_time_parameters()
            return time_params.get('slots_per_epoch', 32)
    
    def get_seconds_per_slot(self) -> int:
        """Get seconds per slot from specs."""
        if self.specs_manager:
            return self.specs_manager.get_seconds_per_slot()
        else:
            # Fallback to getting from clickhouse directly
            time_params = self.clickhouse.get_time_parameters()
            return time_params.get('seconds_per_slot', 12)
    
    @abstractmethod
    async def process(self, block_data: Dict) -> None:
        """
        Process a block and store relevant data.
        Each scraper implementation should extract its relevant data from the block.
        """
        pass
    
    async def get_last_processed_slot(self) -> int:
        """Get the last processed slot for this scraper."""
        return self.clickhouse.get_last_processed_slot(self.scraper_id)
    
    async def update_scraper_state(self, last_processed_slot: int, mode: str) -> None:
        """Update the state of this scraper."""
        self.clickhouse.update_scraper_state(
            scraper_id=self.scraper_id,
            last_processed_slot=last_processed_slot,
            mode=mode
        )
        
    async def should_process(self) -> bool:
        """
        Determine if the scraper should process a block.
        One-time scrapers should only process if they haven't run before.
        """
        if not self.one_time:
            return True
            
        # For one-time scrapers, check if we've already run
        last_slot = await self.get_last_processed_slot()
        return last_slot == 0  # Only process if we haven't processed anything yet