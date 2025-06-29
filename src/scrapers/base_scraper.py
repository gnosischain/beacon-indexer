from abc import ABC, abstractmethod
from typing import Dict, Set, Optional
from src.services.state_manager import StateManager


class BaseScraper(ABC):
    """Base class for all scrapers."""
    
    def __init__(self, scraper_id: str, beacon_api, clickhouse, one_time: bool = False):
        self.scraper_id = scraper_id
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.one_time = one_time
        self.specs_manager = None
        self.state_manager = None
        self._tables_written = set()
        self._row_counts = {}
        self._target_table = None  # For future table-specific processing
    
    def set_state_manager(self, state_manager: StateManager) -> None:
        """Set the state manager instance."""
        self.state_manager = state_manager
    
    def set_specs_manager(self, specs_manager) -> None:
        """Set the specs manager instance."""
        self.specs_manager = specs_manager
    
    def register_table(self, table_name: str) -> None:
        """Register a table that this scraper writes to."""
        self._tables_written.add(table_name)
        if table_name not in self._row_counts:
            self._row_counts[table_name] = 0
    
    def get_tables_written(self) -> Set[str]:
        """Get the set of tables this scraper writes to."""
        return self._tables_written.copy()
    
    def get_slots_per_epoch(self) -> int:
        """Get slots per epoch from specs."""
        if self.specs_manager:
            return self.specs_manager.get_slots_per_epoch()
        return 32  # Default
    
    def get_seconds_per_slot(self) -> int:
        """Get seconds per slot from specs."""
        if self.specs_manager:
            return self.specs_manager.get_seconds_per_slot()
        return 12  # Default
    
    def reset_row_counts(self) -> None:
        """Reset row counts for all tables."""
        self._row_counts = {table: 0 for table in self._tables_written}
    
    def increment_row_count(self, table_name: str, count: int = 1) -> None:
        """Increment the row count for a table."""
        if table_name not in self._row_counts:
            self._row_counts[table_name] = 0
        self._row_counts[table_name] += count
    
    def get_row_counts(self) -> Dict[str, int]:
        """Get the current row counts."""
        return self._row_counts.copy()
    
    def set_target_table(self, table_name: Optional[str]) -> None:
        """Set a specific table to target for processing (future enhancement)."""
        self._target_table = table_name
    
    @abstractmethod
    async def process(self, block_data: Dict) -> None:
        """Process a block and store relevant data."""
        pass
    
    async def get_last_processed_slot(self) -> int:
        """Get the last processed slot for this scraper."""
        if self.state_manager:
            max_slot = 0
            for table in self._tables_written:
                progress = self.state_manager.get_progress(self.scraper_id, table)
                if progress['max_slot'] > max_slot:
                    max_slot = progress['max_slot']
            return max_slot
        
        # Fallback to old method
        return self.clickhouse.get_last_processed_slot(self.scraper_id)
    
    async def update_scraper_state(self, last_processed_slot: int, mode: str) -> None:
        """Update the state of this scraper (for compatibility)."""
        self.clickhouse.update_scraper_state(
            scraper_id=self.scraper_id,
            last_processed_slot=last_processed_slot,
            mode=mode
        )
    
    async def should_process(self) -> bool:
        """Check if this scraper should process (for one-time scrapers)."""
        if not self.one_time:
            return True
        
        # Check if already processed
        last_slot = await self.get_last_processed_slot()
        return last_slot == 0