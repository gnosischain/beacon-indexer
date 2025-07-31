import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from .base import BaseLoader
from src.config import config
from src.utils.logger import logger

class ValidatorsLoader(BaseLoader):
    """Loader for validator data with String payload storage and support for daily snapshots or all slots."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("validators", beacon_api, clickhouse)
        self.mode = config.VALIDATOR_MODE  # 'daily' or 'all_slots'
        # Cache timing parameters at instance level
        self._cached_genesis_time = None
        self._cached_seconds_per_slot = None
        self._cache_initialized = False
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch validators data from beacon API using slot number."""
        return await self.beacon_api.get_validators(str(slot))  # Use slot as state_id
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare validators data for database insertion with String payload."""
        return {
            "slot": slot,  # Direct slot value
            "payload": json.dumps(data),  # Convert dict to JSON string
            "retrieved_at": datetime.now()
        }
    
    async def load_single(self, slot: int) -> bool:
        """Load a single slot - overridden to handle slot parameter directly."""
        try:
            # Check if this slot should be processed
            if not self.should_process_slot(slot):
                logger.debug("Skipping slot for validator loader", slot=slot, mode=self.mode)
                return True  # Not an error, just skipped
            
            data = await self.fetch_data(slot)
            if data is None:
                logger.debug("No validator data found", loader=self.name, slot=slot)
                return True  # Empty slots are normal
            
            row = self.prepare_row(slot, data)
            self.store_data([row])
            return True
            
        except Exception as e:
            logger.error("Failed to load validator data", 
                        loader=self.name, 
                        slot=slot, 
                        error=str(e))
            return False
    
    def _init_timing_cache(self):
        """Initialize timing parameters cache from database."""
        if self._cache_initialized:
            return True
            
        try:
            # Get timing parameters from time_helpers
            time_helpers_query = """
            SELECT genesis_time_unix, seconds_per_slot 
            FROM time_helpers FINAL 
            ORDER BY genesis_time_unix DESC 
            LIMIT 1
            """
            time_helpers_result = self.clickhouse.execute(time_helpers_query)
            if time_helpers_result and len(time_helpers_result) > 0:
                self._cached_genesis_time = time_helpers_result[0].get("genesis_time_unix")
                self._cached_seconds_per_slot = time_helpers_result[0].get("seconds_per_slot")
                logger.debug("Cached timing from time_helpers", 
                           genesis_time=self._cached_genesis_time,
                           seconds_per_slot=self._cached_seconds_per_slot)
            
            # Check if we have both parameters
            if self._cached_genesis_time and self._cached_seconds_per_slot:
                self._cache_initialized = True
                logger.info("Validators loader timing cache initialized", 
                           genesis_time=self._cached_genesis_time,
                           seconds_per_slot=self._cached_seconds_per_slot)
                return True
            else:
                logger.warning("Incomplete timing parameters", 
                             genesis_time=self._cached_genesis_time,
                             seconds_per_slot=self._cached_seconds_per_slot)
                return False
                
        except Exception as e:
            logger.error("Error initializing timing cache", error=str(e))
            return False
    
    def should_process_slot(self, slot: int) -> bool:
        """
        Determine if this slot should be processed by the validator loader.
        
        Returns:
            bool: True if the slot should be processed
        """
        if self.mode == "all_slots":
            return True
        elif self.mode == "daily":
            # Timing parameters should ALWAYS be available at this point
            if not self._init_timing_cache():
                raise Exception(f"Timing parameters not available for slot {slot}. This indicates a critical setup failure.")
            return self._is_target_slot_for_daily_mode(slot)
        else:
            logger.warning("Unknown validator mode, defaulting to daily", mode=self.mode)
            return self._is_target_slot_for_daily_mode(slot)
    
    def _is_target_slot_for_daily_mode(self, slot: int) -> bool:
        """Check if slot is the last slot of its day (for daily mode)."""
        try:
            # Timing parameters should be available at this point - if not, it's a critical error
            if not self._init_timing_cache():
                raise Exception(f"Cannot determine timing parameters for slot {slot}. Foundation data is missing.")
            
            return self._is_last_slot_of_day(slot, self._cached_genesis_time, self._cached_seconds_per_slot)
            
        except Exception as e:
            logger.error("Error checking if slot should be processed", slot=slot, error=str(e))
            raise
    
    def _is_last_slot_of_day(self, slot: int, genesis_time: int, seconds_per_slot: int) -> bool:
        """
        Check if the given slot is the last slot of its day in UTC.
        """
        from datetime import datetime, timezone
        
        try:
            # Get timestamp for current slot
            slot_timestamp = genesis_time + (slot * seconds_per_slot)
            slot_time = datetime.fromtimestamp(slot_timestamp, tz=timezone.utc)
            
            # Get timestamp for next slot
            next_slot_timestamp = genesis_time + ((slot + 1) * seconds_per_slot)
            next_slot_time = datetime.fromtimestamp(next_slot_timestamp, tz=timezone.utc)
            
            # If they're on different days, this is the last slot of the day
            return slot_time.date() != next_slot_time.date()
            
        except Exception as e:
            logger.error("Error checking if slot is last of day", slot=slot, error=str(e))
            return False
    
    def get_target_slots_in_range(self, start_slot: int, end_slot: int) -> List[int]:
        """
        Get all slots in the range that should be processed by this loader.
        Range is [start_slot, end_slot) - start_slot inclusive, end_slot exclusive.
        
        Returns:
            List of slots to process
        """
        if self.mode == "all_slots":
            return list(range(start_slot, end_slot))  # FIXED: end_slot is now exclusive
        elif self.mode == "daily":
            return self._get_daily_target_slots(start_slot, end_slot)
        else:
            logger.warning("Unknown validator mode, defaulting to daily", mode=self.mode)
            return self._get_daily_target_slots(start_slot, end_slot)
    
    def _get_daily_target_slots(self, start_slot: int, end_slot: int) -> List[int]:
        """Get target slots for daily mode (last slot of each day). Range is [start_slot, end_slot)."""
        try:
            # Initialize cache if needed
            if not self._init_timing_cache():
                logger.warning("Cannot determine timing parameters for range", 
                             start=start_slot, end=end_slot)
                return []
            
            target_slots = []
            # FIXED: Make range exclusive of end_slot
            for slot in range(start_slot, end_slot):
                if self._is_last_slot_of_day(slot, self._cached_genesis_time, self._cached_seconds_per_slot):
                    target_slots.append(slot)
            
            logger.debug("Found daily target slots", 
                        count=len(target_slots),
                        first_few=target_slots[:5] if target_slots else [])
            
            return target_slots
            
        except Exception as e:
            logger.error("Error getting daily target slots", 
                        start_slot=start_slot, end_slot=end_slot, error=str(e))
            return []
    
    async def load_batch(self, identifiers: list) -> int:
        """
        Override load_batch to respect validator mode for slot-based loading.
        """
        # Initialize cache before processing
        if not self._cache_initialized:
            self._init_timing_cache()
        
        if self.mode == "all_slots":
            # Process all slots
            rows = []
            success_count = 0
            
            for slot in identifiers:
                try:
                    data = await self.fetch_data(slot)
                    if data is not None:
                        row = self.prepare_row(slot, data)
                        rows.append(row)
                    success_count += 1
                    
                except Exception as e:
                    logger.error("Failed to load single slot in batch", 
                               loader=self.name, 
                               slot=slot, 
                               error=str(e))
            
            if rows:
                self.store_data(rows)
            
            return success_count
        else:
            # Filter slots based on daily mode
            target_slots = []
            for slot in identifiers:
                if self.should_process_slot(slot):
                    target_slots.append(slot)
            
            if target_slots:
                logger.info("Processing validator batch", 
                           mode=self.mode,
                           total_slots=len(identifiers),
                           target_slots=len(target_slots))
                
                rows = []
                success_count = 0
                
                for slot in target_slots:
                    try:
                        data = await self.fetch_data(slot)
                        if data is not None:
                            row = self.prepare_row(slot, data)
                            rows.append(row)
                        success_count += 1
                        
                    except Exception as e:
                        logger.error("Failed to load single slot in batch", 
                                   loader=self.name, 
                                   slot=slot, 
                                   error=str(e))
                
                if rows:
                    self.store_data(rows)
                
                return len(identifiers)  # Return total count, not just processed
            else:
                logger.debug("No target slots found in batch", 
                           mode=self.mode,
                           total_slots=len(identifiers))
                return len(identifiers)  # Return success count