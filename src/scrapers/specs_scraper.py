from typing import Dict, Optional, Tuple, Any

from src.scrapers.base_scraper import BaseScraper
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger
import traceback

class SpecsScraper(BaseScraper):
    """Scraper for chain specifications - runs as a one-time process only."""
    
    def __init__(self, beacon_api: BeaconAPIService, clickhouse: ClickHouseService):
        super().__init__("specs_scraper", beacon_api, clickhouse, one_time=True)
        self.current_specs = {}
        self.register_table("specs")
        
    async def _load_current_specs(self) -> Dict[str, str]:
        """Load current specs from the database."""
        query = """
        WITH latest_specs AS (
            SELECT 
                parameter_name,
                argMax(parameter_value, updated_at) as parameter_value
            FROM specs
            GROUP BY parameter_name
        )
        SELECT parameter_name, parameter_value FROM latest_specs
        """
        
        try:
            results = self.clickhouse.execute(query)
            # Ensure results is a list
            if hasattr(results, '__iter__') and not isinstance(results, list):
                results = list(results)
            
            self.current_specs = {row["parameter_name"]: row["parameter_value"] for row in results}
            return self.current_specs
        except Exception as e:
            logger.error(f"Error loading current specs from database: {e}")
            logger.debug(traceback.format_exc())
            return {}
    
    async def _fetch_specs(self) -> Dict[str, any]:
        """Fetch current chain specs from the beacon node."""
        try:
            response = await self.beacon_api._make_request("GET", "/eth/v1/config/spec")
            specs_data = response.get("data", {})
            return specs_data
        except Exception as e:
            logger.error(f"Error fetching chain specs: {e}")
            logger.debug(traceback.format_exc())
            return {}
    
    async def has_data(self) -> bool:
        """Check if the specs table already has data."""
        try:
            query = "SELECT COUNT(*) as count FROM specs"
            results = self.clickhouse.execute(query)
            count = results[0]["count"] if results else 0
            return count > 0
        except Exception as e:
            logger.error(f"Error checking specs table: {e}")
            return False
            
    async def process(self, block_data: Optional[Dict] = None) -> Tuple[int, int]:
        """
        Process chain specs. This scraper doesn't need block data, 
        as it fetches specs directly from the beacon API.
        
        Returns:
            Tuple[int, int]: (seconds_per_slot, slots_per_epoch)
        """
        # Default values
        seconds_per_slot = 12
        slots_per_epoch = 32
        
        try:
            # Check if we've already processed specs before
            has_specs = await self.has_data()
            already_ran = await self.get_last_processed_slot() > 0
            
            # If we have specs and we've already run once, just load values from database
            if has_specs and already_ran:
                logger.info("Specs already exist and scraper has run before - using stored values")
                await self._load_current_specs()
                if "SECONDS_PER_SLOT" in self.current_specs:
                    seconds_per_slot = int(self.current_specs["SECONDS_PER_SLOT"])
                if "SLOTS_PER_EPOCH" in self.current_specs:
                    slots_per_epoch = int(self.current_specs["SLOTS_PER_EPOCH"])
                
                return seconds_per_slot, slots_per_epoch
            
            # Otherwise, load current specs from database for comparison
            await self._load_current_specs()
            
            # Fetch latest specs from beacon node
            new_specs = await self._fetch_specs()
            
            if not new_specs:
                logger.warning("Failed to fetch specs or received empty specs")
                return seconds_per_slot, slots_per_epoch
            
            # Extract time specs for return values
            if "SECONDS_PER_SLOT" in new_specs:
                seconds_per_slot = int(new_specs["SECONDS_PER_SLOT"])
            if "SLOTS_PER_EPOCH" in new_specs:
                slots_per_epoch = int(new_specs["SLOTS_PER_EPOCH"])
            
            # Check for changes and update only those that have changed
            params_to_update = []
            for name, value in new_specs.items():
                str_value = str(value)
                if name not in self.current_specs or self.current_specs[name] != str_value:
                    params_to_update.append({
                        "parameter_name": name,
                        "parameter_value": str_value
                    })
            
            if params_to_update:
                # Use direct SQL insert for better control
                for param in params_to_update:
                    try:
                        query = """
                        INSERT INTO specs (parameter_name, parameter_value) VALUES
                        (%(parameter_name)s, %(parameter_value)s)
                        """
                        self.clickhouse.execute(query, param)
                    except Exception as e:
                        logger.error(f"Error inserting spec {param['parameter_name']}: {e}")
                
                logger.info(f"Updated {len(params_to_update)} chain spec parameters")
                
                # Log time specs if they changed
                if "SECONDS_PER_SLOT" in [p["parameter_name"] for p in params_to_update] or \
                   "SLOTS_PER_EPOCH" in [p["parameter_name"] for p in params_to_update]:
                    logger.info(f"Updated time specs: seconds_per_slot={seconds_per_slot}, slots_per_epoch={slots_per_epoch}")
            else:
                logger.info("No spec changes detected")
                
            # Update scraper state to mark as run
            await self.update_scraper_state(1, "one_time")
            
            return seconds_per_slot, slots_per_epoch
            
        except Exception as e:
            logger.error(f"Error in specs scraper: {e}")
            logger.debug(traceback.format_exc())
            return seconds_per_slot, slots_per_epoch