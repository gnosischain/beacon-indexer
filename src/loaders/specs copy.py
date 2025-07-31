import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader
from src.utils.logger import logger

class SpecsLoader(BaseLoader):
    """Loader for chain specifications."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("specs", beacon_api, clickhouse)
    
    async def fetch_data(self, _: Any = None) -> Optional[Dict[str, Any]]:
        """Fetch specs data from beacon API."""
        return await self.beacon_api.get_spec()
    
    def prepare_row(self, _: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare specs data for database insertion."""
        return {
            "payload": data,  # Keep as dict for JSON column
            "retrieved_at": datetime.now()
        }
    
    async def load_single(self, identifier: Any) -> bool:
        """Load specs data and properly construct the time_helpers table."""
        try:
            # First check if we already have processed specs data
            existing_specs = self.clickhouse.execute("SELECT COUNT(*) as count FROM specs")
            if existing_specs and existing_specs[0]["count"] > 0:
                logger.info("Specs data already exists, skipping")
                return True
            
            # Check if we have raw specs data to process first
            raw_specs = self.clickhouse.execute("SELECT payload FROM raw_specs ORDER BY retrieved_at DESC LIMIT 1")
            
            if raw_specs:
                # Process existing raw data (already parsed as dict from JSON column)
                logger.info("Processing existing raw specs data")
                data = raw_specs[0]["payload"]
            else:
                # Fetch new data
                logger.info("Fetching new specs data from API")
                data = await self.fetch_data(identifier)
                if data is None:
                    return False
                
                # Store raw data
                row = self.prepare_row(identifier, data)
                self.store_data([row])
            
            # Process into structured tables
            if "data" in data:
                specs_data = data["data"]
                specs_rows = []
                
                # Extract important timing parameters
                for param_name, param_value in specs_data.items():
                    specs_rows.append({
                        "parameter_name": param_name,
                        "parameter_value": str(param_value),
                        "updated_at": datetime.now()
                    })
                
                if specs_rows:
                    self.clickhouse.insert_batch("specs", specs_rows)
                
                # Extract timing parameters
                seconds_per_slot = int(specs_data.get("SECONDS_PER_SLOT", 12))
                slots_per_epoch = int(specs_data.get("SLOTS_PER_EPOCH", 32))
                
                # Get genesis time from genesis table
                try:
                    genesis_query = "SELECT toUnixTimestamp(genesis_time) as genesis_time_unix FROM genesis LIMIT 1"
                    genesis_result = self.clickhouse.execute(genesis_query)
                    if genesis_result and len(genesis_result) > 0:
                        genesis_time_unix = int(genesis_result[0]["genesis_time_unix"])
                        logger.info("Retrieved genesis time from genesis table", 
                                   genesis_time_unix=genesis_time_unix)
                    else:
                        logger.error("No genesis time found in genesis table")
                        return False
                except Exception as e:
                    logger.error("Error retrieving genesis time", error=str(e))
                    return False
                
                # Clear existing time_helpers and insert the correct single row
                try:
                    self.clickhouse.client.command("TRUNCATE TABLE time_helpers")
                    logger.info("Cleared existing time_helpers table")
                except Exception as e:
                    logger.warning("Could not truncate time_helpers", error=str(e))
                
                # Insert the single correct time_helpers row
                time_helpers_row = {
                    "genesis_time_unix": genesis_time_unix,
                    "seconds_per_slot": seconds_per_slot,
                    "slots_per_epoch": slots_per_epoch
                }
                self.clickhouse.insert_batch("time_helpers", [time_helpers_row])
                
                logger.info("Time helpers table constructed correctly", 
                           genesis_time_unix=genesis_time_unix,
                           seconds_per_slot=seconds_per_slot,
                           slots_per_epoch=slots_per_epoch)
            
            return True
            
        except Exception as e:
            logger.error("Failed to load specs data", error=str(e))
            return False