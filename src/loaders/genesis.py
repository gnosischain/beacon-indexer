import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader
from src.utils.logger import logger

class GenesisLoader(BaseLoader):
    """Loader for genesis information. Keeps JSON payload (small data)."""
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("genesis", beacon_api, clickhouse)
    
    async def fetch_data(self, _: Any = None) -> Optional[Dict[str, Any]]:
        """Fetch genesis data from beacon API."""
        return await self.beacon_api.get_genesis()
    
    def prepare_row(self, _: Any, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare genesis data for database insertion."""
        return {
            "payload": data,  # Keep as dict for JSON column
            "retrieved_at": datetime.now()
        }
    
    async def load_single(self, identifier: Any) -> bool:
        """Load genesis data and populate the genesis table."""
        try:
            # First check if we already have processed genesis data
            existing_genesis = self.clickhouse.execute("SELECT COUNT(*) as count FROM genesis")
            if existing_genesis and existing_genesis[0]["count"] > 0:
                logger.info("Genesis data already exists, skipping")
                return True
            
            # Check if we have raw genesis data to process first
            raw_genesis = self.clickhouse.execute("SELECT payload FROM raw_genesis ORDER BY retrieved_at DESC LIMIT 1")
            
            if raw_genesis:
                # Process existing raw data (already parsed as dict from JSON column)
                logger.info("Processing existing raw genesis data")
                data = raw_genesis[0]["payload"]
            else:
                # Fetch new data
                logger.info("Fetching new genesis data from API")
                data = await self.fetch_data(identifier)
                if data is None:
                    return False
                
                # Store raw data
                row = self.prepare_row(identifier, data)
                self.store_data([row])
            
            # Process into structured tables
            if "data" in data:
                genesis_data = data["data"]
                
                # Parse the genesis time - handle both Unix timestamp and ISO format
                genesis_time_str = genesis_data.get("genesis_time", "")
                if not genesis_time_str:
                    logger.error("No genesis_time found in genesis data")
                    return False
                
                try:
                    # Check if it's a Unix timestamp (numeric string)
                    if genesis_time_str.isdigit():
                        # Unix timestamp
                        genesis_time_unix = int(genesis_time_str)
                        genesis_time_dt = datetime.fromtimestamp(genesis_time_unix)
                        logger.info("Parsed Unix timestamp genesis time", 
                                   genesis_time_str=genesis_time_str,
                                   genesis_time_unix=genesis_time_unix,
                                   genesis_time_dt=genesis_time_dt)
                    else:
                        # ISO format string
                        if genesis_time_str.endswith("Z"):
                            genesis_time_str = genesis_time_str[:-1] + "+00:00"
                        genesis_time_dt = datetime.fromisoformat(genesis_time_str)
                        genesis_time_unix = int(genesis_time_dt.timestamp())
                        logger.info("Parsed ISO format genesis time", 
                                   genesis_time_str=genesis_time_str,
                                   genesis_time_unix=genesis_time_unix,
                                   genesis_time_dt=genesis_time_dt)
                        
                except Exception as e:
                    logger.error("Error parsing genesis time", error=str(e), genesis_time_str=genesis_time_str)
                    return False
                
                # Insert into genesis table
                genesis_row = {
                    "genesis_time": genesis_time_dt.replace(tzinfo=None),
                    "genesis_validators_root": genesis_data.get("genesis_validators_root", ""),
                    "genesis_fork_version": genesis_data.get("genesis_fork_version", "")
                }
                self.clickhouse.insert_batch("genesis", [genesis_row])
                
                logger.info("Genesis data loaded successfully", 
                           genesis_time_unix=genesis_time_unix,
                           genesis_time_dt=genesis_time_dt)
            else:
                logger.error("No 'data' field in genesis response")
                return False
            
            return True
            
        except Exception as e:
            logger.error("Failed to load genesis data", error=str(e))
            import traceback
            logger.error("Genesis loader traceback", traceback=traceback.format_exc())
            return False