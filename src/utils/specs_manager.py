from typing import Dict, Any, Optional
import functools
import time
from src.utils.logger import logger

class SpecsManager:
    """
    A centralized manager for accessing chain specifications.
    Uses caching to avoid frequent database queries.
    """
    
    def __init__(self, clickhouse_service, cache_ttl: int = 300):
        """
        Initialize the SpecsManager with a ClickHouse service.
        
        Args:
            clickhouse_service: ClickHouse service for database access
            cache_ttl: Cache time-to-live in seconds (default: 5 minutes)
        """
        self.clickhouse = clickhouse_service
        self.cache_ttl = cache_ttl
        self._specs_cache = {}
        self._cache_timestamp = 0
        self.refresh_cache()
    
    def refresh_cache(self) -> None:
        """Refresh the specs cache from the database."""
        try:
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
            
            results = self.clickhouse.execute(query)
            
            # Update the cache
            self._specs_cache = {row["parameter_name"]: row["parameter_value"] for row in results}
            self._cache_timestamp = time.time()
            
            # Also cache common parameters in their correct types
            self._seconds_per_slot = int(self._specs_cache.get("SECONDS_PER_SLOT", 12))
            self._slots_per_epoch = int(self._specs_cache.get("SLOTS_PER_EPOCH", 32))
            
            logger.debug(f"Refreshed specs cache, found {len(self._specs_cache)} parameters")
        except Exception as e:
            logger.error(f"Error refreshing specs cache: {e}")
    
    def _check_cache(self) -> None:
        """Check if the cache needs refreshing and refresh if needed."""
        if time.time() - self._cache_timestamp > self.cache_ttl:
            self.refresh_cache()
    
    def get_spec(self, param_name: str, default: Any = None) -> Any:
        """
        Get a specification parameter by name.
        
        Args:
            param_name: The name of the parameter to retrieve
            default: Default value to return if parameter is not found
            
        Returns:
            The parameter value if found, otherwise the default value
        """
        self._check_cache()
        return self._specs_cache.get(param_name, default)
    
    def get_int_spec(self, param_name: str, default: int = 0) -> int:
        """Get a spec parameter as an integer."""
        value = self.get_spec(param_name, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    
    def get_seconds_per_slot(self) -> int:
        """Get the SECONDS_PER_SLOT parameter (cached)."""
        self._check_cache()
        return self._seconds_per_slot
    
    def get_slots_per_epoch(self) -> int:
        """Get the SLOTS_PER_EPOCH parameter (cached)."""
        self._check_cache()
        return self._slots_per_epoch
    
    def get_all_specs(self) -> Dict[str, str]:
        """Get all specification parameters."""
        self._check_cache()
        return self._specs_cache.copy()