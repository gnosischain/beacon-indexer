"""Operation types and modes for the indexer."""
from enum import Enum
from typing import Protocol, Dict, Any, List, Optional, TYPE_CHECKING
from abc import ABC, abstractmethod

from src.utils.logger import logger

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from src.services.beacon_api_service import BeaconAPIService
    from src.services.clickhouse_service import ClickHouseService
    from src.services.state_manager import StateManager
    from src.scrapers.base_scraper import BaseScraper


class OperationType(Enum):
    """Types of operations the indexer can perform."""
    CONTINUOUS = "continuous"
    HISTORICAL = "historical"
    FILL_GAPS = "fill_gaps"
    VALIDATE = "validate"
    BACKFILL = "backfill"
    CONSOLIDATE = "consolidate"
    PROCESS_FAILED = "process_failed"


class OperationMode(ABC):
    """Base class for operation modes."""
    
    def __init__(
        self,
        beacon_api: 'BeaconAPIService',
        clickhouse: 'ClickHouseService',
        state_manager: 'StateManager',
        scrapers: List['BaseScraper'],
        config: Dict[str, Any]
    ):
        self.beacon_api = beacon_api
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.scrapers = scrapers
        self.config = config
        
    @abstractmethod
    async def execute(self) -> Dict[str, Any]:
        """Execute the operation and return results."""
        pass
        
    @abstractmethod
    def validate_config(self) -> None:
        """Validate operation-specific configuration."""
        pass