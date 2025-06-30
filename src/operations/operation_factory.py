"""Factory for creating operation instances."""
from typing import Dict, Any, List, Type, TYPE_CHECKING

from src.config import OperationType
from src.core.operations import OperationMode
from src.utils.logger import logger

if TYPE_CHECKING:
    from src.services.beacon_api_service import BeaconAPIService
    from src.services.clickhouse_service import ClickHouseService
    from src.services.state_manager import StateManager
    from src.scrapers.base_scraper import BaseScraper


class OperationFactory:
    """Factory for creating operation mode instances."""
    
    _operation_map: Dict[OperationType, Type[OperationMode]] = {}
    _initialized = False
    
    @classmethod
    def _lazy_init(cls):
        """Lazy initialization to avoid circular imports."""
        if cls._initialized:
            return
            
        # Import operation classes only when needed
        from .continuous_operation import ContinuousOperation
        from .historical_operation import HistoricalOperation
        from .fill_gaps_operation import FillGapsOperation
        from .validate_operation import ValidateOperation
        from .backfill_operation import BackfillOperation
        from .consolidate_operation import ConsolidateOperation
        from .process_failed_operation import ProcessFailedOperation
        
        cls._operation_map = {
            OperationType.CONTINUOUS: ContinuousOperation,
            OperationType.HISTORICAL: HistoricalOperation,
            OperationType.FILL_GAPS: FillGapsOperation,
            OperationType.VALIDATE: ValidateOperation,
            OperationType.BACKFILL: BackfillOperation,
            OperationType.CONSOLIDATE: ConsolidateOperation,
            OperationType.PROCESS_FAILED: ProcessFailedOperation,
        }
        cls._initialized = True
    
    @classmethod
    def create(
        cls,
        operation_type: OperationType,
        beacon_api: 'BeaconAPIService',
        clickhouse: 'ClickHouseService',
        state_manager: 'StateManager',
        scrapers: List['BaseScraper'],
        config: Dict[str, Any]
    ) -> OperationMode:
        """Create an operation mode instance."""
        cls._lazy_init()
        
        operation_class = cls._operation_map.get(operation_type)
        if not operation_class:
            raise ValueError(f"Unknown operation type: {operation_type}")
            
        logger.info(f"Creating operation mode: {operation_type.value}")
        return operation_class(
            beacon_api=beacon_api,
            clickhouse=clickhouse,
            state_manager=state_manager,
            scrapers=scrapers,
            config=config
        )