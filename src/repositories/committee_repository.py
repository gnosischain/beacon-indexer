from typing import List, Optional, Dict, Any

from src.models.committee import Committee, SyncCommittee
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class CommitteeRepository:
    """Repository for handling committee data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, committee: Committee) -> None:
        """Save a single committee."""
        query = """
        INSERT INTO committees (
            slot, epoch, committee_slot, committee_index, validators
        ) VALUES (
            %(slot)s, %(epoch)s, %(committee_slot)s, %(committee_index)s, %(validators)s
        )
        """
        
        self.clickhouse.execute(query, committee.to_db_dict())
    
    def save_many(self, committees: List[Committee]) -> None:
        """Save multiple committees."""
        if not committees:
            return
            
        query = """
        INSERT INTO committees (
            slot, epoch, committee_slot, committee_index, validators
        ) VALUES
        """
        
        params = [committee.to_db_dict() for committee in committees]
        self.clickhouse.execute_many(query, params)
    
    def get_by_slot(self, slot: int) -> List[Committee]:
        """Get committees for a specific slot."""
        query = """
        SELECT *
        FROM committees
        WHERE slot = %(slot)s
        ORDER BY committee_index
        """
        
        results = self.clickhouse.execute(query, {"slot": slot})
        
        return [Committee(**row) for row in results]
    
    def get_by_month(self, month: str) -> List[Committee]:
        """Get committees for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM committees
        WHERE month = %(month)s
        ORDER BY slot, committee_index
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [Committee(**row) for row in results]
    
    def get_by_epoch(self, epoch: int) -> List[Committee]:
        """Get committees for a specific epoch."""
        query = """
        SELECT *
        FROM committees
        WHERE epoch = %(epoch)s
        ORDER BY slot, committee_index
        """
        
        results = self.clickhouse.execute(query, {"epoch": epoch})
        
        return [Committee(**row) for row in results]
    
    def get_by_validator(self, validator_index: int, epoch: Optional[int] = None) -> List[Committee]:
        """Get committees containing a specific validator."""
        query = """
        SELECT *
        FROM committees
        WHERE hasElement(validators, %(validator_index)s) = 1
        """
        
        params = {"validator_index": validator_index}
        
        if epoch is not None:
            query += " AND epoch = %(epoch)s"
            params["epoch"] = epoch
            
        query += " ORDER BY epoch, slot, committee_index"
        
        results = self.clickhouse.execute(query, params)
        
        return [Committee(**row) for row in results]


class SyncCommitteeRepository:
    """Repository for handling sync committee data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, sync_committee: SyncCommittee) -> None:
        """Save a single sync committee."""
        query = """
        INSERT INTO sync_committees (
            slot, epoch, validators
        ) VALUES (
            %(slot)s, %(epoch)s, %(validators)s
        )
        """
        
        self.clickhouse.execute(query, sync_committee.to_db_dict())
    
    def save_many(self, sync_committees: List[SyncCommittee]) -> None:
        """Save multiple sync committees."""
        if not sync_committees:
            return
            
        query = """
        INSERT INTO sync_committees (
            slot, epoch, validators
        ) VALUES
        """
        
        params = [committee.to_db_dict() for committee in sync_committees]
        self.clickhouse.execute_many(query, params)
    
    def get_by_month(self, month: str) -> List[SyncCommittee]:
        """Get sync committees for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM sync_committees
        WHERE month = %(month)s
        ORDER BY epoch, slot
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [SyncCommittee(**row) for row in results]
    
    def get_by_epoch(self, epoch: int) -> Optional[SyncCommittee]:
        """Get sync committee for a specific epoch."""
        query = """
        SELECT *
        FROM sync_committees
        WHERE epoch = %(epoch)s
        ORDER BY slot DESC
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"epoch": epoch})
        
        if results:
            return SyncCommittee(**results[0])
        return None
    
    def get_by_validator(self, validator_index: int) -> List[SyncCommittee]:
        """Get sync committees containing a specific validator."""
        query = """
        SELECT *
        FROM sync_committees
        WHERE hasElement(validators, %(validator_index)s) = 1
        ORDER BY epoch
        """
        
        results = self.clickhouse.execute(query, {"validator_index": validator_index})
        
        return [SyncCommittee(**row) for row in results]