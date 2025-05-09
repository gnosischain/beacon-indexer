from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

class Committee(BaseModel):
    """Model representing a committee."""
    
    slot: int
    epoch: int
    committee_slot: int
    committee_index: int
    validators: List[int]
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, epoch: int, committee_data: Dict[str, Any]) -> "Committee":
        """Create a Committee from API response data."""
        committee_slot = int(committee_data.get("slot", 0))
        committee_index = int(committee_data.get("index", 0))
        validators = [int(v) for v in committee_data.get("validators", [])]
        
        return cls(
            slot=slot,
            epoch=epoch,
            committee_slot=committee_slot,
            committee_index=committee_index,
            validators=validators
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})

class SyncCommittee(BaseModel):
    """Model representing a sync committee."""
    
    slot: int
    epoch: int
    validators: List[int]
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, epoch: int, committee_data: Dict[str, Any]) -> "SyncCommittee":
        """Create a SyncCommittee from API response data."""
        validators = [int(v) for v in committee_data.get("validators", [])]
        
        return cls(
            slot=slot,
            epoch=epoch,
            validators=validators
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})