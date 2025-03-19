from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

class Attestation(BaseModel):
    """Model representing a beacon chain attestation."""
    
    slot: int
    block_root: str
    attestation_slot: int
    attestation_index: int
    aggregation_bits: str
    committee_bits: Optional[str] = None  # For Electra version
    beacon_block_root: str
    source_epoch: int
    source_root: str
    target_epoch: int
    target_root: str
    signature: str
    validators: List[int] = Field(default_factory=list)
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, attestation_data: Dict[str, Any], version: str) -> "Attestation":
        """Create an Attestation from API response data."""
        aggregation_bits = attestation_data.get("aggregation_bits", "")
        committee_bits = attestation_data.get("committee_bits", "") if version == "electra" else None
        signature = attestation_data.get("signature", "")
        
        data = attestation_data.get("data", {})
        attestation_slot = int(data.get("slot", 0))
        attestation_index = int(data.get("index", 0))
        beacon_block_root = data.get("beacon_block_root", "")
        
        source = data.get("source", {})
        source_epoch = int(source.get("epoch", 0))
        source_root = source.get("root", "")
        
        target = data.get("target", {})
        target_epoch = int(target.get("epoch", 0))
        target_root = target.get("root", "")
        
        # In a real implementation, you would convert aggregation bits to validator indices
        validators = []
        
        return cls(
            slot=slot,
            block_root=block_root,
            attestation_slot=attestation_slot,
            attestation_index=attestation_index,
            aggregation_bits=aggregation_bits,
            committee_bits=committee_bits,
            beacon_block_root=beacon_block_root,
            source_epoch=source_epoch,
            source_root=source_root,
            target_epoch=target_epoch,
            target_root=target_root,
            signature=signature,
            validators=validators
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        result = self.dict(exclude={"slot_timestamp", "month"})
        return result