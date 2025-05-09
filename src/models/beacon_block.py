from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

class BeaconBlock(BaseModel):
    """Model representing a beacon chain block."""
    
    slot: int
    block_root: str
    parent_root: str
    state_root: str
    proposer_index: int
    eth1_block_hash: str
    eth1_deposit_root: str
    eth1_deposit_count: int
    graffiti: str
    signature: str
    randao_reveal: str
    timestamp: Optional[datetime] = None
    is_canonical: bool = True
    fork_version: str
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, block_data: Dict[str, Any], block_root: str) -> "BeaconBlock":
        """Create a BeaconBlock from API response data."""
        version = block_data.get("version", "phase0")
        data = block_data.get("data", {})
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Extract basic block information
        slot = int(message.get("slot", 0))
        proposer_index = int(message.get("proposer_index", 0))
        parent_root = message.get("parent_root", "")
        state_root = message.get("state_root", "")
        signature = data.get("signature", "")
        
        # Extract eth1 data
        eth1_data = body.get("eth1_data", {})
        eth1_block_hash = eth1_data.get("block_hash", "")
        eth1_deposit_root = eth1_data.get("deposit_root", "")
        eth1_deposit_count = int(eth1_data.get("deposit_count", 0))
        
        # Other block data
        graffiti = body.get("graffiti", "")
        randao_reveal = body.get("randao_reveal", "")
        
        # Get timestamp from execution payload if available
        timestamp = None
        if version in ["bellatrix", "capella", "deneb", "electra"]:
            execution_payload = body.get("execution_payload", {})
            if execution_payload:
                timestamp_val = int(execution_payload.get("timestamp", 0))
                if timestamp_val > 0:
                    timestamp = datetime.fromtimestamp(timestamp_val)
        
        return cls(
            slot=slot,
            block_root=block_root,
            parent_root=parent_root,
            state_root=state_root,
            proposer_index=proposer_index,
            eth1_block_hash=eth1_block_hash,
            eth1_deposit_root=eth1_deposit_root,
            eth1_deposit_count=eth1_deposit_count,
            graffiti=graffiti,
            signature=signature,
            randao_reveal=randao_reveal,
            timestamp=timestamp,
            is_canonical=True,
            fork_version=version
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        result = self.dict(exclude={"slot_timestamp"})
        # Convert datetime to timestamp if present
        if self.timestamp:
            result["timestamp"] = self.timestamp
        return result