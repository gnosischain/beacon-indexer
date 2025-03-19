from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime

class BlockReward(BaseModel):
    """Model representing rewards for proposing a block."""
    
    slot: int
    block_root: str
    proposer_index: int
    total: int
    attestations: int
    sync_aggregate: int
    proposer_slashings: int
    attester_slashings: int
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, reward_data: Dict[str, Any]) -> "BlockReward":
        """Create a BlockReward from API response data."""
        proposer_index = int(reward_data.get("proposer_index", 0))
        total = int(reward_data.get("total", 0))
        attestations = int(reward_data.get("attestations", 0))
        sync_aggregate = int(reward_data.get("sync_aggregate", 0))
        proposer_slashings = int(reward_data.get("proposer_slashings", 0))
        attester_slashings = int(reward_data.get("attester_slashings", 0))
        
        return cls(
            slot=slot,
            block_root=block_root,
            proposer_index=proposer_index,
            total=total,
            attestations=attestations,
            sync_aggregate=sync_aggregate,
            proposer_slashings=proposer_slashings,
            attester_slashings=attester_slashings
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp", "month"})


class AttestationReward(BaseModel):
    """Model representing rewards for attestations."""
    
    epoch: int
    validator_index: int
    head: int
    target: int
    source: int
    inclusion_delay: Optional[int] = None
    inactivity: int
    epoch_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, epoch: int, reward_data: Dict[str, Any]) -> "AttestationReward":
        """Create an AttestationReward from API response data."""
        validator_index = int(reward_data.get("validator_index", 0))
        head = int(reward_data.get("head", 0))
        target = int(reward_data.get("target", 0))
        source = int(reward_data.get("source", 0))
        inclusion_delay = int(reward_data.get("inclusion_delay", 0)) if reward_data.get("inclusion_delay") is not None else None
        inactivity = int(reward_data.get("inactivity", 0))
        
        return cls(
            epoch=epoch,
            validator_index=validator_index,
            head=head,
            target=target,
            source=source,
            inclusion_delay=inclusion_delay,
            inactivity=inactivity
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"epoch_timestamp", "month"})


class SyncCommitteeReward(BaseModel):
    """Model representing rewards for sync committee participation."""
    
    slot: int
    block_root: str
    validator_index: int
    reward: int
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, reward_data: Dict[str, Any]) -> "SyncCommitteeReward":
        """Create a SyncCommitteeReward from API response data."""
        validator_index = int(reward_data.get("validator_index", 0))
        reward = int(reward_data.get("reward", 0))
        
        return cls(
            slot=slot,
            block_root=block_root,
            validator_index=validator_index,
            reward=reward
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp", "month"})