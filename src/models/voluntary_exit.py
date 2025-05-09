from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime

class VoluntaryExit(BaseModel):
    """Model representing a voluntary exit."""
    
    slot: int
    block_root: str
    validator_index: int
    epoch: int
    signature: str
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, exit_data: Dict[str, Any]) -> "VoluntaryExit":
        """Create a VoluntaryExit from API response data."""
        message = exit_data.get("message", {})
        validator_index = int(message.get("validator_index", 0))
        epoch = int(message.get("epoch", 0))
        signature = exit_data.get("signature", "")
        
        return cls(
            slot=slot,
            block_root=block_root,
            validator_index=validator_index,
            epoch=epoch,
            signature=signature
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})


class ProposerSlashing(BaseModel):
    """Model representing a proposer slashing."""
    
    slot: int
    block_root: str
    proposer_index: int
    header_1_slot: int
    header_1_proposer: int
    header_1_root: str
    header_1_signature: str
    header_2_slot: int
    header_2_proposer: int
    header_2_root: str
    header_2_signature: str
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, slashing_data: Dict[str, Any]) -> "ProposerSlashing":
        """Create a ProposerSlashing from API response data."""
        header_1 = slashing_data.get("signed_header_1", {})
        header_1_message = header_1.get("message", {})
        header_1_slot = int(header_1_message.get("slot", 0))
        header_1_proposer = int(header_1_message.get("proposer_index", 0))
        header_1_root = header_1_message.get("body_root", "")
        header_1_signature = header_1.get("signature", "")
        
        header_2 = slashing_data.get("signed_header_2", {})
        header_2_message = header_2.get("message", {})
        header_2_slot = int(header_2_message.get("slot", 0))
        header_2_proposer = int(header_2_message.get("proposer_index", 0))
        header_2_root = header_2_message.get("body_root", "")
        header_2_signature = header_2.get("signature", "")
        
        # Both headers should have the same proposer index
        proposer_index = header_1_proposer
        
        return cls(
            slot=slot,
            block_root=block_root,
            proposer_index=proposer_index,
            header_1_slot=header_1_slot,
            header_1_proposer=header_1_proposer,
            header_1_root=header_1_root,
            header_1_signature=header_1_signature,
            header_2_slot=header_2_slot,
            header_2_proposer=header_2_proposer,
            header_2_root=header_2_root,
            header_2_signature=header_2_signature
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})


class AttesterSlashing(BaseModel):
    """Model representing an attester slashing."""
    
    slot: int
    block_root: str
    slashing_index: int
    attestation_1_indices: List[int]
    attestation_1_slot: int
    attestation_1_index: int
    attestation_1_root: str
    attestation_1_sig: str
    attestation_2_indices: List[int]
    attestation_2_slot: int
    attestation_2_index: int
    attestation_2_root: str
    attestation_2_sig: str
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, slashing_index: int, slashing_data: Dict[str, Any]) -> "AttesterSlashing":
        """Create an AttesterSlashing from API response data."""
        attestation_1 = slashing_data.get("attestation_1", {})
        attestation_1_data = attestation_1.get("data", {})
        attestation_1_indices = [int(idx) for idx in slashing_data.get("attestation_1_indices", [])]
        attestation_1_slot = int(attestation_1_data.get("slot", 0))
        attestation_1_index = int(attestation_1_data.get("index", 0))
        attestation_1_root = attestation_1_data.get("beacon_block_root", "")
        attestation_1_sig = attestation_1.get("signature", "")
        
        attestation_2 = slashing_data.get("attestation_2", {})
        attestation_2_data = attestation_2.get("data", {})
        attestation_2_indices = [int(idx) for idx in slashing_data.get("attestation_2_indices", [])]
        attestation_2_slot = int(attestation_2_data.get("slot", 0))
        attestation_2_index = int(attestation_2_data.get("index", 0))
        attestation_2_root = attestation_2_data.get("beacon_block_root", "")
        attestation_2_sig = attestation_2.get("signature", "")
        
        return cls(
            slot=slot,
            block_root=block_root,
            slashing_index=slashing_index,
            attestation_1_indices=attestation_1_indices,
            attestation_1_slot=attestation_1_slot,
            attestation_1_index=attestation_1_index,
            attestation_1_root=attestation_1_root,
            attestation_1_sig=attestation_1_sig,
            attestation_2_indices=attestation_2_indices,
            attestation_2_slot=attestation_2_slot,
            attestation_2_index=attestation_2_index,
            attestation_2_root=attestation_2_root,
            attestation_2_sig=attestation_2_sig
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})