from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime

class BlobSidecar(BaseModel):
    """Model representing a blob sidecar."""
    
    slot: int
    block_root: str
    blob_index: int
    kzg_commitment: str
    kzg_proof: str
    blob_data: str
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, blob_data: Dict[str, Any]) -> "BlobSidecar":
        """Create a BlobSidecar from API response data."""
        blob_index = int(blob_data.get("index", 0))
        kzg_commitment = blob_data.get("kzg_commitment", "")
        kzg_proof = blob_data.get("kzg_proof", "")
        blob = blob_data.get("blob", "")  # This could be large, might need special handling
        
        return cls(
            slot=slot,
            block_root=block_root,
            blob_index=blob_index,
            kzg_commitment=kzg_commitment,
            kzg_proof=kzg_proof,
            blob_data=blob[:1000]  # For this example, truncate to avoid massive data
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp", "month"})