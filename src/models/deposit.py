from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime

class Deposit(BaseModel):
    """Model representing a deposit."""
    
    slot: int
    block_root: str
    deposit_index: int
    pubkey: str
    withdrawal_credentials: str
    amount: int
    signature: str
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, deposit_index: int, deposit_data: Dict[str, Any]) -> "Deposit":
        """Create a Deposit from API response data."""
        data = deposit_data.get("data", {})
        
        pubkey = data.get("pubkey", "")
        withdrawal_credentials = data.get("withdrawal_credentials", "")
        amount = int(data.get("amount", 0))
        signature = data.get("signature", "")
        
        return cls(
            slot=slot,
            block_root=block_root,
            deposit_index=deposit_index,
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            amount=amount,
            signature=signature
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp", "month"})