from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class Validator(BaseModel):
    """Model representing a beacon chain validator."""
    
    slot: int
    validator_index: int
    pubkey: str
    withdrawal_credentials: str
    effective_balance: int
    slashed: bool
    activation_eligibility_epoch: int
    activation_epoch: int
    exit_epoch: int
    withdrawable_epoch: int
    status: str
    balance: int
    slot_timestamp: Optional[datetime] = None
    month: Optional[str] = None

    @classmethod
    def from_api_response(cls, slot: int, validator_data: Dict[str, Any]) -> "Validator":
        """Create a Validator from API response data."""
        validator_index = int(validator_data.get("index", 0))
        balance = int(validator_data.get("balance", 0))
        status = validator_data.get("status", "")
        
        validator = validator_data.get("validator", {})
        pubkey = validator.get("pubkey", "")
        withdrawal_credentials = validator.get("withdrawal_credentials", "")
        effective_balance = int(validator.get("effective_balance", 0))
        slashed = validator.get("slashed", False)
        activation_eligibility_epoch = int(validator.get("activation_eligibility_epoch", 0))
        activation_epoch = int(validator.get("activation_epoch", 0))
        exit_epoch = int(validator.get("exit_epoch", 0))
        withdrawable_epoch = int(validator.get("withdrawable_epoch", 0))
        
        return cls(
            slot=slot,
            validator_index=validator_index,
            pubkey=pubkey,
            withdrawal_credentials=withdrawal_credentials,
            effective_balance=effective_balance,
            slashed=slashed,
            activation_eligibility_epoch=activation_eligibility_epoch,
            activation_epoch=activation_epoch,
            exit_epoch=exit_epoch,
            withdrawable_epoch=withdrawable_epoch,
            status=status,
            balance=balance
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        result = self.dict(exclude={"slot_timestamp", "month"})
        # Convert bool to int for database
        result["slashed"] = 1 if self.slashed else 0
        return result