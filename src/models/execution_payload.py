from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

class ExecutionPayload(BaseModel):
    """Model representing an execution payload."""
    
    slot: int
    block_root: str
    parent_hash: str
    fee_recipient: str
    state_root: str
    receipts_root: str
    logs_bloom: str
    prev_randao: str
    block_number: int
    gas_limit: int
    gas_used: int
    timestamp: datetime
    extra_data: str
    base_fee_per_gas: int
    block_hash: str
    excess_blob_gas: Optional[int] = None  
    transactions_count: int
    withdrawals_count: int
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, payload_data: Dict[str, Any], version: str) -> "ExecutionPayload":
        """Create an ExecutionPayload from API response data."""
        parent_hash = payload_data.get("parent_hash", "")
        fee_recipient = payload_data.get("fee_recipient", "")
        state_root = payload_data.get("state_root", "")
        receipts_root = payload_data.get("receipts_root", "")
        logs_bloom = payload_data.get("logs_bloom", "")
        prev_randao = payload_data.get("prev_randao", "")
        block_number = int(payload_data.get("block_number", 0))
        gas_limit = int(payload_data.get("gas_limit", 0))
        gas_used = int(payload_data.get("gas_used", 0))
        timestamp_val = int(payload_data.get("timestamp", 0))
        timestamp = datetime.fromtimestamp(timestamp_val) if timestamp_val > 0 else datetime.now()
        extra_data = payload_data.get("extra_data", "")
        base_fee_per_gas = int(payload_data.get("base_fee_per_gas", 0))
        block_hash = payload_data.get("block_hash", "")
        
        # Deneb+ fields
        excess_blob_gas = None
        if version in ["deneb", "electra"]:
            excess_blob_gas = int(payload_data.get("excess_blob_gas", 0))
        
        # Count transactions and withdrawals
        transactions = payload_data.get("transactions", [])
        transactions_count = len(transactions)
        
        withdrawals = []
        if version in ["capella", "deneb", "electra"]:
            withdrawals = payload_data.get("withdrawals", [])
        withdrawals_count = len(withdrawals)
        
        return cls(
            slot=slot,
            block_root=block_root,
            parent_hash=parent_hash,
            fee_recipient=fee_recipient,
            state_root=state_root,
            receipts_root=receipts_root,
            logs_bloom=logs_bloom,
            prev_randao=prev_randao,
            block_number=block_number,
            gas_limit=gas_limit,
            gas_used=gas_used,
            timestamp=timestamp,
            extra_data=extra_data,
            base_fee_per_gas=base_fee_per_gas,
            block_hash=block_hash,
            excess_blob_gas=excess_blob_gas,
            transactions_count=transactions_count,
            withdrawals_count=withdrawals_count
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})

class Transaction(BaseModel):
    """Model representing an execution transaction."""
    
    slot: int
    block_root: str
    tx_index: int
    transaction_data: str
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, tx_index: int, tx_data: str) -> "Transaction":
        """Create a Transaction from API response data."""
        return cls(
            slot=slot,
            block_root=block_root,
            tx_index=tx_index,
            transaction_data=tx_data
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})

class Withdrawal(BaseModel):
    """Model representing a withdrawal."""
    
    slot: int
    block_root: str
    withdrawal_index: int
    validator_index: int
    address: str
    amount: int
    slot_timestamp: Optional[datetime] = None

    @classmethod
    def from_api_response(cls, slot: int, block_root: str, withdrawal_data: Dict[str, Any]) -> "Withdrawal":
        """Create a Withdrawal from API response data."""
        withdrawal_index = int(withdrawal_data.get("index", 0))
        validator_index = int(withdrawal_data.get("validator_index", 0))
        address = withdrawal_data.get("address", "")
        amount = int(withdrawal_data.get("amount", 0))
        
        return cls(
            slot=slot,
            block_root=block_root,
            withdrawal_index=withdrawal_index,
            validator_index=validator_index,
            address=address,
            amount=amount
        )

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return self.dict(exclude={"slot_timestamp"})