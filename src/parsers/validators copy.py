from typing import Dict, List, Any
from .base import BaseParser

class ValidatorsParser(BaseParser):
    """Parser for validator data."""
    
    def __init__(self):
        super().__init__("validators")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse validators data into structured format."""
        if "data" not in raw_data:
            return {}
        
        validators_data = raw_data["data"]
        validator_rows = []
        
        # Extract slot from the raw data if available (for snapshot-based validator data)
        # This would typically come from the state_id used to fetch the data
        slot = raw_data.get("slot", 0)  # Default to 0 if not available
        
        for validator in validators_data:
            validator_info = validator.get("validator", {})
            validator_rows.append({
                "slot": slot,
                "validator_index": int(validator.get("index", 0)),
                "balance": int(validator.get("balance", 0)),
                "status": validator.get("status", ""),
                "pubkey": validator_info.get("pubkey", ""),
                "withdrawal_credentials": validator_info.get("withdrawal_credentials", ""),
                "effective_balance": int(validator_info.get("effective_balance", 0)),
                "slashed": 1 if validator_info.get("slashed", False) else 0,
                "activation_eligibility_epoch": int(validator_info.get("activation_eligibility_epoch", 0)),
                "activation_epoch": int(validator_info.get("activation_epoch", 0)),
                "exit_epoch": int(validator_info.get("exit_epoch", 0)),
                "withdrawable_epoch": int(validator_info.get("withdrawable_epoch", 0))
            })
        
        return {"validators": validator_rows}