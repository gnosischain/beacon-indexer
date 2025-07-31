import json
from typing import Dict, List, Any
from .base import BaseParser

class ValidatorsParser(BaseParser):
    """Parser for validator data stored as String payloads."""
    
    def __init__(self):
        super().__init__("validators")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse validators data from String payload into structured format."""
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")
        
        try:
            # Parse JSON string back to dict
            if isinstance(payload_str, str):
                payload = json.loads(payload_str)
            else:
                payload = payload_str  # Already a dict
        except (json.JSONDecodeError, TypeError):
            return {}
        
        if "data" not in payload:
            return {}
        
        validators_data = payload["data"]
        validator_rows = []
        
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