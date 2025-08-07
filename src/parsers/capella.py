from typing import Dict, List, Any, Optional
from .bellatrix import BellatrixParser

class CapellaParser(BellatrixParser):
    """Capella parser with clean schema - withdrawals data in withdrawals table only."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "capella"
    
    def get_supported_tables(self) -> List[str]:
        """Capella adds withdrawals and BLS changes."""
        return super().get_supported_tables() + ["withdrawals", "bls_changes"]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Capella withdrawals and BLS changes - no duplication."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Parse withdrawals from execution payload
        execution_payload = body.get("execution_payload", {})
        if execution_payload:
            withdrawals = execution_payload.get("withdrawals", [])
            if withdrawals:
                withdrawal_rows = []
                for withdrawal in withdrawals:
                    withdrawal_rows.append({
                        "slot": slot,
                        "block_number": self.safe_int(execution_payload.get("block_number"), 0),
                        "block_hash": self.safe_str(execution_payload.get("block_hash"), ""),
                        "withdrawal_index": self.safe_int(withdrawal.get("index"), 0),
                        "validator_index": self.safe_int(withdrawal.get("validator_index"), 0),
                        "address": self.safe_str(withdrawal.get("address"), ""),
                        "amount": self.safe_int(withdrawal.get("amount"), 0)
                    })
                
                result["withdrawals"] = withdrawal_rows
            
            # Update execution payload with withdrawals count (already in base parser)
            if "execution_payloads" in result and result["execution_payloads"]:
                result["execution_payloads"][0]["withdrawals_count"] = len(withdrawals)
        
        # Parse BLS to execution changes
        bls_to_execution_changes = body.get("bls_to_execution_changes", [])
        if bls_to_execution_changes:
            bls_change_rows = []
            for i, change in enumerate(bls_to_execution_changes):
                change_message = change.get("message", {})
                bls_change_rows.append({
                    "slot": slot,
                    "change_index": i,
                    "signature": self.safe_str(change.get("signature"), ""),
                    "validator_index": self.safe_int(change_message.get("validator_index"), 0),
                    "from_bls_pubkey": self.safe_str(change_message.get("from_bls_pubkey"), ""),
                    "to_execution_address": self.safe_str(change_message.get("to_execution_address"), "")
                })
            
            result["bls_changes"] = bls_change_rows
        
        return result
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Capella (mainnet)."""
        return "0x03000000"