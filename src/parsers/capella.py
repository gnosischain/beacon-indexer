from typing import Dict, List, Any, Optional
from .bellatrix import BellatrixParser

class CapellaParser(BellatrixParser):
    """Enhanced Capella parser with withdrawal support and network-aware fork versions."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "capella"
    
    def get_supported_tables(self) -> List[str]:
        """Capella adds withdrawals and BLS changes."""
        return super().get_supported_tables() + ["withdrawals", "bls_changes"]
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extend Bellatrix block parsing with withdrawals data."""
        block_data = super().parse_block(slot, data)
        
        if block_data:
            message = data.get("message", {})
            body = message.get("body", {})
            execution_payload = body.get("execution_payload", {})
            
            if execution_payload:
                withdrawals = execution_payload.get("withdrawals", [])
                block_data["withdrawals_count"] = len(withdrawals)
                block_data["withdrawals_root"] = self._calculate_withdrawals_root(withdrawals)
        
        return block_data
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Capella withdrawals and BLS changes."""
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
                        "block_number": self.safe_int(execution_payload.get("block_number")),
                        "block_hash": self.safe_str(execution_payload.get("block_hash")),
                        "withdrawal_index": self.safe_int(withdrawal.get("index")),
                        "validator_index": self.safe_int(withdrawal.get("validator_index")),
                        "address": self.safe_str(withdrawal.get("address")),
                        "amount": self.safe_int(withdrawal.get("amount"))
                    })
                
                result["withdrawals"] = withdrawal_rows
            
            # Update execution payload with withdrawals info
            if "execution_payloads" in result and result["execution_payloads"]:
                result["execution_payloads"][0].update({
                    "withdrawals_root": self._calculate_withdrawals_root(withdrawals),
                    "withdrawals_count": len(withdrawals)
                })
        
        # Parse BLS to execution changes
        bls_to_execution_changes = body.get("bls_to_execution_changes", [])
        if bls_to_execution_changes:
            bls_change_rows = []
            for i, change in enumerate(bls_to_execution_changes):
                change_message = change.get("message", {})
                bls_change_rows.append({
                    "slot": slot,
                    "change_index": i,
                    "signature": self.safe_str(change.get("signature")),
                    "validator_index": self.safe_int(change_message.get("validator_index")),
                    "from_bls_pubkey": self.safe_str(change_message.get("from_bls_pubkey")),
                    "to_execution_address": self.safe_str(change_message.get("to_execution_address"))
                })
            
            result["bls_changes"] = bls_change_rows
        
        return result
    
    def _calculate_withdrawals_root(self, withdrawals: List[Dict[str, Any]]) -> str:
        """Calculate withdrawals root (placeholder)."""
        if not withdrawals:
            return ""
        # In a real implementation, this would calculate the Merkle root
        return f"withdrawals_root_{len(withdrawals)}"
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Capella (mainnet)."""
        return "0x03000000"