from typing import Dict, List, Any, Optional
from .altair import AltairParser

class BellatrixParser(AltairParser):
    """Bellatrix parser with clean schema - no duplication between tables."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "bellatrix"
    
    def get_supported_tables(self) -> List[str]:
        """Bellatrix adds execution payloads and transactions."""
        return super().get_supported_tables() + ["execution_payloads", "transactions"]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Bellatrix execution data - all execution data goes to execution_payloads table."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        execution_payload = body.get("execution_payload", {})
        
        if execution_payload:
            # ALL execution payload data goes here, not in blocks table
            payload_data = {
                "slot": slot,
                "parent_hash": self.safe_str(execution_payload.get("parent_hash"), ""),
                "fee_recipient": self.safe_str(execution_payload.get("fee_recipient"), ""),
                "state_root": self.safe_str(execution_payload.get("state_root"), ""),
                "receipts_root": self.safe_str(execution_payload.get("receipts_root"), ""),
                "logs_bloom": self.safe_str(execution_payload.get("logs_bloom"), ""),
                "prev_randao": self.safe_str(execution_payload.get("prev_randao"), ""),
                "block_number": self.safe_int(execution_payload.get("block_number"), 0),
                "gas_limit": self.safe_int(execution_payload.get("gas_limit"), 0),
                "gas_used": self.safe_int(execution_payload.get("gas_used"), 0),
                "timestamp": self.safe_int(execution_payload.get("timestamp"), 0),  # This IS a column
                "extra_data": self.safe_str(execution_payload.get("extra_data"), ""),
                "base_fee_per_gas": self.safe_str(execution_payload.get("base_fee_per_gas"), "0"),
                "block_hash": self.safe_str(execution_payload.get("block_hash"), ""),
                "transactions_count": len(execution_payload.get("transactions", [])),
                # Set default fields that later forks will override
                "blob_gas_used": 0,
                "excess_blob_gas": 0,
                "withdrawals_count": 0
            }
            
            result["execution_payloads"] = [payload_data]
            
            # Parse transactions
            transactions = self._parse_transactions(
                slot, 
                execution_payload.get("transactions", []),
                execution_payload
            )
            if transactions:
                result["transactions"] = transactions
        
        return result
    
    def _parse_transactions(self, slot: int, transactions: List[str], execution_payload: Dict) -> List[Dict[str, Any]]:
        """Parse transaction data."""
        tx_rows = []
        
        block_number = self.safe_int(execution_payload.get("block_number"), 0)
        block_hash = self.safe_str(execution_payload.get("block_hash"), "")
        fee_recipient = self.safe_str(execution_payload.get("fee_recipient"), "")
        base_fee_per_gas = self.safe_str(execution_payload.get("base_fee_per_gas", "0"), "0")
        gas_limit = self.safe_int(execution_payload.get("gas_limit"), 0)
        gas_used = self.safe_int(execution_payload.get("gas_used"), 0)
        
        for i, tx_data in enumerate(transactions):
            # Store raw transaction data
            tx_rows.append({
                "slot": slot,
                "block_number": block_number,
                "block_hash": block_hash,
                "transaction_index": i,
                "transaction_hash": self._calculate_tx_hash(tx_data),
                "fee_recipient": fee_recipient,
                "gas_limit": gas_limit,
                "gas_used": gas_used,
                "base_fee_per_gas": base_fee_per_gas,
                "from_address": "",  # Would decode from tx
                "to_address": "",    # Would decode from tx
                "value": "0",        # Would decode from tx
                "gas_price": 0,      # Would decode from tx
                "nonce": 0,          # Would decode from tx
                "input": tx_data if isinstance(tx_data, str) else ""
            })
        
        return tx_rows
    
    def _calculate_tx_hash(self, tx_data: str) -> str:
        """Calculate transaction hash (placeholder)."""
        # In a real implementation, this would hash the transaction data
        return f"tx_hash_{hash(tx_data) & 0xffffffff:08x}"
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Bellatrix (mainnet)."""
        return "0x02000000"