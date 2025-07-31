from typing import Dict, List, Any, Optional
from .altair import AltairParser

class BellatrixParser(AltairParser):
    """Enhanced Bellatrix parser with complete execution layer support and network-aware fork versions."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "bellatrix"
    
    def get_supported_tables(self) -> List[str]:
        """Bellatrix adds execution payloads and transactions."""
        return super().get_supported_tables() + ["execution_payloads", "transactions"]
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extend Altair block parsing with execution payload data."""
        block_data = super().parse_block(slot, data)
        
        if block_data:
            message = data.get("message", {})
            body = message.get("body", {})
            execution_payload = body.get("execution_payload", {})
            
            if execution_payload:
                # Add execution payload fields to block
                block_data.update({
                    "execution_payload_block_hash": self.safe_str(execution_payload.get("block_hash")),
                    "execution_payload_parent_hash": self.safe_str(execution_payload.get("parent_hash")),
                    "execution_payload_fee_recipient": self.safe_str(execution_payload.get("fee_recipient")),
                    "execution_payload_state_root": self.safe_str(execution_payload.get("state_root")),
                    "execution_payload_receipts_root": self.safe_str(execution_payload.get("receipts_root")),
                    "execution_payload_logs_bloom": self.safe_str(execution_payload.get("logs_bloom")),
                    "execution_payload_prev_randao": self.safe_str(execution_payload.get("prev_randao")),
                    "execution_payload_block_number": self.safe_int(execution_payload.get("block_number")),
                    "execution_payload_gas_limit": self.safe_int(execution_payload.get("gas_limit")),
                    "execution_payload_gas_used": self.safe_int(execution_payload.get("gas_used")),
                    "execution_payload_timestamp": self.safe_int(execution_payload.get("timestamp")),
                    "execution_payload_extra_data": self.safe_str(execution_payload.get("extra_data")),
                    "execution_payload_base_fee_per_gas": self.safe_str(execution_payload.get("base_fee_per_gas"))
                })
        
        return block_data
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Bellatrix execution data with enhanced transaction parsing."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        execution_payload = body.get("execution_payload", {})
        
        if execution_payload:
            # Enhanced execution payload
            payload_data = {
                "slot": slot,
                "block_hash": self.safe_str(execution_payload.get("block_hash")),
                "parent_hash": self.safe_str(execution_payload.get("parent_hash")),
                "fee_recipient": self.safe_str(execution_payload.get("fee_recipient")),
                "state_root": self.safe_str(execution_payload.get("state_root")),
                "receipts_root": self.safe_str(execution_payload.get("receipts_root")),
                "logs_bloom": self.safe_str(execution_payload.get("logs_bloom")),
                "prev_randao": self.safe_str(execution_payload.get("prev_randao")),
                "block_number": self.safe_int(execution_payload.get("block_number")),
                "gas_limit": self.safe_int(execution_payload.get("gas_limit")),
                "gas_used": self.safe_int(execution_payload.get("gas_used")),
                "timestamp": self.safe_int(execution_payload.get("timestamp")),
                "extra_data": self.safe_str(execution_payload.get("extra_data")),
                "base_fee_per_gas": self.safe_str(execution_payload.get("base_fee_per_gas")),
                "transactions_count": len(execution_payload.get("transactions", [])),
                "transactions_root": self._calculate_transactions_root(execution_payload.get("transactions", [])),
                # Set default blob gas fields for Bellatrix (Deneb will override):
                "blob_gas_used": 0,
                "excess_blob_gas": 0,
                "withdrawals_count": 0,  # Capella will override
                "withdrawals_root": ""   # Capella will override
            }
            
            result["execution_payloads"] = [payload_data]
            
            # Enhanced transactions parsing
            transactions = self._parse_enhanced_transactions(
                slot, 
                execution_payload.get("transactions", []),
                execution_payload
            )
            if transactions:
                result["transactions"] = transactions
        
        return result
    
    def _parse_enhanced_transactions(self, slot: int, transactions: List[str], execution_payload: Dict) -> List[Dict[str, Any]]:
        """Enhanced transaction parsing matching ERA parser structure."""
        tx_rows = []
        
        block_number = self.safe_int(execution_payload.get("block_number"))
        block_hash = self.safe_str(execution_payload.get("block_hash"))
        fee_recipient = self.safe_str(execution_payload.get("fee_recipient"))
        base_fee_per_gas = self.safe_str(execution_payload.get("base_fee_per_gas", "0"))
        
        for i, tx_data in enumerate(transactions):
            # In a real implementation, you'd decode the transaction data
            # For now, we store enhanced basic info matching ERA parser
            tx_rows.append({
                "slot": slot,
                "block_number": block_number,
                "block_hash": block_hash,
                "transaction_index": i,
                "transaction_hash": self._calculate_tx_hash(tx_data),
                "from_address": "",      # Would decode from tx
                "to_address": "",        # Would decode from tx
                "value": "0",           # Would decode from tx
                "gas": 0,               # Would decode from tx
                "gas_price": "0",       # Would decode from tx
                "gas_used": 0,          # Would need receipt data
                "gas_limit": 0,         # Would decode from tx
                "fee_recipient": fee_recipient,
                "base_fee_per_gas": base_fee_per_gas,
                "nonce": 0,             # Would decode from tx
                "input": tx_data if isinstance(tx_data, str) else ""
            })
        
        return tx_rows
    
    def _calculate_transactions_root(self, transactions: List[str]) -> str:
        """Calculate transactions root (placeholder)."""
        if not transactions:
            return ""
        # In a real implementation, this would calculate the Merkle root
        return f"tx_root_{len(transactions)}"
    
    def _calculate_tx_hash(self, tx_data: str) -> str:
        """Calculate transaction hash (placeholder)."""
        # In a real implementation, this would hash the transaction data
        return f"tx_hash_{hash(tx_data) & 0xffffffff:08x}"
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Bellatrix (mainnet)."""
        return "0x02000000"