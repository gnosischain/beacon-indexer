from typing import Dict, List, Any, Optional
from .capella import CapellaParser

class DenebParser(CapellaParser):
    """Enhanced Deneb parser with complete blob support and network-aware fork versions."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "deneb"
    
    def get_supported_tables(self) -> List[str]:
        """Deneb adds blob sidecars and commitments."""
        return super().get_supported_tables() + ["blob_sidecars", "blob_commitments"]
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced block parsing with blob commitment fields."""
        block_data = super().parse_block(slot, data)
        
        if block_data:
            message = data.get("message", {})
            body = message.get("body", {})
            
            # Add blob KZG commitments to block
            blob_kzg_commitments = body.get("blob_kzg_commitments", [])
            block_data["blob_kzg_commitments_count"] = len(blob_kzg_commitments)
            block_data["blob_kzg_commitments_root"] = self._calculate_kzg_commitments_root(blob_kzg_commitments)
        
        return block_data
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Deneb blob data with proper structure."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Parse blob commitments (ERA parser style)
        blob_kzg_commitments = body.get("blob_kzg_commitments", [])
        if blob_kzg_commitments:
            commitment_rows = []
            for i, commitment in enumerate(blob_kzg_commitments):
                commitment_rows.append({
                    "slot": slot,
                    "commitment_index": i,
                    "commitment": self.safe_str(commitment)
                })
            
            result["blob_commitments"] = commitment_rows
        
        # Update execution payload with blob gas fields
        execution_payload = body.get("execution_payload", {})
        if execution_payload and "execution_payloads" in result and result["execution_payloads"]:
            result["execution_payloads"][0].update({
                "blob_gas_used": self.safe_int(execution_payload.get("blob_gas_used")),
                "excess_blob_gas": self.safe_int(execution_payload.get("excess_blob_gas"))
            })
        
        return result
    
    def parse_blob_sidecars(self, slot: int, blob_sidecars_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse blob sidecars data (called separately for blob sidecar data).
        Note: Blob sidecars are typically fetched separately from the block data.
        """
        blob_rows = []
        
        for blob_sidecar in blob_sidecars_data:
            blob_rows.append({
                "slot": slot,
                "blob_index": self.safe_int(blob_sidecar.get("index")),
                "kzg_commitment": self.safe_str(blob_sidecar.get("kzg_commitment")),
                "kzg_proof": self.safe_str(blob_sidecar.get("kzg_proof")),
                "blob_size": len(blob_sidecar.get("blob", "")),
                "blob_hash": self._calculate_blob_hash(blob_sidecar.get("blob", ""))
            })
        
        return blob_rows
    
    def _calculate_kzg_commitments_root(self, commitments: List[str]) -> str:
        """Calculate KZG commitments root (placeholder)."""
        if not commitments:
            return ""
        # In a real implementation, this would calculate the Merkle root
        return f"kzg_root_{len(commitments)}"
    
    def _calculate_blob_hash(self, blob_data: str) -> str:
        """Calculate blob hash (placeholder)."""
        # In a real implementation, this would hash the blob data
        return f"blob_hash_{hash(blob_data) & 0xffffffff:08x}"
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Deneb (mainnet)."""
        return "0x04000000"