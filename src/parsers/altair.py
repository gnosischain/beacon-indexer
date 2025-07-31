from typing import Dict, List, Any, Optional
from .phase0 import Phase0Parser

class AltairParser(Phase0Parser):
    """Enhanced Altair parser with sync committee support and network-aware fork versions."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "altair"
    
    def get_supported_tables(self) -> List[str]:
        """Altair adds sync aggregates to Phase 0 tables."""
        return super().get_supported_tables() + ["sync_aggregates"]
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extend Phase 0 block parsing with sync aggregate data."""
        block_data = super().parse_block(slot, data)
        
        if block_data:
            # Add sync aggregate fields
            message = data.get("message", {})
            body = message.get("body", {})
            sync_aggregate = body.get("sync_aggregate", {})
            
            if sync_aggregate:
                # Calculate participation from sync committee bits
                sync_committee_bits = sync_aggregate.get("sync_committee_bits", "")
                participation = self._calculate_sync_participation(sync_committee_bits)
                
                block_data["sync_aggregate_participation"] = participation
                block_data["sync_aggregate_signature"] = sync_aggregate.get("sync_committee_signature", "")
        
        return block_data
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Altair-specific sync aggregate data with participation count."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        sync_aggregate = body.get("sync_aggregate", {})
        
        if sync_aggregate:
            sync_committee_bits = sync_aggregate.get("sync_committee_bits", "")
            sync_committee_signature = sync_aggregate.get("sync_committee_signature", "")
            participation_count = self._calculate_sync_participation(sync_committee_bits)
            
            sync_aggregate_data = {
                "slot": slot,
                "sync_committee_bits": sync_committee_bits,
                "sync_committee_signature": sync_committee_signature,
                "participation_count": participation_count,
                "participating_validators": participation_count  # ERA parser field
            }
            
            result["sync_aggregates"] = [sync_aggregate_data]
        
        return result
    
    def _calculate_sync_participation(self, sync_committee_bits: str) -> int:
        """Calculate participation count from sync committee bits."""
        if not sync_committee_bits:
            return 0
        
        try:
            # Remove 0x prefix if present
            if sync_committee_bits.startswith("0x"):
                sync_committee_bits = sync_committee_bits[2:]
            
            # Convert hex to binary and count 1 bits
            participation = 0
            for hex_char in sync_committee_bits:
                # Convert each hex digit to binary and count bits
                bits = bin(int(hex_char, 16))[2:].zfill(4)
                participation += bits.count('1')
            
            return participation
            
        except (ValueError, TypeError):
            return 0
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Altair (mainnet)."""
        return "0x01000000"