from typing import Dict, List, Any, Optional
from .fork_base import ForkBaseParser
from src.utils.logger import logger

class Phase0Parser(ForkBaseParser):
    """Phase 0 parser with clean schema - no duplication between tables."""
    
    def __init__(self):
        super().__init__("phase0")
    
    def get_supported_tables(self) -> List[str]:
        """Phase 0 supports blocks, attestations, and operations."""
        return [
            "blocks", 
            "attestations", 
            "deposits", 
            "voluntary_exits", 
            "proposer_slashings", 
            "attester_slashings"
        ]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Phase 0 operations (deposits, exits, slashings)."""
        result = {}
        
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Parse deposits
        deposits = body.get("deposits", [])
        if deposits:
            deposit_rows = []
            for i, deposit in enumerate(deposits):
                deposit_data = deposit.get("data", {})
                deposit_rows.append({
                    "slot": slot,
                    "deposit_index": i,
                    "pubkey": self.safe_str(deposit_data.get("pubkey"), ""),
                    "withdrawal_credentials": self.safe_str(deposit_data.get("withdrawal_credentials"), ""),
                    "amount": self.safe_int(deposit_data.get("amount"), 0),
                    "signature": self.safe_str(deposit_data.get("signature"), ""),
                    "proof": deposit.get("proof", [])
                })
            result["deposits"] = deposit_rows
        
        # Parse voluntary exits
        voluntary_exits = body.get("voluntary_exits", [])
        if voluntary_exits:
            exit_rows = []
            for i, exit in enumerate(voluntary_exits):
                exit_message = exit.get("message", {})
                exit_rows.append({
                    "slot": slot,
                    "exit_index": i,
                    "signature": self.safe_str(exit.get("signature"), ""),
                    "epoch": self.safe_int(exit_message.get("epoch"), 0),
                    "validator_index": self.safe_int(exit_message.get("validator_index"), 0)
                })
            result["voluntary_exits"] = exit_rows
        
        # Parse proposer slashings  
        proposer_slashings = body.get("proposer_slashings", [])
        if proposer_slashings:
            slash_rows = []
            for i, slashing in enumerate(proposer_slashings):
                header_1 = slashing.get("signed_header_1", {}).get("message", {})
                header_2 = slashing.get("signed_header_2", {}).get("message", {})
                
                slash_rows.append({
                    "slot": slot,
                    "slashing_index": i,
                    "header_1_slot": self.safe_int(header_1.get("slot"), 0),
                    "header_1_proposer_index": self.safe_int(header_1.get("proposer_index"), 0),
                    "header_1_parent_root": self.safe_str(header_1.get("parent_root"), ""),
                    "header_1_state_root": self.safe_str(header_1.get("state_root"), ""),
                    "header_1_body_root": self.safe_str(header_1.get("body_root"), ""),
                    "header_1_signature": self.safe_str(slashing.get("signed_header_1", {}).get("signature"), ""),
                    "header_2_slot": self.safe_int(header_2.get("slot"), 0),
                    "header_2_proposer_index": self.safe_int(header_2.get("proposer_index"), 0),
                    "header_2_parent_root": self.safe_str(header_2.get("parent_root"), ""),
                    "header_2_state_root": self.safe_str(header_2.get("state_root"), ""),
                    "header_2_body_root": self.safe_str(header_2.get("body_root"), ""),
                    "header_2_signature": self.safe_str(slashing.get("signed_header_2", {}).get("signature"), "")
                })
            result["proposer_slashings"] = slash_rows
        
        # Parse attester slashings
        attester_slashings = body.get("attester_slashings", [])
        if attester_slashings:
            logger.debug("Processing attester slashings", 
                       slot=slot, 
                       count=len(attester_slashings))
            
            slash_rows = []
            for i, slashing in enumerate(attester_slashings):
                att_1 = slashing.get("attestation_1", {})
                att_2 = slashing.get("attestation_2", {})
                att_1_data = att_1.get("data", {})
                att_2_data = att_2.get("data", {})
                
                # Parse attesting indices properly
                att_1_indices = self._parse_attesting_indices(att_1.get("attesting_indices", []))
                att_2_indices = self._parse_attesting_indices(att_2.get("attesting_indices", []))
                
                slash_rows.append({
                    "slot": slot,
                    "slashing_index": i,
                    "att_1_slot": self.safe_int(att_1_data.get("slot"), 0),
                    "att_1_committee_index": self.safe_int(att_1_data.get("index"), 0),
                    "att_1_beacon_block_root": self.safe_str(att_1_data.get("beacon_block_root"), ""),
                    "att_1_source_epoch": self.safe_int(att_1_data.get("source", {}).get("epoch"), 0),
                    "att_1_source_root": self.safe_str(att_1_data.get("source", {}).get("root"), ""),
                    "att_1_target_epoch": self.safe_int(att_1_data.get("target", {}).get("epoch"), 0),
                    "att_1_target_root": self.safe_str(att_1_data.get("target", {}).get("root"), ""),
                    "att_1_signature": self.safe_str(att_1.get("signature"), ""),
                    "att_1_attesting_indices": att_1_indices,
                    "att_1_validator_count": len(att_1_indices),
                    "att_2_slot": self.safe_int(att_2_data.get("slot"), 0),
                    "att_2_committee_index": self.safe_int(att_2_data.get("index"), 0),
                    "att_2_beacon_block_root": self.safe_str(att_2_data.get("beacon_block_root"), ""),
                    "att_2_source_epoch": self.safe_int(att_2_data.get("source", {}).get("epoch"), 0),
                    "att_2_source_root": self.safe_str(att_2_data.get("source", {}).get("root"), ""),
                    "att_2_target_epoch": self.safe_int(att_2_data.get("target", {}).get("epoch"), 0),
                    "att_2_target_root": self.safe_str(att_2_data.get("target", {}).get("root"), ""),
                    "att_2_signature": self.safe_str(att_2.get("signature"), ""),
                    "att_2_attesting_indices": att_2_indices,
                    "att_2_validator_count": len(att_2_indices),
                    "total_slashed_validators": len(set(att_1_indices + att_2_indices))
                })
            
            result["attester_slashings"] = slash_rows
        
        return result
    
    def _parse_attesting_indices(self, indices_data: Any) -> List[int]:
        """Parse attesting indices from various formats."""
        if not indices_data:
            return []
        
        try:
            # Handle list format (most common case)
            if isinstance(indices_data, list):
                result = []
                for item in indices_data:
                    if isinstance(item, str):
                        # String number - convert directly to int
                        if item.isdigit():
                            result.append(int(item))
                    elif isinstance(item, int):
                        # Already an integer
                        result.append(item)
                
                return result
            
            # Handle string format (backup case)
            elif isinstance(indices_data, str):
                indices_data = indices_data.strip()
                if not indices_data:
                    return []
                
                # Handle comma-separated values
                if ',' in indices_data:
                    result = []
                    for item in indices_data.split(','):
                        item = item.strip()
                        if item.isdigit():
                            result.append(int(item))
                    return result
                
                # Single value
                if indices_data.isdigit():
                    return [int(indices_data)]
                else:
                    return []
            
            # Handle integer format (single value)
            elif isinstance(indices_data, int):
                return [indices_data]
            
            else:
                logger.error("Unexpected attesting_indices format", 
                           data=repr(indices_data)[:100],
                           type=type(indices_data))
                return []
                
        except Exception as e:
            logger.error("Error parsing attesting_indices", 
                        data=repr(indices_data)[:100],
                        error=str(e))
            return []
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Phase 0 (mainnet)."""
        return "0x00000000"