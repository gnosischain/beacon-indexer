from typing import Dict, List, Any, Optional
from .fork_base import ForkBaseParser
from src.utils.logger import logger

class Phase0Parser(ForkBaseParser):
    """Enhanced Phase 0 parser with FIXED attester slashing parsing."""
    
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
        """Parse Phase 0 operations (deposits, exits, slashings) with FIXED attester slashing parsing."""
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
                    "pubkey": self.safe_str(deposit_data.get("pubkey")),
                    "withdrawal_credentials": self.safe_str(deposit_data.get("withdrawal_credentials")),
                    "amount": self.safe_int(deposit_data.get("amount")),
                    "signature": self.safe_str(deposit_data.get("signature")),
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
                    "signature": self.safe_str(exit.get("signature")),
                    "epoch": self.safe_int(exit_message.get("epoch")),
                    "validator_index": self.safe_int(exit_message.get("validator_index"))
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
                    "header_1_slot": self.safe_int(header_1.get("slot")),
                    "header_1_proposer_index": self.safe_int(header_1.get("proposer_index")),
                    "header_1_parent_root": self.safe_str(header_1.get("parent_root")),
                    "header_1_state_root": self.safe_str(header_1.get("state_root")),
                    "header_1_body_root": self.safe_str(header_1.get("body_root")),
                    "header_1_signature": self.safe_str(slashing.get("signed_header_1", {}).get("signature")),
                    "header_2_slot": self.safe_int(header_2.get("slot")),
                    "header_2_proposer_index": self.safe_int(header_2.get("proposer_index")),
                    "header_2_parent_root": self.safe_str(header_2.get("parent_root")),
                    "header_2_state_root": self.safe_str(header_2.get("state_root")),
                    "header_2_body_root": self.safe_str(header_2.get("body_root")),
                    "header_2_signature": self.safe_str(slashing.get("signed_header_2", {}).get("signature"))
                })
            result["proposer_slashings"] = slash_rows
        
        # FIXED Parse attester slashings - this was the problematic part
        attester_slashings = body.get("attester_slashings", [])
        if attester_slashings:
            logger.info("Processing attester slashings", 
                       slot=slot, 
                       count=len(attester_slashings))
            
            slash_rows = []
            for i, slashing in enumerate(attester_slashings):
                att_1 = slashing.get("attestation_1", {})
                att_2 = slashing.get("attestation_2", {})
                att_1_data = att_1.get("data", {})
                att_2_data = att_2.get("data", {})
                
                # FIXED: Proper handling of attesting_indices - this is the key fix
                att_1_indices = self._parse_attesting_indices_fixed(att_1.get("attesting_indices", []))
                att_2_indices = self._parse_attesting_indices_fixed(att_2.get("attesting_indices", []))
                
                logger.debug("Parsed attester slashing", 
                           slot=slot,
                           slashing_index=i,
                           att_1_indices_count=len(att_1_indices),
                           att_2_indices_count=len(att_2_indices),
                           att_1_indices_sample=att_1_indices[:3] if att_1_indices else [],
                           att_2_indices_sample=att_2_indices[:3] if att_2_indices else [])
                
                slash_rows.append({
                    "slot": slot,
                    "slashing_index": i,
                    "att_1_slot": self.safe_int(att_1_data.get("slot")),
                    "att_1_committee_index": self.safe_int(att_1_data.get("index")),
                    "att_1_beacon_block_root": self.safe_str(att_1_data.get("beacon_block_root")),
                    "att_1_source_epoch": self.safe_int(att_1_data.get("source", {}).get("epoch")),
                    "att_1_source_root": self.safe_str(att_1_data.get("source", {}).get("root")),
                    "att_1_target_epoch": self.safe_int(att_1_data.get("target", {}).get("epoch")),
                    "att_1_target_root": self.safe_str(att_1_data.get("target", {}).get("root")),
                    "att_1_signature": self.safe_str(att_1.get("signature")),
                    "att_1_attesting_indices": att_1_indices,
                    "att_1_validator_count": len(att_1_indices),
                    "att_2_slot": self.safe_int(att_2_data.get("slot")),
                    "att_2_committee_index": self.safe_int(att_2_data.get("index")),
                    "att_2_beacon_block_root": self.safe_str(att_2_data.get("beacon_block_root")),
                    "att_2_source_epoch": self.safe_int(att_2_data.get("source", {}).get("epoch")),
                    "att_2_source_root": self.safe_str(att_2_data.get("source", {}).get("root")),
                    "att_2_target_epoch": self.safe_int(att_2_data.get("target", {}).get("epoch")),
                    "att_2_target_root": self.safe_str(att_2_data.get("target", {}).get("root")),
                    "att_2_signature": self.safe_str(att_2.get("signature")),
                    "att_2_attesting_indices": att_2_indices,
                    "att_2_validator_count": len(att_2_indices),
                    "total_slashed_validators": len(set(att_1_indices + att_2_indices))
                })
            
            result["attester_slashings"] = slash_rows
            logger.info("Successfully parsed attester slashings", 
                       slot=slot, 
                       slashings_count=len(slash_rows))
        
        return result
    
    def _parse_attesting_indices_fixed(self, indices_data: Any) -> List[int]:
        """
        COMPLETELY FIXED method to properly parse attesting indices from various formats.
        
        The beacon API returns attesting_indices as an array of strings like ["4338"].
        The bug was in trying to convert the entire array to string and then parsing.
        """
        if not indices_data:
            return []
        
        try:
            # Handle list format (most common case - this is what we actually receive)
            if isinstance(indices_data, list):
                result = []
                for item in indices_data:
                    if isinstance(item, str):
                        # String number - convert directly to int
                        if item.isdigit():
                            result.append(int(item))
                        else:
                            logger.warning("Non-numeric string in attesting_indices", 
                                         item=item, 
                                         type=type(item))
                    elif isinstance(item, int):
                        # Already an integer
                        result.append(item)
                    else:
                        logger.warning("Unexpected type in attesting_indices list", 
                                     item=item,
                                     type=type(item))
                
                logger.debug("Successfully parsed attesting_indices", 
                           input_format="list",
                           input_length=len(indices_data),
                           output_length=len(result),
                           sample_input=indices_data[:3] if len(indices_data) > 0 else [],
                           sample_output=result[:3] if len(result) > 0 else [])
                
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
                    logger.debug("Parsed comma-separated attesting_indices", 
                               input=indices_data, 
                               output=result)
                    return result
                
                # Single value
                if indices_data.isdigit():
                    result = [int(indices_data)]
                    logger.debug("Parsed single string attesting_indices", 
                               input=indices_data, 
                               output=result)
                    return result
                else:
                    logger.warning("Non-numeric string attesting_indices", 
                                 data=indices_data)
                    return []
            
            # Handle integer format (single value)
            elif isinstance(indices_data, int):
                result = [indices_data]
                logger.debug("Parsed single int attesting_indices", 
                           input=indices_data, 
                           output=result)
                return result
            
            else:
                logger.error("Unexpected attesting_indices format", 
                           data=repr(indices_data)[:100],
                           type=type(indices_data))
                return []
                
        except Exception as e:
            logger.error("Critical error parsing attesting_indices", 
                        data=repr(indices_data)[:100],
                        error=str(e))
            # Return empty list rather than crashing the entire parsing
            return []
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Phase 0 (mainnet)."""
        return "0x00000000"