from typing import Dict, List, Any, Optional
from .fork_base import ForkBaseParser

class Phase0Parser(ForkBaseParser):
    """Enhanced Phase 0 parser with all missing fields and network-aware fork versions."""
    
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
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Enhanced block parsing with all missing fields."""
        message = data.get("message", {})
        body = message.get("body", {})
        eth1_data = body.get("eth1_data", {})
        
        return {
            "slot": slot,
            "proposer_index": self.safe_int(message.get("proposer_index")),
            "parent_root": self.safe_str(message.get("parent_root")),
            "state_root": self.safe_str(message.get("state_root")),
            "signature": self.safe_str(data.get("signature")),
            
            # Enhanced fields from ERA parser with network-aware fork version:
            "version": self._get_fork_version(slot),
            "randao_reveal": self.safe_str(body.get("randao_reveal")),
            "graffiti": self.safe_str(body.get("graffiti")),
            "eth1_deposit_root": self.safe_str(eth1_data.get("deposit_root")),
            "eth1_deposit_count": self.safe_int(eth1_data.get("deposit_count")),
            "eth1_block_hash": self.safe_str(eth1_data.get("block_hash"))
        }
    
    def parse_attestations(self, slot: int, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enhanced attestation parsing with missing fields."""
        message = data.get("message", {})
        body = message.get("body", {})
        attestations = body.get("attestations", [])
        
        attestation_rows = []
        for i, att in enumerate(attestations):
            att_data = att.get("data", {})
            attestation_rows.append({
                "slot": slot,
                "attestation_index": i,  # Index within the block
                "committee_index": self.safe_int(att_data.get("index")),
                "beacon_block_root": self.safe_str(att_data.get("beacon_block_root")),
                "source_epoch": self.safe_int(att_data.get("source", {}).get("epoch")),
                "source_root": self.safe_str(att_data.get("source", {}).get("root")),
                "target_epoch": self.safe_int(att_data.get("target", {}).get("epoch")),
                "target_root": self.safe_str(att_data.get("target", {}).get("root")),
                "aggregation_bits": self.safe_str(att.get("aggregation_bits")),
                "signature": self.safe_str(att.get("signature")),
                "attestation_slot": self.safe_int(att_data.get("slot", slot))  # The slot being attested
            })
        
        return attestation_rows
    
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
        
        # Parse attester slashings
        attester_slashings = body.get("attester_slashings", [])
        if attester_slashings:
            slash_rows = []
            for i, slashing in enumerate(attester_slashings):
                att_1 = slashing.get("attestation_1", {})
                att_2 = slashing.get("attestation_2", {})
                att_1_data = att_1.get("data", {})
                att_2_data = att_2.get("data", {})
                
                # Parse attesting indices arrays
                att_1_indices = att_1.get("attesting_indices", [])
                att_2_indices = att_2.get("attesting_indices", [])
                
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
                    "att_1_attesting_indices": [self.safe_int(idx) for idx in att_1_indices],
                    "att_1_validator_count": len(att_1_indices),
                    "att_2_slot": self.safe_int(att_2_data.get("slot")),
                    "att_2_committee_index": self.safe_int(att_2_data.get("index")),
                    "att_2_beacon_block_root": self.safe_str(att_2_data.get("beacon_block_root")),
                    "att_2_source_epoch": self.safe_int(att_2_data.get("source", {}).get("epoch")),
                    "att_2_source_root": self.safe_str(att_2_data.get("source", {}).get("root")),
                    "att_2_target_epoch": self.safe_int(att_2_data.get("target", {}).get("epoch")),
                    "att_2_target_root": self.safe_str(att_2_data.get("target", {}).get("root")),
                    "att_2_signature": self.safe_str(att_2.get("signature")),
                    "att_2_attesting_indices": [self.safe_int(idx) for idx in att_2_indices],
                    "att_2_validator_count": len(att_2_indices),
                    "total_slashed_validators": len(set(att_1_indices + att_2_indices))
                })
            result["attester_slashings"] = slash_rows
        
        return result
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Phase 0 (mainnet)."""
        return "0x00000000"