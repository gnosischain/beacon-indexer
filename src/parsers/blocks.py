import json
from typing import Dict, List, Any
from .base import BaseParser

class BlocksParser(BaseParser):
    """Parser for beacon blocks stored as String payloads."""
    
    def __init__(self):
        super().__init__("blocks")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse block data from String payload into structured format."""
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
        
        block_data = payload["data"]
        message = block_data.get("message", {})
        
        # Parse main block
        block_row = {
            "slot": slot,
            "proposer_index": int(message.get("proposer_index", 0)),
            "parent_root": message.get("parent_root", ""),
            "state_root": message.get("state_root", ""),
            "body_root": message.get("body", {}).get("root", ""),
            "signature": block_data.get("signature", "")
        }
        
        result = {"blocks": [block_row]}
        
        # Parse attestations if present
        body = message.get("body", {})
        attestations = body.get("attestations", [])
        
        if attestations:
            attestation_rows = []
            for att in attestations:
                att_data = att.get("data", {})
                attestation_rows.append({
                    "slot": slot,
                    "committee_index": int(att_data.get("index", 0)),
                    "beacon_block_root": att_data.get("beacon_block_root", ""),
                    "source_epoch": int(att_data.get("source", {}).get("epoch", 0)),
                    "source_root": att_data.get("source", {}).get("root", ""),
                    "target_epoch": int(att_data.get("target", {}).get("epoch", 0)),
                    "target_root": att_data.get("target", {}).get("root", ""),
                    "aggregation_bits": att.get("aggregation_bits", ""),
                    "signature": att.get("signature", "")
                })
            result["attestations"] = attestation_rows
        
        return result