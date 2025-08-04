import json
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from src.utils.logger import logger

class ForkBaseParser(ABC):
    """Enhanced base class for fork-aware parsers with network-specific fork versions."""
    
    def __init__(self, fork_name: str):
        self.fork_name = fork_name
        # Fork service will be injected by the factory
        self.fork_service = None
    
    def set_fork_service(self, fork_service):
        """Inject fork service for network-aware operations."""
        self.fork_service = fork_service
    
    @abstractmethod
    def get_supported_tables(self) -> List[str]:
        """Return list of table names this parser can populate."""
        pass
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Main parse method that coordinates parsing of all supported data.
        
        Returns:
            Dictionary where keys are table names and values are lists of rows.
        """
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")
        
        try:
            # Parse JSON string back to dict
            if isinstance(payload_str, str):
                payload = json.loads(payload_str)
            else:
                payload = payload_str  # Already a dict
        except (json.JSONDecodeError, TypeError) as e:
            logger.error("Failed to parse JSON payload", 
                        slot=slot, 
                        fork=self.fork_name,
                        error=str(e))
            return {}
        
        if "data" not in payload:
            logger.debug("No data field in payload", slot=slot, fork=self.fork_name)
            return {}
        
        # Parse the data
        result = {}
        
        try:
            # Parse common block data first
            if self._should_parse_blocks():
                block_data = self.parse_block(slot, payload["data"])
                if block_data:
                    result["blocks"] = [block_data]
            
            # Parse attestations if present
            if self._should_parse_attestations():
                attestations = self.parse_attestations(slot, payload["data"])
                if attestations:
                    result["attestations"] = attestations
            
            # Let derived classes add fork-specific parsing
            fork_specific = self.parse_fork_specific(slot, payload["data"])
            result.update(fork_specific)
            
        except Exception as e:
            logger.error("Fork parser failed", 
                        slot=slot, 
                        fork=self.fork_name,
                        error=str(e))
        
        return result
    
    def _should_parse_blocks(self) -> bool:
        """Override in subclasses if blocks parsing should be skipped."""
        return "blocks" in self.get_supported_tables()
    
    def _should_parse_attestations(self) -> bool:
        """Override in subclasses if attestations parsing should be skipped."""
        return "attestations" in self.get_supported_tables()
    
    def parse_block(self, slot: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse basic block data. Override to extend."""
        message = data.get("message", {})
        body = message.get("body", {})
        eth1_data = body.get("eth1_data", {})
        
        return {
            "slot": slot,
            "proposer_index": self.safe_int(message.get("proposer_index")),
            "parent_root": self.safe_str(message.get("parent_root")),
            "state_root": self.safe_str(message.get("state_root")),
            "signature": self.safe_str(data.get("signature")),
            
            # Enhanced fields with network-aware fork version
            "version": self._get_fork_version(slot),
            "randao_reveal": self.safe_str(body.get("randao_reveal")),
            "graffiti": self.safe_str(body.get("graffiti")),
            "eth1_deposit_root": self.safe_str(eth1_data.get("deposit_root")),
            "eth1_deposit_count": self.safe_int(eth1_data.get("deposit_count")),
            "eth1_block_hash": self.safe_str(eth1_data.get("block_hash"))
        }
    
    def parse_attestations(self, slot: int, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse attestations from block. Override to extend."""
        message = data.get("message", {})
        body = message.get("body", {})
        attestations = body.get("attestations", [])
        
        attestation_rows = []
        for i, att in enumerate(attestations):
            att_data = att.get("data", {})
            attestation_rows.append({
                "slot": slot,
                "attestation_index": i,
                "committee_index": self.safe_int(att_data.get("index")),
                "beacon_block_root": self.safe_str(att_data.get("beacon_block_root")),
                "source_epoch": self.safe_int(att_data.get("source", {}).get("epoch")),
                "source_root": self.safe_str(att_data.get("source", {}).get("root")),
                "target_epoch": self.safe_int(att_data.get("target", {}).get("epoch")),
                "target_root": self.safe_str(att_data.get("target", {}).get("root")),
                "aggregation_bits": self.safe_str(att.get("aggregation_bits")),
                "signature": self.safe_str(att.get("signature")),
                "attestation_slot": self.safe_int(att_data.get("slot", slot))
            })
        
        return attestation_rows
    
    @abstractmethod
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse fork-specific data. Must be implemented by subclasses."""
        pass
    
    def _get_fork_version(self, slot: int) -> str:
        """Get network-specific fork version for this slot."""
        if self.fork_service:
            # Use fork service to get the correct version for this network and slot
            fork_info = self.fork_service.get_fork_at_slot(slot)
            if fork_info.name == self.fork_name:
                return fork_info.version
            else:
                # This parser is being used for a different fork than expected
                # This can happen during fork transitions - log but use the actual fork version
                logger.debug("Parser fork mismatch", 
                           parser_fork=self.fork_name, 
                           actual_fork=fork_info.name,
                           slot=slot,
                           version=fork_info.version)
                return fork_info.version
        else:
            # Fallback to hardcoded version (shouldn't happen in production)
            logger.warning("Fork service not available, using fallback version", 
                          fork=self.fork_name, slot=slot)
            return self._get_fallback_fork_version()
    
    @abstractmethod 
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version (mainnet). Must be implemented by subclasses."""
        pass
    
    def safe_int(self, value: Any, default: int = 0) -> int:
        """Safely convert value to int."""
        try:
            if value is None:
                return default
            if isinstance(value, str):
                # Handle hex strings
                if value.startswith("0x"):
                    return int(value, 16)
                return int(value)
            return int(value)
        except (ValueError, TypeError):
            return default
    
    def safe_str(self, value: Any, default: str = "") -> str:
        """Safely convert value to string."""
        try:
            return str(value) if value is not None else default
        except (ValueError, TypeError):
            return default