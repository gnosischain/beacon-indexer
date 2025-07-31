from typing import Dict, Optional, List
from src.services.fork import ForkDetectionService
from src.utils.logger import logger
from .fork_base import ForkBaseParser
from .phase0 import Phase0Parser
from .altair import AltairParser
from .bellatrix import BellatrixParser
from .capella import CapellaParser
from .deneb import DenebParser
from .electra import ElectraParser

class ParserFactory:
    """Enhanced factory for creating fork-aware parsers with network-specific fork versions."""
    
    def __init__(self, fork_service: ForkDetectionService):
        self.fork_service = fork_service
        self.parser_cache: Dict[str, ForkBaseParser] = {}
        self._init_parsers()
    
    def _init_parsers(self):
        """Initialize parser instances for all forks and inject fork service."""
        self.parser_cache = {
            "phase0": Phase0Parser(),
            "altair": AltairParser(),
            "bellatrix": BellatrixParser(),
            "capella": CapellaParser(),
            "deneb": DenebParser(),
            "electra": ElectraParser()
        }
        
        # Inject fork service into all parsers for network-aware operations
        for parser in self.parser_cache.values():
            parser.set_fork_service(self.fork_service)
        
        network_info = f" ({self.fork_service.get_network_name()}"
        if self.fork_service.is_auto_detected():
            network_info += " - auto-detected"
        network_info += ")"
        
        logger.info("Enhanced parser factory initialized", 
                   parsers=list(self.parser_cache.keys()),
                   network=network_info)
    
    def get_parser_for_slot(self, slot: int) -> ForkBaseParser:
        """Get the appropriate parser for a given slot."""
        fork_info = self.fork_service.get_fork_at_slot(slot)
        return self.get_parser_for_fork(fork_info.name)
    
    def get_parser_for_fork(self, fork_name: str) -> ForkBaseParser:
        """Get parser for a specific fork."""
        if fork_name in self.parser_cache:
            return self.parser_cache[fork_name]
        
        # Fallback to phase0 for unknown forks
        logger.warning("Unknown fork, falling back to phase0", fork=fork_name)
        return self.parser_cache["phase0"]
    
    def get_all_parsers(self) -> Dict[str, ForkBaseParser]:
        """Get all available parsers."""
        return self.parser_cache.copy()
    
    def get_supported_tables_for_slot(self, slot: int) -> List[str]:
        """Get supported tables for a given slot."""
        parser = self.get_parser_for_slot(slot)
        return parser.get_supported_tables()
    
    def parse_data(self, slot: int, raw_data: dict) -> dict:
        """Parse data using the appropriate fork parser with network-aware fork versions."""
        parser = self.get_parser_for_slot(slot)
        fork_info = self.fork_service.get_fork_at_slot(slot)
        
        logger.debug("Parsing data with network-aware parser", 
                    slot=slot, 
                    fork=fork_info.name,
                    parser=parser.__class__.__name__,
                    fork_version=fork_info.version,
                    network=self.fork_service.get_network_name(),
                    supported_tables=len(parser.get_supported_tables()))
        
        return parser.parse(raw_data)