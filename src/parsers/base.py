from abc import ABC, abstractmethod
from typing import Dict, List, Any

class BaseParser(ABC):
    """Base class for all parsers."""
    
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse raw data and return structured data.
        
        Returns:
            Dictionary where keys are table names and values are lists of rows.
        """
        pass