import json
from typing import Dict, List, Any, Optional
from .deneb import DenebParser

class ElectraParser(DenebParser):
    """Electra parser with clean schema - execution requests in separate table."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "electra"
    
    def get_supported_tables(self) -> List[str]:
        """Electra adds execution requests table."""
        return super().get_supported_tables() + ["execution_requests"]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Electra execution requests - stored separately, not in blocks."""
        result = super().parse_fork_specific(slot, data)
        
        message = data.get("message", {})
        body = message.get("body", {})
        execution_requests = body.get("execution_requests", {})
        
        if execution_requests:
            # Count actual requests
            deposits = execution_requests.get("deposits", [])
            withdrawals = execution_requests.get("withdrawals", [])
            consolidations = execution_requests.get("consolidations", [])
            
            deposits_count = len(deposits)
            withdrawals_count = len(withdrawals)
            consolidations_count = len(consolidations)
            
            # Only add execution requests data if there are actual requests
            if deposits_count > 0 or withdrawals_count > 0 or consolidations_count > 0:
                # Create the execution requests row with JSON payload
                execution_requests_row = {
                    "slot": slot,
                    "payload": json.dumps(execution_requests),  # Store as JSON string
                    "deposits_count": deposits_count,
                    "withdrawals_count": withdrawals_count,
                    "consolidations_count": consolidations_count
                }
                
                result["execution_requests"] = [execution_requests_row]
        
        return result
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Electra (mainnet)."""
        return "0x05000000"