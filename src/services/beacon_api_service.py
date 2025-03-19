import aiohttp
import asyncio
import json
from typing import Dict, List, Any, Optional

from src.config import config
from src.utils.logger import logger
from src.utils.retry import api_retry

class BeaconAPIService:
    """Service to interact with the Beacon Chain API."""
    
    def __init__(self, base_url: str = None):
        self.base_url = base_url or config.beacon_node.url
        self.session = None
        self.semaphore = asyncio.Semaphore(config.scraper.max_concurrent_requests)
        self._block_cache = {}  # Cache for frequently accessed blocks
    
    async def start(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
    
    async def stop(self):
        if self.session:
            await self.session.close()
            self.session = None
    
    @api_retry()
    async def _make_request(self, method: str, path: str, params: Dict = None, data: Dict = None) -> Any:
        """Make an HTTP request to the Beacon Chain API."""
        url = f"{self.base_url}{path}"
        
        async with self.semaphore:
            async with self.session.request(method, url, params=params, json=data) as response:
                response.raise_for_status()
                return await response.json()
    
    # Genesis information
    async def get_genesis(self) -> Dict:
        """Get the genesis information of the beacon chain."""
        response = await self._make_request("GET", "/eth/v1/beacon/genesis")
        return response["data"]
    
    # Block information
    async def get_block(self, block_id: str) -> Dict:
        """
        Get a block by its identifier.
        Uses V2 endpoint to get both block data and its execution payload.
        Also caches blocks to reduce API calls.
        """
        # Check cache first
        if block_id in self._block_cache:
            return self._block_cache[block_id]
            
        response = await self._make_request("GET", f"/eth/v2/beacon/blocks/{block_id}")
        
        # Try to get block root and add it to the response
        try:
            header_response = await self.get_block_header(block_id)
            block_root = header_response.get("root", "")
            response["block_root"] = block_root
        except Exception:
            # If we can't get the header, just continue without the root
            pass
            
        # Cache the result (with LRU-style management)
        if len(self._block_cache) > 100:  # Limit cache size
            # Remove a random item to avoid synchronization issues
            if self._block_cache:
                key_to_remove = next(iter(self._block_cache))
                del self._block_cache[key_to_remove]
                
        self._block_cache[block_id] = response
        
        return response
    
    async def get_block_header(self, block_id: str) -> Dict:
        """Get a block header by its identifier."""
        response = await self._make_request("GET", f"/eth/v1/beacon/headers/{block_id}")
        return response["data"]
    
    async def get_block_headers(self, slot: Optional[int] = None, parent_root: Optional[str] = None) -> List[Dict]:
        """Get block headers matching the given criteria."""
        params = {}
        if slot is not None:
            params["slot"] = str(slot)
        if parent_root is not None:
            params["parent_root"] = parent_root
        
        response = await self._make_request("GET", "/eth/v1/beacon/headers", params=params)
        return response["data"]
    
    # Block components
    async def get_block_attestations(self, block_id: str) -> List[Dict]:
        """Get attestations included in a block."""
        response = await self._make_request("GET", f"/eth/v2/beacon/blocks/{block_id}/attestations")
        return response["data"]
    
    async def get_blob_sidecars(self, block_id: str, indices: List[int] = None) -> List[Dict]:
        """Get blob sidecars for a block."""
        params = {}
        if indices:
            params["indices"] = [str(idx) for idx in indices]
        
        try:
            response = await self._make_request("GET", f"/eth/v1/beacon/blob_sidecars/{block_id}", params=params)
            return response["data"]
        except Exception as e:
            # This endpoint may not be available on all nodes or for all blocks
            if "404" in str(e):
                logger.debug(f"No blob sidecars available for block {block_id}")
                return []
            raise
    
    # State information
    async def get_state_validator(self, state_id: str, validator_id: str) -> Dict:
        """Get information about a specific validator."""
        response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/validators/{validator_id}")
        return response["data"]
    
    async def get_state_validators(self, state_id: str, validator_ids: List[str] = None, statuses: List[str] = None) -> List[Dict]:
        """Get validators from a state, optionally filtered by IDs or statuses."""
        params = {}
        if validator_ids:
            params["id"] = validator_ids
        if statuses:
            params["status"] = statuses
        
        try:
            response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/validators", params=params)
            return response["data"]
        except Exception as e:
            # For large validator sets, GET might fail due to URL length limits
            # If that happens, try POST instead
            if len(str(params)) > 1000:
                return await self.post_state_validators(state_id, validator_ids, statuses)
            raise
    
    async def post_state_validators(self, state_id: str, validator_ids: List[str] = None, statuses: List[str] = None) -> List[Dict]:
        """Post a request to get validators from a state."""
        data = {}
        if validator_ids:
            data["ids"] = validator_ids
        if statuses:
            data["statuses"] = statuses
        
        response = await self._make_request("POST", f"/eth/v1/beacon/states/{state_id}/validators", data=data)
        return response["data"]
    
    async def get_state_validator_balances(self, state_id: str, validator_ids: List[str] = None) -> List[Dict]:
        """Get validator balances from a state."""
        params = {}
        if validator_ids:
            params["id"] = validator_ids
        
        response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/validator_balances", params=params)
        return response["data"]
    
    async def get_state_committees(self, state_id: str, epoch: Optional[int] = None, committee_index: Optional[int] = None, slot: Optional[int] = None) -> List[Dict]:
        """Get committees for a state."""
        params = {}
        if epoch is not None:
            params["epoch"] = str(epoch)
        if committee_index is not None:
            params["index"] = str(committee_index)
        if slot is not None:
            params["slot"] = str(slot)
        
        response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/committees", params=params)
        return response["data"]
    
    async def get_state_sync_committees(self, state_id: str, epoch: Optional[int] = None) -> Dict:
        """Get sync committees for a state."""
        params = {}
        if epoch is not None:
            params["epoch"] = str(epoch)
        
        try:
            response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/sync_committees", params=params)
            return response["data"]
        except Exception as e:
            # This endpoint may not be available on all nodes or for all states
            if "404" in str(e):
                logger.debug(f"No sync committees available for state {state_id}")
                return {}
            raise
    
   # async def get_state_fork(self, state_id: str) -> Dict:
   #     """Get fork information for a state."""
   #     response = await self._make_request("GET", f"/eth/v1/beacon/states/{state_id}/fork")
   #     return response["data"]
    
    # Rewards
    async def get_block_rewards(self, block_id: str) -> Dict:
        """Get block rewards."""
        try:
            response = await self._make_request("GET", f"/eth/v1/beacon/rewards/blocks/{block_id}")
            return response["data"]
        except Exception as e:
            # This endpoint may not be available on all nodes
            logger.warning(f"Failed to get block rewards: {e}")
            return {}
    
    async def get_attestation_rewards(self, epoch: int, validator_ids: List[str] = None) -> Dict:
        """
        Get attestation rewards for an epoch.
        """
        data = validator_ids if validator_ids else []
        
        try:
            return await self._make_request("POST", f"/eth/v1/beacon/rewards/attestations/{epoch}", data=data)
        except Exception as e:
            # This endpoint may not be available on all nodes
            logger.warning(f"Failed to get attestation rewards: {e}")
            return {}
    
    async def get_sync_committee_rewards(self, block_id: str, validator_ids: List[str] = None) -> List[Dict]:
        """Get sync committee rewards for a block."""
        data = validator_ids if validator_ids else []
    
        try:
            return await self._make_request("POST", f"/eth/v1/beacon/rewards/sync_committee/{block_id}", data=data)
        except Exception as e:
            # This endpoint may not be available on all nodes
            logger.warning(f"Failed to get sync committee rewards: {e}")
            return {"data": []}