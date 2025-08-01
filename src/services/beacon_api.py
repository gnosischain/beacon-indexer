import aiohttp
import asyncio
from typing import Optional, Dict, Any
from src.config import config
from src.utils.logger import logger

class BeaconAPI:
    """Beacon node API client with retry logic."""
    
    def __init__(self):
        self.base_url = config.BEACON_NODE_URL.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_retries = 3
        self.retry_delay = 5  # seconds
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Initialize the HTTP session."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=600)
            self.session = aiohttp.ClientSession(timeout=timeout)
    
    async def close(self):
        """Close the HTTP session."""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def get(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Make a GET request to the beacon API with retry logic."""
        if not self.session:
            await self.start()
        
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.get(url) as response:
                    if response.status == 404:
                        # Slot is empty (no block) - this is normal, not an error
                        return None
                    
                    if response.status != 200:
                        error_text = await response.text()
                        logger.warning("API request failed", 
                                     url=url, 
                                     status=response.status,
                                     attempt=attempt + 1,
                                     max_retries=self.max_retries,
                                     error=error_text)
                        
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                            continue
                        else:
                            logger.error("API request failed after all retries", 
                                       url=url, 
                                       status=response.status)
                            return None
                    
                    return await response.json()
            
            except asyncio.TimeoutError:
                logger.warning("API request timeout", 
                             url=url, 
                             attempt=attempt + 1,
                             max_retries=self.max_retries)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error("API request timeout after all retries", url=url)
                    return None
                    
            except Exception as e:
                logger.warning("API request error", 
                             url=url, 
                             attempt=attempt + 1,
                             max_retries=self.max_retries,
                             error=str(e))
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error("API request error after all retries", url=url, error=str(e))
                    return None
        
        return None
    
    async def get_genesis(self) -> Optional[Dict[str, Any]]:
        """Get genesis information."""
        return await self.get("/eth/v1/beacon/genesis")
    
    async def get_spec(self) -> Optional[Dict[str, Any]]:
        """Get chain specification."""
        return await self.get("/eth/v1/config/spec")
    
    async def get_block(self, slot: int) -> Optional[Dict[str, Any]]:
        """Get block by slot."""
        return await self.get(f"/eth/v2/beacon/blocks/{slot}")
    
    async def get_validators(self, state_id: str = "head", 
                           validator_ids: Optional[list] = None) -> Optional[Dict[str, Any]]:
        """Get validators for a given state."""
        endpoint = f"/eth/v1/beacon/states/{state_id}/validators"
        if validator_ids:
            # Convert list to comma-separated string
            ids_str = ",".join(map(str, validator_ids))
            endpoint += f"?id={ids_str}"
        return await self.get(endpoint)
    
    async def get_head_slot(self) -> Optional[int]:
        """Get the current head slot."""
        data = await self.get("/eth/v1/beacon/headers/head")
        if data and "data" in data:
            return int(data["data"]["header"]["message"]["slot"])
        return None
    
    async def get_rewards(self, slot: str = "head") -> Optional[Dict[str, Any]]:
        """Get rewards by slot."""
        return await self.get(f"/eth/v1/beacon/rewards/blocks/{slot}")