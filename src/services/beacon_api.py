import aiohttp
import asyncio
import time
from typing import Optional, Dict, Any
from src.config import config
from src.utils.logger import logger
from src import observability as obs


class BeaconAPIError(Exception):
    """Raised when the beacon node returns an error that should fail the chunk."""
    pass

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
    
    async def get(
        self,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        allow_empty_404: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Make a GET request to the beacon API with retry logic.

        Args:
            endpoint: API path, e.g. "/eth/v2/beacon/blocks/123".
            params: Optional query parameters, merged with the configured API key.
            allow_empty_404: When True (block-style endpoints) a 404 means "empty slot"
                and returns None. When False (beacon-state endpoints, which always exist
                for valid slots) a 404 is a real error and raises BeaconAPIError, so the
                chunk fails loudly instead of being marked complete with zero rows.
        """
        if not self.session:
            await self.start()

        url = f"{self.base_url}{endpoint}"
        endpoint_name = obs.normalize_api_endpoint(endpoint)

        # Merge the configured API key into the query params. Keep it out of `url` so it
        # never leaks into logs; aiohttp attaches it to the outgoing request only.
        query: Dict[str, Any] = dict(params or {})
        if config.BEACON_API_KEY:
            query[config.BEACON_API_KEY_PARAM] = config.BEACON_API_KEY
        request_params = query or None

        for attempt in range(self.max_retries):
            start_time = time.monotonic()
            try:
                async with self.session.get(url, params=request_params) as response:
                    obs.api_requests_total.labels(
                        endpoint=endpoint_name,
                        status=str(response.status)
                    ).inc()
                    obs.api_request_duration_seconds.labels(
                        endpoint=endpoint_name
                    ).observe(time.monotonic() - start_time)

                    if response.status == 404:
                        if allow_empty_404:
                            # Slot is empty (no block) - this is normal, not an error
                            return None
                        # Beacon states always exist for valid slots; a 404 here means the
                        # node cannot serve this historical state (e.g. a non-archive
                        # endpoint). Fail loudly instead of silently returning zero rows.
                        error_text = await response.text()
                        logger.error("Beacon state not found (404)",
                                     endpoint=endpoint_name,
                                     error=error_text[:300])
                        raise BeaconAPIError(f"404 state not found: {endpoint_name}")
                    
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
                            raise BeaconAPIError(f"{response.status}: {error_text[:300]}")
                    
                    try:
                        return await response.json()
                    except Exception as e:
                        logger.error("API response JSON decode failed", url=url, error=str(e))
                        raise BeaconAPIError(f"Invalid JSON response from {endpoint_name}: {e}")
            
            except asyncio.TimeoutError:
                obs.api_requests_total.labels(endpoint=endpoint_name, status="timeout").inc()
                obs.api_request_duration_seconds.labels(
                    endpoint=endpoint_name
                ).observe(time.monotonic() - start_time)
                logger.warning("API request timeout", 
                             url=url, 
                             attempt=attempt + 1,
                             max_retries=self.max_retries)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error("API request timeout after all retries", url=url)
                    raise BeaconAPIError(f"Timeout requesting {endpoint_name}")
                    
            except BeaconAPIError:
                raise

            except Exception as e:
                obs.api_requests_total.labels(endpoint=endpoint_name, status="error").inc()
                obs.api_request_duration_seconds.labels(
                    endpoint=endpoint_name
                ).observe(time.monotonic() - start_time)
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
                    raise BeaconAPIError(f"Error requesting {endpoint_name}: {e}")
        
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
        params = None
        if validator_ids:
            # Convert list to comma-separated string
            params = {"id": ",".join(map(str, validator_ids))}
        return await self.get(endpoint, params=params, allow_empty_404=False)
    
    async def get_pending_consolidations(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
        """Get pending consolidations queue for a given state (Electra+).

        Returns the PendingConsolidation[] list from the beacon state — post-validation,
        pre-application queue. Entries here are guaranteed to apply (the spec's
        process_consolidation_request drops invalid requests BEFORE they enter the queue).

        The endpoint returns HTTP 400 for pre-Electra states; callers should fork-gate
        via config.ELECTRA_START_SLOT to avoid noisy 400s on backfills of old slots.
        """
        return await self.get(f"/eth/v1/beacon/states/{state_id}/pending_consolidations", allow_empty_404=False)

    async def get_pending_deposits(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
        """Get pending deposits queue for a given state (Electra+).

        Returns the PendingDeposit[] list — the churn-bounded queue of deposits awaiting
        activation. Each entry carries the exact `amount` that will be credited, which
        removes the reported-vs-credited ambiguity that execution_requests has post-Pectra
        (a single 2048-GNO request landing as ~1 GNO/day over weeks).
        """
        return await self.get(f"/eth/v1/beacon/states/{state_id}/pending_deposits", allow_empty_404=False)

    async def get_pending_partial_withdrawals(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
        """Get pending partial withdrawals queue for a given state (Electra+).

        Returns the PendingPartialWithdrawal[] list — scheduled partial withdrawals with
        their `withdrawable_epoch` and `amount`. Needed to distinguish scheduled partial
        withdrawals from full exits and from consolidation-driven sweeps.
        """
        return await self.get(f"/eth/v1/beacon/states/{state_id}/pending_partial_withdrawals", allow_empty_404=False)

    async def get_head_slot(self) -> Optional[int]:
        """Get the current head slot."""
        data = await self.get("/eth/v1/beacon/headers/head")
        if data and "data" in data:
            return int(data["data"]["header"]["message"]["slot"])
        return None
    
    async def get_rewards(self, slot: str = "head") -> Optional[Dict[str, Any]]:
        """Get rewards by slot."""
        return await self.get(f"/eth/v1/beacon/rewards/blocks/{slot}")

    async def get_data_column_sidecars(self, slot: int) -> Optional[Dict[str, Any]]:
        """Get Fulu data column sidecars by slot."""
        return await self.get(f"/eth/v1/debug/beacon/data_column_sidecars/{slot}")

    async def get_blobs(self, slot: int) -> Optional[Dict[str, Any]]:
        """Get blob data by slot."""
        return await self.get(f"/eth/v1/beacon/blobs/{slot}")
