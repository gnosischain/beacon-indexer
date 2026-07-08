"""Loader for Electra+ pending_consolidations beacon-state queue.

Slot-based loader with per-slot fetch cadence, matching the existing RewardsLoader
pattern. Range-chunked via CHUNK_SIZE in src/services/loader.py's slot-based list.

Fork-gate: pre-Electra slots short-circuit to None in fetch_data so the beacon
node isn't hit with calls that would return HTTP 400. The BaseLoader's
load_single treats a None fetch as "no data" without raising.

Per-slot sampling is required because the pending_consolidations queue reflects
only entries currently waiting to apply — an entry added in one slot could be
processed at a subsequent epoch boundary, and we need the queue-state change
visible at that resolution for downstream dbt models to reconstruct lifecycles.
"""
import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader
from src.config import config


class PendingConsolidationsLoader(BaseLoader):
    """Loader for the EIP-7251 pending_consolidations queue."""

    def __init__(self, beacon_api, storage):
        super().__init__("pending_consolidations", beacon_api, storage)

    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        # Fork gate: the endpoint returns HTTP 400 pre-Electra. Skip silently so
        # historical backfills don't spam the error rate.
        if config.ELECTRA_START_SLOT and slot < config.ELECTRA_START_SLOT:
            return None
        return await self.beacon_api.get_pending_consolidations(str(slot))

    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": self.calculate_payload_hash(data),
            "retrieved_at": datetime.now(),
        }
