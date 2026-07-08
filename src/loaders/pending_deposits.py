"""Loader for Electra+ pending_deposits beacon-state queue.

Slot-based loader with per-slot fetch cadence, matching the existing RewardsLoader
pattern. Each PendingDeposit entry carries the EXACT `amount` the beacon chain
will credit (not the reported request amount in execution_requests).

Fork-gated: pre-Electra slots short-circuit to None in fetch_data.
"""
import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader
from src.config import config


class PendingDepositsLoader(BaseLoader):
    """Loader for the EIP-7251 pending_deposits queue."""

    def __init__(self, beacon_api, storage):
        super().__init__("pending_deposits", beacon_api, storage)

    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        if config.ELECTRA_START_SLOT and slot < config.ELECTRA_START_SLOT:
            return None
        return await self.beacon_api.get_pending_deposits(str(slot))

    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": self.calculate_payload_hash(data),
            "retrieved_at": datetime.now(),
        }
