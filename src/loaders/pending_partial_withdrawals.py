"""Loader for Electra+ pending_partial_withdrawals beacon-state queue.

Slot-based loader with per-slot fetch cadence, matching the existing RewardsLoader
pattern. Captures scheduled EIP-7002 partial withdrawals with their
`withdrawable_epoch` and `amount`.

Fork-gated: pre-Electra slots short-circuit to None in fetch_data.
"""
import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader
from src.config import config


class PendingPartialWithdrawalsLoader(BaseLoader):
    """Loader for the EIP-7002 pending_partial_withdrawals queue."""

    def __init__(self, beacon_api, storage):
        super().__init__("pending_partial_withdrawals", beacon_api, storage)

    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        if config.ELECTRA_START_SLOT and slot < config.ELECTRA_START_SLOT:
            return None
        return await self.beacon_api.get_pending_partial_withdrawals(str(slot))

    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": self.calculate_payload_hash(data),
            "retrieved_at": datetime.now(),
        }
