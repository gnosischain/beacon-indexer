import json
from datetime import datetime
from typing import Any, Dict, Optional

from .base import BaseLoader
from src.config import config
from src import observability as obs


class DataColumnSidecarsLoader(BaseLoader):
    """Loader for Fulu PeerDAS data column sidecars."""

    def __init__(self, beacon_api, clickhouse):
        super().__init__("data_column_sidecars", beacon_api, clickhouse)

    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch data column sidecars for Fulu and later slots."""
        if slot < config.FULU_START_SLOT:
            return None
        return await self.beacon_api.get_data_column_sidecars(slot)

    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare raw data column sidecar payload for insertion."""
        payload_hash = self.calculate_payload_hash(data)
        sidecars = data.get("data", [])
        obs.data_column_sidecars_fetched_total.labels(
            status="non_empty" if sidecars else "empty"
        ).inc()
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": payload_hash,
            "retrieved_at": datetime.now()
        }
