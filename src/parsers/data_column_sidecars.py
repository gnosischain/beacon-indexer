import hashlib
import json
from typing import Any, Dict, List

from .base import BaseParser
from src import observability as obs


class DataColumnSidecarsParser(BaseParser):
    """Parse PeerDAS data column sidecars into query-friendly metadata."""

    def __init__(self):
        super().__init__("data_column_sidecars")

    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")

        try:
            payload = json.loads(payload_str) if isinstance(payload_str, str) else payload_str
        except (json.JSONDecodeError, TypeError):
            return {}

        sidecars = payload.get("data", [])
        if not sidecars:
            return {}

        rows = []
        for sidecar in sidecars:
            signed_header = sidecar.get("signed_block_header", {})
            header_message = signed_header.get("message", {})
            column = sidecar.get("column", [])
            commitments = sidecar.get("kzg_commitments", [])
            proofs = sidecar.get("kzg_proofs", [])

            row = {
                "slot": slot,
                "column_index": self._safe_int(sidecar.get("index"), 0),
                "column_cells": len(column) if isinstance(column, list) else 1 if column else 0,
                "column_bytes": self._hex_payload_bytes(column),
                "kzg_commitments_count": len(commitments) if isinstance(commitments, list) else 0,
                "kzg_proofs_count": len(proofs) if isinstance(proofs, list) else 0,
                "signed_block_slot": self._safe_int(header_message.get("slot"), 0),
                "proposer_index": self._safe_int(header_message.get("proposer_index"), 0),
                "body_root": str(header_message.get("body_root") or ""),
                "column_hash": self._hash_payload(column),
                "commitments_hash": self._hash_payload(commitments),
                "proofs_hash": self._hash_payload(proofs),
            }
            rows.append(row)

        obs.data_column_sidecars_transformed_total.inc(len(rows))
        return {"data_column_sidecars": rows}

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value) if value not in (None, "") else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _hash_payload(value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _hex_payload_bytes(value: Any) -> int:
        if isinstance(value, list):
            return sum(DataColumnSidecarsParser._hex_payload_bytes(item) for item in value)
        if isinstance(value, str):
            item = value[2:] if value.startswith("0x") else value
            return len(item) // 2
        return 0
