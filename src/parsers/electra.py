import json
from typing import Dict, List, Any, Optional
from .deneb import DenebParser

class ElectraParser(DenebParser):
    """Electra parser: adds execution_requests + pending-queue state tables.

    Electra introduces:
      * execution_requests — EIP-7002 / EIP-7251 request events included in blocks.
      * pending_consolidations — BeaconState queue of post-validation consolidations.
      * pending_deposits — BeaconState queue of post-validation deposits awaiting
        activation churn (carries the EXACT credit amount per entry).
      * pending_partial_withdrawals — BeaconState queue of EIP-7002 partial
        withdrawals with their withdrawable_epoch.

    The pending_* tables are populated from separate beacon-state endpoints
    (scraped by PendingConsolidationsLoader / PendingDepositsLoader /
    PendingPartialWithdrawalsLoader). Their raw payloads arrive in the parser
    via `raw_data` keyed by source — parse_fork_specific dispatches on the
    source table name.
    """

    def __init__(self):
        super().__init__()
        self.fork_name = "electra"

    def get_supported_tables(self) -> List[str]:
        """Electra adds one block-derived table + three state-queue tables."""
        return super().get_supported_tables() + [
            "execution_requests",
            "pending_consolidations",
            "pending_deposits",
            "pending_partial_withdrawals",
        ]

    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Route based on source table.

        The transformer invokes parsers per raw source. For blocks-derived tables
        the parent class's slot-based parse flow applies. For state-queue tables
        the raw payload is a flat `data: [...]` list we unpack directly.
        """
        source_table = raw_data.get("_source_table", "")
        if source_table == "raw_pending_consolidations":
            return self._parse_pending_consolidations(raw_data)
        if source_table == "raw_pending_deposits":
            return self._parse_pending_deposits(raw_data)
        if source_table == "raw_pending_partial_withdrawals":
            return self._parse_pending_partial_withdrawals(raw_data)
        # Otherwise defer to the blocks-based parse (executes parse_fork_specific).
        return super().parse(raw_data)

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

    # --- pending-queue parsers -------------------------------------------------

    def _load_payload(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Decode the JSON payload from a raw row. Returns {} on parse error."""
        payload = raw_data.get("payload", "{}")
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                return {}
        return payload or {}

    def _parse_pending_consolidations(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """PendingConsolidation[] → {source_index, target_index} rows per snapshot slot."""
        slot = raw_data.get("slot", 0)
        payload = self._load_payload(raw_data)
        entries = payload.get("data", []) or []
        rows = [
            {
                "slot": slot,
                "source_index": self.safe_int(e.get("source_index")),
                "target_index": self.safe_int(e.get("target_index")),
            }
            for e in entries
        ]
        return {"pending_consolidations": rows}

    def _parse_pending_deposits(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """PendingDeposit[] → {pubkey, withdrawal_credentials, amount, signature, deposit_slot} rows."""
        slot = raw_data.get("slot", 0)
        payload = self._load_payload(raw_data)
        entries = payload.get("data", []) or []
        rows = [
            {
                "slot": slot,
                "pubkey": self.safe_str(e.get("pubkey")),
                "withdrawal_credentials": self.safe_str(e.get("withdrawal_credentials")),
                "amount": self.safe_int(e.get("amount")),
                "signature": self.safe_str(e.get("signature")),
                "deposit_slot": self.safe_int(e.get("slot")),
            }
            for e in entries
        ]
        return {"pending_deposits": rows}

    def _parse_pending_partial_withdrawals(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """PendingPartialWithdrawal[] → {validator_index, amount, withdrawable_epoch} rows."""
        slot = raw_data.get("slot", 0)
        payload = self._load_payload(raw_data)
        entries = payload.get("data", []) or []
        rows = [
            {
                "slot": slot,
                "validator_index": self.safe_int(e.get("validator_index")),
                "amount": self.safe_int(e.get("amount")),
                "withdrawable_epoch": self.safe_int(e.get("withdrawable_epoch")),
            }
            for e in entries
        ]
        return {"pending_partial_withdrawals": rows}

    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Electra (mainnet)."""
        return "0x05000000"