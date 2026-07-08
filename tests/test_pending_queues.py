"""Tests for the Electra pending-queue loaders + parser.

Covers:
  1. Parser correctly unpacks each queue's payload shape.
  2. Empty queues produce zero rows without raising.
  3. Malformed JSON is handled gracefully.
  4. Loader fork-gate (fetch_data returns None pre-Electra, no API call).
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.electra import ElectraParser
from src.loaders.pending_consolidations import PendingConsolidationsLoader
from src.loaders.pending_deposits import PendingDepositsLoader
from src.loaders.pending_partial_withdrawals import PendingPartialWithdrawalsLoader


# ---------- Parser tests ----------

def test_parse_pending_consolidations_unpacks_entries():
    parser = ElectraParser()
    payload = {
        "data": [
            {"source_index": "100", "target_index": "200"},
            {"source_index": "101", "target_index": "200"},
        ]
    }
    result = parser.parse({
        "_source_table": "raw_pending_consolidations",
        "slot": 27_500_000,
        "payload": json.dumps(payload),
    })
    rows = result["pending_consolidations"]
    assert len(rows) == 2
    assert rows[0] == {"slot": 27_500_000, "source_index": 100, "target_index": 200}
    assert rows[1] == {"slot": 27_500_000, "source_index": 101, "target_index": 200}


def test_parse_pending_consolidations_empty_queue_is_zero_rows():
    parser = ElectraParser()
    result = parser.parse({
        "_source_table": "raw_pending_consolidations",
        "slot": 27_500_000,
        "payload": json.dumps({"data": []}),
    })
    assert result == {"pending_consolidations": []}


def test_parse_pending_deposits_preserves_amount_and_deposit_slot():
    parser = ElectraParser()
    payload = {
        "data": [
            {
                "pubkey": "0xabc123",
                "withdrawal_credentials": "0x0200deadbeef",
                "amount": "1000000000",       # 1 GNO in Gwei
                "signature": "0xsig",
                "slot": "27400000",           # PendingDeposit.slot — distinct from snapshot slot
            }
        ]
    }
    result = parser.parse({
        "_source_table": "raw_pending_deposits",
        "slot": 27_500_000,
        "payload": json.dumps(payload),
    })
    rows = result["pending_deposits"]
    assert len(rows) == 1
    r = rows[0]
    assert r["slot"] == 27_500_000
    assert r["pubkey"] == "0xabc123"
    assert r["amount"] == 1_000_000_000
    assert r["deposit_slot"] == 27_400_000


def test_parse_pending_partial_withdrawals_unpacks_entries():
    parser = ElectraParser()
    payload = {
        "data": [
            {"validator_index": "5001", "amount": "50000000", "withdrawable_epoch": "1500000"},
        ]
    }
    result = parser.parse({
        "_source_table": "raw_pending_partial_withdrawals",
        "slot": 27_500_000,
        "payload": json.dumps(payload),
    })
    assert result["pending_partial_withdrawals"] == [
        {"slot": 27_500_000, "validator_index": 5001,
         "amount": 50_000_000, "withdrawable_epoch": 1_500_000}
    ]


def test_parse_malformed_payload_returns_empty_rows():
    parser = ElectraParser()
    result = parser.parse({
        "_source_table": "raw_pending_consolidations",
        "slot": 27_500_000,
        "payload": "this is not json",
    })
    assert result == {"pending_consolidations": []}


def test_supported_tables_includes_pending_queues():
    parser = ElectraParser()
    tables = parser.get_supported_tables()
    assert "pending_consolidations" in tables
    assert "pending_deposits" in tables
    assert "pending_partial_withdrawals" in tables


# ---------- Loader fork-gate tests ----------

@pytest.mark.asyncio
async def test_pending_consolidations_fork_gate_skips_pre_electra():
    """Pre-Electra fetch returns None without an API call."""
    beacon_api = MagicMock()
    beacon_api.get_pending_consolidations = AsyncMock(return_value={"data": []})
    storage = MagicMock()

    with patch("src.loaders.pending_consolidations.config") as cfg:
        cfg.ELECTRA_START_SLOT = 27_000_000
        loader = PendingConsolidationsLoader(beacon_api, storage)
        result = await loader.fetch_data(26_999_999)
        assert result is None
        beacon_api.get_pending_consolidations.assert_not_called()


@pytest.mark.asyncio
async def test_pending_consolidations_fork_gate_fires_at_and_after_electra():
    beacon_api = MagicMock()
    beacon_api.get_pending_consolidations = AsyncMock(return_value={"data": []})
    storage = MagicMock()

    with patch("src.loaders.pending_consolidations.config") as cfg:
        cfg.ELECTRA_START_SLOT = 27_000_000
        loader = PendingConsolidationsLoader(beacon_api, storage)

        # Exactly at activation: hits the API.
        await loader.fetch_data(27_000_000)
        # After activation: hits the API.
        await loader.fetch_data(27_500_000)
        assert beacon_api.get_pending_consolidations.call_count == 2


@pytest.mark.asyncio
async def test_pending_consolidations_fork_gate_disabled_with_zero():
    """ELECTRA_START_SLOT=0 means no gating — attempt every slot."""
    beacon_api = MagicMock()
    beacon_api.get_pending_consolidations = AsyncMock(return_value={"data": []})
    storage = MagicMock()

    with patch("src.loaders.pending_consolidations.config") as cfg:
        cfg.ELECTRA_START_SLOT = 0
        loader = PendingConsolidationsLoader(beacon_api, storage)
        await loader.fetch_data(1000)
        beacon_api.get_pending_consolidations.assert_called_once_with("1000")


@pytest.mark.asyncio
async def test_pending_deposits_fork_gate():
    beacon_api = MagicMock()
    beacon_api.get_pending_deposits = AsyncMock(return_value={"data": []})
    storage = MagicMock()

    with patch("src.loaders.pending_deposits.config") as cfg:
        cfg.ELECTRA_START_SLOT = 27_000_000
        loader = PendingDepositsLoader(beacon_api, storage)
        assert await loader.fetch_data(26_999_999) is None
        beacon_api.get_pending_deposits.assert_not_called()
        await loader.fetch_data(27_500_000)
        beacon_api.get_pending_deposits.assert_called_once()


@pytest.mark.asyncio
async def test_pending_partial_withdrawals_fork_gate():
    beacon_api = MagicMock()
    beacon_api.get_pending_partial_withdrawals = AsyncMock(return_value={"data": []})
    storage = MagicMock()

    with patch("src.loaders.pending_partial_withdrawals.config") as cfg:
        cfg.ELECTRA_START_SLOT = 27_000_000
        loader = PendingPartialWithdrawalsLoader(beacon_api, storage)
        assert await loader.fetch_data(26_999_999) is None
        beacon_api.get_pending_partial_withdrawals.assert_not_called()
        await loader.fetch_data(27_500_000)
        beacon_api.get_pending_partial_withdrawals.assert_called_once()


# ---------- prepare_row round-trip ----------

def test_prepare_row_shape_matches_raw_tables():
    beacon_api = MagicMock()
    storage = MagicMock()
    loader = PendingConsolidationsLoader(beacon_api, storage)

    row = loader.prepare_row(27_500_000, {"data": [{"source_index": "1", "target_index": "2"}]})
    assert set(row.keys()) == {"slot", "payload", "payload_hash", "retrieved_at"}
    assert row["slot"] == 27_500_000
    assert "source_index" in row["payload"]
    # Payload hash is 16 hex chars (64 bits) — matches BaseLoader contract.
    assert len(row["payload_hash"]) == 16
