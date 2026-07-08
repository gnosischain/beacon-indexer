"""Tests for ValidatorsLoader.load_batch error propagation.

The loader overrides load_batch; a BeaconAPIError from the beacon API (e.g. a non-archive
endpoint returning 404 for a historical state) must surface as a raised error so the caller
marks the chunk failed — NOT be swallowed and counted as a processed slot (the silent-failure
bug that hid a 30-day validator gap).
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.loaders.validators import ValidatorsLoader
from src.services.beacon_api import BeaconAPIError


def _loader(mode="all_slots"):
    beacon_api = MagicMock()
    storage = MagicMock()
    # _init_timing_cache() queries this; [] keeps it a clean no-op for all_slots mode.
    storage.execute = MagicMock(return_value=[])
    with patch("src.loaders.validators.config") as cfg:
        cfg.VALIDATOR_MODE = mode
        loader = ValidatorsLoader(beacon_api, storage)
    return loader, beacon_api, storage


@pytest.mark.asyncio
async def test_load_batch_raises_on_beacon_api_error():
    loader, beacon_api, storage = _loader("all_slots")
    beacon_api.get_validators = AsyncMock(side_effect=BeaconAPIError("404 state not found"))

    with pytest.raises(RuntimeError):
        await loader.load_batch([100, 200, 300])

    # Nothing is written when the batch fails.
    storage.insert_batch.assert_not_called()


@pytest.mark.asyncio
async def test_load_batch_success_counts_and_stores():
    loader, beacon_api, storage = _loader("all_slots")
    beacon_api.get_validators = AsyncMock(return_value={"data": [{"index": "0"}]})

    result = await loader.load_batch([100, 200])

    assert result == 2
    assert storage.insert_batch.called


@pytest.mark.asyncio
async def test_load_batch_generic_error_is_not_fatal():
    # A non-BeaconAPIError (e.g. a transient parse issue) is logged per-slot but does not
    # raise — only genuine beacon API errors fail the chunk.
    loader, beacon_api, storage = _loader("all_slots")
    beacon_api.get_validators = AsyncMock(side_effect=ValueError("boom"))

    result = await loader.load_batch([100, 200])

    assert result == 0
    storage.insert_batch.assert_not_called()
