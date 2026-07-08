"""Tests for the BeaconAPI client — API-key injection and 404 semantics.

Covers:
  1. The configured API key is attached as a query parameter on every request.
  2. No `params` are sent when no API key is configured.
  3. A 404 on a block-style endpoint returns None (missed slot — normal).
  4. A 404 on a beacon-state endpoint raises BeaconAPIError (fail loudly, no silent
     zero-row "success").
  5. get_validators wires the state semantics (allow_empty_404=False) and passes the
     validator id filter via params (merged with the API key), not string-concatenation.
"""
from unittest.mock import MagicMock, patch

import pytest

import src.services.beacon_api as beacon_module
from src.services.beacon_api import BeaconAPI, BeaconAPIError


class _FakeResp:
    def __init__(self, status, json_data=None, text_data=""):
        self.status = status
        self._json = json_data
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text


class _FakeCM:
    """Stand-in for the async context manager returned by aiohttp's session.get()."""

    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *exc):
        return False


def _api_with_response(resp):
    api = BeaconAPI()
    api.session = MagicMock()
    api.session.get = MagicMock(return_value=_FakeCM(resp))
    return api


@pytest.mark.asyncio
async def test_api_key_attached_as_query_param():
    api = _api_with_response(_FakeResp(200, {"data": "ok"}))
    with patch.object(beacon_module.config, "BEACON_API_KEY", "secret-key"), \
         patch.object(beacon_module.config, "BEACON_API_KEY_PARAM", "apiKey"):
        result = await api.get("/eth/v1/beacon/genesis")
    assert result == {"data": "ok"}
    _, kwargs = api.session.get.call_args
    assert kwargs["params"] == {"apiKey": "secret-key"}


@pytest.mark.asyncio
async def test_no_params_when_api_key_unset():
    api = _api_with_response(_FakeResp(200, {"data": "ok"}))
    with patch.object(beacon_module.config, "BEACON_API_KEY", ""):
        await api.get("/eth/v1/beacon/genesis")
    _, kwargs = api.session.get.call_args
    assert kwargs["params"] is None


@pytest.mark.asyncio
async def test_block_endpoint_404_returns_none():
    api = _api_with_response(_FakeResp(404, text_data="not found"))
    with patch.object(beacon_module.config, "BEACON_API_KEY", ""):
        # allow_empty_404 defaults to True — a missed block slot is normal.
        result = await api.get("/eth/v2/beacon/blocks/123")
    assert result is None


@pytest.mark.asyncio
async def test_state_endpoint_404_raises():
    api = _api_with_response(_FakeResp(404, text_data="NOT_FOUND: beacon state at slot 123"))
    with patch.object(beacon_module.config, "BEACON_API_KEY", ""):
        with pytest.raises(BeaconAPIError):
            await api.get("/eth/v1/beacon/states/123/validators", allow_empty_404=False)


@pytest.mark.asyncio
async def test_get_validators_state_semantics_and_id_param():
    # A 404 on the validators state endpoint must raise (proves allow_empty_404=False is wired).
    api = _api_with_response(_FakeResp(404, text_data="NOT_FOUND"))
    with patch.object(beacon_module.config, "BEACON_API_KEY", ""):
        with pytest.raises(BeaconAPIError):
            await api.get_validators("123")

    # The id filter goes into params (not concatenated onto the URL) and merges with the key.
    api2 = _api_with_response(_FakeResp(200, {"data": []}))
    with patch.object(beacon_module.config, "BEACON_API_KEY", "k"), \
         patch.object(beacon_module.config, "BEACON_API_KEY_PARAM", "apiKey"):
        await api2.get_validators("head", validator_ids=[1, 2, 3])
    _, kwargs = api2.session.get.call_args
    assert kwargs["params"] == {"id": "1,2,3", "apiKey": "k"}
