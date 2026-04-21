import json

from src.config import config
from src.parsers.data_column_sidecars import DataColumnSidecarsParser
from src.services.fork import ForkDetectionService


class FakeClickHouse:
    def execute(self, query, params=None):
        if "CONFIG_NAME" in query:
            return [{"parameter_value": "gnosis"}]
        if "SECONDS_PER_SLOT" in query:
            return [
                {"parameter_name": "SECONDS_PER_SLOT", "parameter_value": "5"},
                {"parameter_name": "SLOTS_PER_EPOCH", "parameter_value": "16"},
            ]
        if "toUnixTimestamp(genesis_time)" in query:
            return [{"genesis_time_unix": 1638993340}]
        if "LIKE '%_FORK_EPOCH'" in query:
            return [
                {"parameter_name": "ALTAIR_FORK_EPOCH", "parameter_value": "512"},
                {"parameter_name": "BELLATRIX_FORK_EPOCH", "parameter_value": "385536"},
                {"parameter_name": "CAPELLA_FORK_EPOCH", "parameter_value": "648704"},
                {"parameter_name": "DENEB_FORK_EPOCH", "parameter_value": "889856"},
                {"parameter_name": "ELECTRA_FORK_EPOCH", "parameter_value": "1337856"},
                {"parameter_name": "FULU_FORK_EPOCH", "parameter_value": "18446744073709551615"},
            ]
        return []


def test_gnosis_fulu_slot_resolution_overrides_stale_specs():
    service = ForkDetectionService(clickhouse_client=FakeClickHouse())

    assert service.get_fork_at_slot(27435007).name == "electra"
    assert service.get_fork_at_slot(27435008).name == "fulu"
    assert service.get_fork_at_slot(27435008).version == "0x06000064"


def test_data_column_sidecars_parser_metadata():
    parser = DataColumnSidecarsParser()
    payload = {
        "data": [
            {
                "index": "2",
                "column": ["0xaaaa", "0xbbbb"],
                "kzg_commitments": ["0x11", "0x22"],
                "kzg_proofs": ["0x33"],
                "signed_block_header": {
                    "message": {
                        "slot": "27435008",
                        "proposer_index": "42",
                        "body_root": "0xabc",
                    }
                },
            }
        ]
    }

    result = parser.parse({"slot": config.FULU_START_SLOT, "payload": json.dumps(payload)})
    row = result["data_column_sidecars"][0]

    assert row["slot"] == 27435008
    assert row["column_index"] == 2
    assert row["column_cells"] == 2
    assert row["column_bytes"] == 4
    assert row["kzg_commitments_count"] == 2
    assert row["kzg_proofs_count"] == 1
    assert row["signed_block_slot"] == 27435008
    assert row["proposer_index"] == 42
