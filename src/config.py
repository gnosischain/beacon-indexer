import os
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Beacon Node
    BEACON_NODE_URL = os.getenv("BEACON_NODE_URL", "http://localhost:5052")
    # Optional API key for gated / archive beacon endpoints. When set, it is sent as a
    # query parameter (name from BEACON_API_KEY_PARAM, default "apiKey") on every beacon
    # API request. Leave empty for nodes needing no auth or that carry credentials in the
    # URL path itself (e.g. gateway-style "https://host/<key>" URLs).
    BEACON_API_KEY = os.getenv("BEACON_API_KEY", "")
    BEACON_API_KEY_PARAM = os.getenv("BEACON_API_KEY_PARAM", "apiKey")

    # Storage Backend - either 'clickhouse' or 'parquet'
    STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "clickhouse")
    
    # ClickHouse (only used if STORAGE_BACKEND=clickhouse)
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "beacon_chain")
    CLICKHOUSE_SECURE = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
    
    # Parquet (only used if STORAGE_BACKEND=parquet)
    PARQUET_OUTPUT_DIR = os.getenv("PARQUET_OUTPUT_DIR", "./parquet_data")
    PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")
    PARQUET_ROWS_PER_FILE = int(os.getenv("PARQUET_ROWS_PER_FILE", "100000"))
    
    # Loaders - ensure all loaders are enabled by default
    ENABLED_LOADERS = os.getenv("ENABLED_LOADERS", "blocks,validators,specs,genesis").split(",")
    
    # Validator Configuration
    VALIDATOR_MODE = os.getenv("VALIDATOR_MODE", "daily")  # daily or all_slots
    
    # Backfill Configuration
    START_SLOT = int(os.getenv("START_SLOT", "0"))
    END_SLOT = int(os.getenv("END_SLOT", "0")) if os.getenv("END_SLOT") else None
    BACKFILL_WORKERS = int(os.getenv("BACKFILL_WORKERS", "4"))
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "100"))

    # Realtime Configuration
    REALTIME_SLOT_DELAY = int(os.getenv("REALTIME_SLOT_DELAY", "0"))

    # Fulu/Fusaka configuration for Gnosis mainnet
    FULU_START_SLOT = int(os.getenv("FULU_START_SLOT", "27435008"))

    # Electra activation slot — gates the pending_consolidations / pending_deposits /
    # pending_partial_withdrawals loaders. Pre-Electra slots return HTTP 400 from those
    # endpoints; the fork gate lets historical backfills run without error spam.
    # Defaults to 0 (disabled; try the call on every slot). Deployments should set:
    #   - Gnosis mainnet: the Gnosis Electra activation slot
    #   - Ethereum mainnet: 11649024
    ELECTRA_START_SLOT = int(os.getenv("ELECTRA_START_SLOT", "0"))

    # Transform Configuration — parallelism/batch sizes for the continuous transformer.
    # Defaults match the historical hardcoded values. Lower TRANSFORM_CHUNKS_PER_BATCH for
    # large-payload loaders (e.g. validators): each validators chunk parses a full
    # ~400k-entry state, so batching many in parallel OOMs the transform pod.
    TRANSFORM_CHUNKS_PER_BATCH      = int(os.getenv("TRANSFORM_CHUNKS_PER_BATCH", "10"))
    TRANSFORM_CHUNKS_PER_FETCH      = int(os.getenv("TRANSFORM_CHUNKS_PER_FETCH", "50"))
    TRANSFORM_MAX_CONCURRENT_WRITES = int(os.getenv("TRANSFORM_MAX_CONCURRENT_WRITES", "4"))

    # Metrics
    METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
    
    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

config = Config()
