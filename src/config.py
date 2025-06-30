import os
from pydantic import BaseModel, Field
from typing import Optional, List
from dotenv import load_dotenv
from enum import Enum

load_dotenv()


class OperationType(Enum):
    """Types of operations the indexer can perform."""
    CONTINUOUS = "continuous"
    HISTORICAL = "historical"
    FILL_GAPS = "fill_gaps"
    VALIDATE = "validate"
    BACKFILL = "backfill"
    CONSOLIDATE = "consolidate"
    PROCESS_FAILED = "process_failed"


class BeaconNodeConfig(BaseModel):
    url: str = Field(default=os.getenv("BEACON_NODE_URL", "http://localhost:5052"))


class ClickHouseConfig(BaseModel):
    host: str = Field(default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = Field(default=int(os.getenv("CLICKHOUSE_PORT", "9000")))
    user: str = Field(default=os.getenv("CLICKHOUSE_USER", "default"))
    password: str = Field(default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = Field(default=os.getenv("CLICKHOUSE_DATABASE", "beacon_chain"))
    
    @property
    def secure(self) -> bool:
        """Determine if connection should be secure based on port or env var."""
        env_secure = os.getenv("CLICKHOUSE_SECURE")
        if env_secure is not None:
            return env_secure.lower() in ("true", "1", "yes")
        return self.port == 443 or self.port == 8443
    
    @property
    def verify(self) -> bool:
        """Determine if SSL certificates should be verified."""
        return os.getenv("CLICKHOUSE_VERIFY", "false").lower() in ("true", "1", "yes")


class OperationConfig(BaseModel):
    """Configuration for operation modes."""
    operation_type: OperationType = Field(
        default=OperationType(os.getenv("OPERATION", "continuous").lower())
    )
    datasets: List[str] = Field(default_factory=list)
    force: bool = Field(default=os.getenv("FORCE", "false").lower() == "true")
    
    # Continuous mode
    confirmation_blocks: int = Field(default=int(os.getenv("CONFIRMATION_BLOCKS", "12")))
    poll_interval: int = Field(default=int(os.getenv("POLL_INTERVAL", "10")))
    
    # Historical mode
    start_slot: int = Field(default=int(os.getenv("START_SLOT", "0")))
    end_slot: Optional[int] = Field(
        default=int(os.getenv("END_SLOT", "0")) if os.getenv("END_SLOT") else None
    )
    
    # Fill gaps mode
    min_gap_size: int = Field(default=int(os.getenv("MIN_GAP_SIZE", "1")))
    handle_failed_ranges: bool = Field(
        default=os.getenv("HANDLE_FAILED_RANGES", "true").lower() == "true"
    )
    
    # Validate mode
    validation_types: List[str] = Field(default_factory=lambda: ["completeness", "consistency"])
    fix_issues: bool = Field(default=os.getenv("FIX_ISSUES", "false").lower() == "true")
    
    # Backfill mode
    chunk_size: int = Field(default=int(os.getenv("BACKFILL_CHUNK_SIZE", "100000")))
    
    # Consolidate mode
    dry_run: bool = Field(default=os.getenv("DRY_RUN", "false").lower() == "true")
    
    # Process failed mode
    delete_before_retry: bool = Field(
        default=os.getenv("DELETE_BEFORE_RETRY", "true").lower() == "true"
    )
    
    def __init__(self, **data):
        super().__init__(**data)
        # Parse datasets from environment
        datasets_str = os.getenv("DATASETS", "")
        if datasets_str and not self.datasets:
            self.datasets = [d.strip() for d in datasets_str.split(",") if d.strip()]


class ScraperConfig(BaseModel):
    # Scraper specific settings
    mode: str = Field(default=os.getenv("SCRAPER_MODE", "realtime"))
    
    # Realtime settings
    poll_interval: int = Field(default=int(os.getenv("POLL_INTERVAL", "10")))
    lag_threshold: int = Field(default=int(os.getenv("LAG_THRESHOLD", "3")))
    
    # Historical settings
    historical_start_slot: int = Field(default=int(os.getenv("HISTORICAL_START_SLOT", "0")))
    historical_end_slot: Optional[int] = Field(
        default=int(os.getenv("HISTORICAL_END_SLOT", "0")) if os.getenv("HISTORICAL_END_SLOT") else None
    )
    
    # Parallel settings
    num_workers: int = Field(default=int(os.getenv("NUM_WORKERS", os.getenv("PARALLEL_WORKERS", "4"))))
    parallel_workers: int = Field(default=int(os.getenv("PARALLEL_WORKERS", os.getenv("NUM_WORKERS", "4"))))
    range_size: int = Field(default=int(os.getenv("RANGE_SIZE", "100000")))
    worker_batch_size: int = Field(default=int(os.getenv("WORKER_BATCH_SIZE", "1000")))
    
    # Common settings
    batch_size: int = Field(default=int(os.getenv("BATCH_SIZE", "1000")))
    max_concurrent_requests: int = Field(default=int(os.getenv("MAX_CONCURRENT_REQUESTS", "100")))
    enabled_scrapers: str = Field(default=os.getenv("ENABLED_SCRAPERS", "core_block,operational_events,validator"))
    default_batch_size: int = Field(default=int(os.getenv("DEFAULT_BATCH_SIZE", "1000")))
    
    # Logging
    log_level: str = Field(default=os.getenv("LOG_LEVEL", "INFO"))
    
    # Specs settings
    specs_check_interval: int = Field(default=int(os.getenv("SPECS_CHECK_INTERVAL", "3600")))
    
    # Validator scraper settings
    validator_processor_mode: str = Field(default=os.getenv("VALIDATOR_PROCESSOR_MODE", "efficient"))
    validator_batch_size: int = Field(default=int(os.getenv("VALIDATOR_BATCH_SIZE", "100")))
    
    # Rewards scraper settings
    reward_batch_size: int = Field(default=int(os.getenv("REWARD_BATCH_SIZE", "1000")))
    
    # Bulk insertion service
    bulk_insert_queue_size: int = Field(default=int(os.getenv("BULK_INSERT_QUEUE_SIZE", "10000")))
    bulk_insert_max_wait_time: int = Field(default=int(os.getenv("BULK_INSERT_MAX_WAIT_TIME", "5")))
    bulk_insert_batch_size: int = Field(default=int(os.getenv("BULK_INSERT_BATCH_SIZE", "5000")))


class Config(BaseModel):
    beacon_node: BeaconNodeConfig = Field(default_factory=BeaconNodeConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    scraper: ScraperConfig = Field(default_factory=ScraperConfig)
    operation: OperationConfig = Field(default_factory=OperationConfig)


# Create the config object
config = Config()

# Default scraper combinations
DEFAULT_SCRAPER_SETS = {
    "minimal": ["core_block"],
    "basic": ["core_block", "operational_events", "validator", "transaction"],
    "validators_only": ["validator"],
    "rewards_only": ["reward"],
    "attestations_only": ["attestation"],
    "slashing_monitoring": ["slashing", "operational_events"],
    "blob_data": ["blob"],
    "specs_update": ["specs"],
    "extended": ["core_block", "attestation", "operational_events", "validator", "reward", "slashing", "transaction"],
    "full": ["core_block", "attestation", "operational_events", "validator", "reward", "slashing", "transaction", "blob"],
    "legacy": ["block"],
    "legacy_full": ["block", "validator", "reward", "blob", "specs"]
}


def get_enabled_scrapers(scraper_set: Optional[str] = None, custom_scrapers: Optional[str] = None) -> list:
    """Get the list of enabled scrapers based on scraper set or custom list."""
    if scraper_set and scraper_set in DEFAULT_SCRAPER_SETS:
        return DEFAULT_SCRAPER_SETS[scraper_set]
    elif custom_scrapers:
        return [s.strip() for s in custom_scrapers.split(',') if s.strip()]
    else:
        # Get scrapers from config
        scrapers_str = config.scraper.enabled_scrapers
        scrapers = [s.strip() for s in scrapers_str.split(',') if s.strip()]
        
        # If it's a single item that matches a scraper set, expand it
        if len(scrapers) == 1 and scrapers[0] in DEFAULT_SCRAPER_SETS:
            return DEFAULT_SCRAPER_SETS[scrapers[0]]
        
        return scrapers