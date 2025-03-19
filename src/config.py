import os
from pydantic import BaseModel, Field
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class BeaconNodeConfig(BaseModel):
    url: str = Field(default=os.getenv("BEACON_NODE_URL", "http://localhost:5052"))

class ClickHouseConfig(BaseModel):
    host: str = Field(default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    port: int = Field(default=int(os.getenv("CLICKHOUSE_PORT", "9000")))
    user: str = Field(default=os.getenv("CLICKHOUSE_USER", "default"))
    password: str = Field(default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    database: str = Field(default=os.getenv("CLICKHOUSE_DATABASE", "beacon_chain"))

class ScraperConfig(BaseModel):
    mode: str = Field(default=os.getenv("SCRAPER_MODE", "realtime"))
    historical_start_slot: int = Field(default=int(os.getenv("HISTORICAL_START_SLOT", "0")))
    historical_end_slot: Optional[int] = Field(
        default=int(os.getenv("HISTORICAL_END_SLOT", "0")) if os.getenv("HISTORICAL_END_SLOT") else None
    )
    batch_size: int = Field(default=int(os.getenv("BATCH_SIZE", "100")))
    max_concurrent_requests: int = Field(default=int(os.getenv("MAX_CONCURRENT_REQUESTS", "5")))

class Config(BaseModel):
    beacon_node: BeaconNodeConfig = Field(default_factory=BeaconNodeConfig)
    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    scraper: ScraperConfig = Field(default_factory=ScraperConfig)

# Create the config object
config = Config()