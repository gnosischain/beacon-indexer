# Development Guide

This guide covers extending the Beacon Chain Indexer with new functionality, from adding API endpoints to creating custom parsers and storage backends.

## Development Setup

### Local Environment

```bash
# Clone repository
git clone <repository-url>
cd beacon-indexer

# Install dependencies
pip install -r requirements.txt

# Set up development environment
cp .env.example .env
echo "STORAGE_BACKEND=parquet" >> .env
echo "LOG_LEVEL=DEBUG" >> .env
echo "START_SLOT=9000000" >> .env
echo "END_SLOT=9001000" >> .env

# Run tests
make dev-backfill
make dev-transform
```

### Project Structure

```
beacon-indexer/
├── src/
│   ├── config.py                 # Configuration management
│   ├── cli.py                    # Command line interface
│   ├── main.py                   # Entry point
│   ├── utils/
│   │   └── logger.py             # Logging utilities
│   ├── services/
│   │   ├── beacon_api.py         # Beacon API client
│   │   ├── clickhouse.py         # ClickHouse storage backend
│   │   ├── parquet_storage.py    # Parquet storage backend
│   │   ├── storage_factory.py    # Storage backend factory
│   │   ├── fork.py               # Fork detection (ClickHouse)
│   │   ├── loader.py             # Data loading service
│   │   └── transformer.py        # Data transformation service
│   ├── loaders/
│   │   ├── __init__.py           # Loader registry
│   │   ├── base.py               # Base loader class
│   │   ├── blocks.py             # Blocks loader
│   │   ├── validators.py         # Validators loader
│   │   ├── rewards.py            # Rewards loader
│   │   ├── specs.py              # Specs loader
│   │   └── genesis.py            # Genesis loader
│   └── parsers/
│       ├── __init__.py           # Parser registry
│       ├── factory.py            # Fork-aware parser factory
│       ├── fork_base.py          # Base fork parser
│       ├── phase0.py             # Phase 0 parser
│       ├── altair.py             # Altair parser
│       ├── bellatrix.py          # Bellatrix parser
│       ├── capella.py            # Capella parser
│       ├── deneb.py              # Deneb parser
│       └── electra.py            # Electra parser
├── migrations/                   # ClickHouse migrations
├── scripts/                      # Utility scripts
├── config/                       # Configuration files
└── docs/                         # Documentation
```

## Adding New API Endpoints

### Step 1: Add API Method to BeaconAPI

Add the new endpoint method to `src/services/beacon_api.py`:

```python
async def get_committees(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
    """Get committees for a given state."""
    return await self.get(f"/eth/v1/beacon/states/{state_id}/committees")

async def get_node_version(self) -> Optional[Dict[str, Any]]:
    """Get beacon node version information.""" 
    return await self.get("/eth/v1/node/version")

async def get_sync_status(self) -> Optional[Dict[str, Any]]:
    """Get beacon node sync status."""
    return await self.get("/eth/v1/node/syncing")
```

### Step 2: Create Raw Table Migration (ClickHouse)

Create `migrations/012_raw_committees.sql`:

```sql
-- Raw committees data
CREATE TABLE IF NOT EXISTS raw_committees (
    slot UInt64,
    payload String,
    payload_hash String,
    retrieved_at DateTime DEFAULT now(),
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    )
) ENGINE = ReplacingMergeTree(retrieved_at)  
ORDER BY (slot, payload_hash)
PARTITION BY toStartOfMonth(slot_timestamp);
```

### Step 3: Create Structured Table Migration

Add structured tables in the same migration or create `migrations/013_committees_table.sql`:

```sql
-- Committees table
CREATE TABLE IF NOT EXISTS committees (
    slot UInt64,
    committee_index UInt64,
    validators Array(UInt64),
    validator_count UInt32,
    slot_timestamp DateTime64(0, 'UTC') MATERIALIZED addSeconds(
        (SELECT toDateTime(genesis_time_unix, 'UTC') FROM time_helpers LIMIT 1),
        slot * (SELECT seconds_per_slot FROM time_helpers LIMIT 1)
    ),
    insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
) ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (slot, committee_index)
PARTITION BY toStartOfMonth(slot_timestamp);
```

### Step 4: Create Loader

Create `src/loaders/committees.py`:

```python
import json
from datetime import datetime
from typing import Dict, Any, Optional
from .base import BaseLoader

class CommitteesLoader(BaseLoader):
    """Loader for committee data with payload hash for fork protection."""
    
    def __init__(self, beacon_api, storage):
        super().__init__("committees", beacon_api, storage)
    
    async def fetch_data(self, slot: int) -> Optional[Dict[str, Any]]:
        """Fetch committee data from beacon API using slot as state_id."""
        return await self.beacon_api.get_committees(str(slot))
    
    def prepare_row(self, slot: int, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare committee data for storage with payload hash."""
        payload_hash = self.calculate_payload_hash(data)
        
        return {
            "slot": slot,
            "payload": json.dumps(data),
            "payload_hash": payload_hash,
            "retrieved_at": datetime.now()
        }
```

### Step 5: Register Loader

Add the loader to `src/loaders/__init__.py`:

```python
from .committees import CommitteesLoader

LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader,
    "rewards": RewardsLoader,
    "committees": CommitteesLoader  # Add new loader
}
```

### Step 6: Create Parser

Create `src/parsers/committees.py`:

```python
import json
from typing import Dict, List, Any
from .base import BaseParser

class CommitteesParser(BaseParser):
    """Parser for committee data stored as String payloads."""
    
    def __init__(self):
        super().__init__("committees")
    
    def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse committee data from String payload into structured format."""
        slot = raw_data.get("slot", 0)
        payload_str = raw_data.get("payload", "{}")
        
        try:
            # Parse JSON string back to dict
            if isinstance(payload_str, str):
                payload = json.loads(payload_str)
            else:
                payload = payload_str
        except (json.JSONDecodeError, TypeError):
            return {}
        
        if "data" not in payload:
            return {}
        
        committees_data = payload["data"]
        committee_rows = []
        
        for committee in committees_data:
            committee_rows.append({
                "slot": slot,
                "committee_index": int(committee.get("index", 0)),
                "validators": [int(v) for v in committee.get("validators", [])],
                "validator_count": len(committee.get("validators", []))
            })
        
        return {"committees": committee_rows}
```

### Step 7: Update Loader Service

For slot-based loaders, update chunk generation in `src/services/loader.py`:

```python
# In _generate_smart_chunks method
if loader_name in ["blocks", "rewards", "committees"]:  # Add committees
    # Slot-based loaders: process all slots
    for i in range(start_slot, end_slot, config.CHUNK_SIZE):
        # ... chunk generation logic
```

### Step 8: Update Configuration

Add the new loader to your environment configuration:

```bash
# In .env
ENABLED_LOADERS=blocks,validators,specs,genesis,rewards,committees
```

### Step 9: Test Implementation

```bash
# Run migration
make migration

# Test loading a few slots
python -m src.main load backfill --start-slot 9000000 --end-slot 9000010

# Test transformation
python -m src.main transform batch

# Verify data
echo "SELECT COUNT(*) FROM raw_committees" | clickhouse-client
echo "SELECT COUNT(*) FROM committees" | clickhouse-client
echo "SELECT * FROM committees ORDER BY slot DESC LIMIT 5" | clickhouse-client
```

## Adding Fork-Aware Parsing

### Extending Existing Fork Parsers

If your new endpoint changes behavior across forks, extend the appropriate parser:

```python
# src/parsers/deneb.py
class DenebParser(CapellaParser):
    def get_supported_tables(self) -> List[str]:
        """Deneb adds blob sidecars and committees to Capella tables."""
        return super().get_supported_tables() + ["blob_sidecars", "blob_commitments", "committees"]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse Deneb-specific data including committee changes."""
        result = super().parse_fork_specific(slot, data)
        
        # Parse committee data if available in block body
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Example: Deneb might include committee info in block body
        if "committees" in body:
            committees_data = body["committees"]
            committee_rows = []
            
            for i, committee in enumerate(committees_data):
                committee_rows.append({
                    "slot": slot,
                    "committee_index": i,
                    "validators": committee.get("validators", []),
                    "validator_count": len(committee.get("validators", []))
                })
            
            if committee_rows:
                result["committees"] = committee_rows
        
        return result
```

### Creating New Fork Parsers

When a new consensus fork is released:

```python
# src/parsers/new_fork.py
from typing import Dict, List, Any
from .deneb import DenebParser

class NewForkParser(DenebParser):
    """Parser for new consensus fork with enhanced features."""
    
    def __init__(self):
        super().__init__()
        self.fork_name = "new_fork"
    
    def get_supported_tables(self) -> List[str]:
        """New fork adds additional tables."""
        return super().get_supported_tables() + ["new_fork_table"]
    
    def parse_fork_specific(self, slot: int, data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Parse new fork-specific data structures."""
        result = super().parse_fork_specific(slot, data)
        
        # Parse new fork-specific data
        message = data.get("message", {})
        body = message.get("body", {})
        
        # Example new field in fork
        new_fork_data = body.get("new_fork_field", {})
        if new_fork_data:
            new_rows = []
            # ... parse new data structure
            result["new_fork_table"] = new_rows
        
        return result
    
    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for new fork (mainnet)."""
        return "0x06000000"  # Next version after Electra
```

Register the new parser in `src/parsers/factory.py`:

```python
from .new_fork import NewForkParser

class ParserFactory:
    def _init_parsers(self):
        self.parser_cache = {
            "phase0": Phase0Parser(),
            "altair": AltairParser(),
            "bellatrix": BellatrixParser(),
            "capella": CapellaParser(),
            "deneb": DenebParser(),
            "electra": ElectraParser(),
            "new_fork": NewForkParser()  # Add new parser
        }
```

## Adding Storage Backends

### Creating a New Storage Backend

Create `src/services/custom_storage.py`:

```python
from typing import List, Dict, Any, Optional
from src.utils.logger import logger

class CustomStorage:
    """Custom storage backend implementation."""
    
    def __init__(self):
        self.connection = self._connect()
        logger.info("Custom storage initialized")
    
    def _connect(self):
        """Initialize connection to custom storage system."""
        # Implementation specific to your storage system
        pass
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]], **kwargs):
        """Insert batch of data into custom storage."""
        if not data:
            return
        
        try:
            # Convert data to storage-specific format
            formatted_data = self._format_data(table, data)
            
            # Insert using storage-specific method
            self._insert_data(table, formatted_data)
            
            logger.debug("Inserted data", table=table, rows=len(data))
            
        except Exception as e:
            logger.error("Custom storage insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise
    
    def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query against custom storage."""
        try:
            # Translate query to storage-specific format
            translated_query = self._translate_query(query, params)
            
            # Execute query
            results = self._execute_query(translated_query)
            
            # Convert results to standard format
            return self._format_results(results)
            
        except Exception as e:
            logger.error("Custom storage query failed", 
                        query=query[:100], 
                        error=str(e))
            return []
    
    async def init_cache(self):
        """Initialize any caching mechanisms."""
        # Implementation specific
        pass
    
    def _format_data(self, table: str, data: List[Dict[str, Any]]):
        """Format data for storage-specific requirements."""
        # Transform data as needed
        return data
    
    def _translate_query(self, query: str, params: Optional[Dict]):
        """Translate standard query to storage-specific format."""
        # Query translation logic
        return query
    
    def _insert_data(self, table: str, data):
        """Storage-specific insert implementation."""
        # Insert logic
        pass
    
    def _execute_query(self, query):
        """Storage-specific query execution."""
        # Query execution logic
        pass
    
    def _format_results(self, results):
        """Format results to standard dictionary format."""
        # Result formatting logic
        return results
```

### Register Storage Backend

Update `src/services/storage_factory.py`:

```python
from src.config import config
from src.utils.logger import logger

def create_storage():
    """Factory function to create the appropriate storage backend."""
    
    if config.STORAGE_BACKEND.lower() == "parquet":
        from src.services.parquet_storage import ParquetStorage
        logger.info("Using Parquet storage backend")
        return ParquetStorage()
    elif config.STORAGE_BACKEND.lower() == "custom":
        from src.services.custom_storage import CustomStorage
        logger.info("Using Custom storage backend")
        return CustomStorage()
    else:
        from src.services.clickhouse import ClickHouse
        logger.info("Using ClickHouse storage backend")
        return ClickHouse()
```

### Add Configuration

Update `src/config.py`:

```python
class Config:
    # Storage Backend - either 'clickhouse', 'parquet', or 'custom'
    STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "clickhouse")
    
    # Custom Storage Configuration
    CUSTOM_STORAGE_HOST = os.getenv("CUSTOM_STORAGE_HOST", "localhost")
    CUSTOM_STORAGE_PORT = int(os.getenv("CUSTOM_STORAGE_PORT", "8000"))
    CUSTOM_STORAGE_AUTH = os.getenv("CUSTOM_STORAGE_AUTH", "")
```

## Testing and Validation

### Unit Testing

Create `tests/test_committees.py`:

```python
import pytest
import json
from src.parsers.committees import CommitteesParser
from src.loaders.committees import CommitteesLoader

class TestCommitteesParser:
    def test_parse_valid_data(self):
        parser = CommitteesParser()
        
        raw_data = {
            "slot": 9000000,
            "payload": json.dumps({
                "data": [
                    {
                        "index": "0",
                        "validators": ["1", "2", "3"]
                    },
                    {
                        "index": "1", 
                        "validators": ["4", "5", "6", "7"]
                    }
                ]
            })
        }
        
        result = parser.parse(raw_data)
        
        assert "committees" in result
        assert len(result["committees"]) == 2
        assert result["committees"][0]["committee_index"] == 0
        assert result["committees"][0]["validator_count"] == 3
        assert result["committees"][1]["validator_count"] == 4
    
    def test_parse_empty_data(self):
        parser = CommitteesParser()
        
        raw_data = {
            "slot": 9000000,
            "payload": "{}"
        }
        
        result = parser.parse(raw_data)
        assert result == {}

class TestCommitteesLoader:
    @pytest.mark.asyncio
    async def test_prepare_row(self):
        # Mock storage and beacon_api
        loader = CommitteesLoader(None, None)
        
        test_data = {"data": [{"index": "0", "validators": ["1", "2"]}]}
        
        row = loader.prepare_row(9000000, test_data)
        
        assert row["slot"] == 9000000
        assert "payload" in row
        assert "payload_hash" in row
        assert "retrieved_at" in row
```

### Integration Testing

```bash
# Test with small dataset
export STORAGE_BACKEND=parquet
export START_SLOT=9000000
export END_SLOT=9000010
export ENABLED_LOADERS=blocks,committees

# Run pipeline
python -m src.main load backfill
python -m src.main transform batch

# Validate results
python -c "
import pandas as pd
committees = pd.read_parquet('./parquet_data/committees/')
print(f'Loaded {len(committees)} committee records')
print(committees.head())
"
```

### Performance Testing

```python
# tests/test_performance.py
import time
import pytest
from src.services.storage_factory import create_storage

class TestPerformance:
    def test_batch_insert_performance(self):
        storage = create_storage()
        
        # Generate test data
        test_data = []
        for i in range(10000):
            test_data.append({
                "slot": i,
                "committee_index": i % 64,
                "validators": list(range(i % 100)),
                "validator_count": i % 100
            })
        
        # Measure insert performance
        start_time = time.time()
        storage.insert_batch("committees", test_data)
        end_time = time.time()
        
        insert_time = end_time - start_time
        rows_per_second = len(test_data) / insert_time
        
        print(f"Inserted {len(test_data)} rows in {insert_time:.2f}s")
        print(f"Performance: {rows_per_second:.0f} rows/second")
        
        # Assert reasonable performance
        assert rows_per_second > 1000  # At least 1K rows/second
```

## Debugging and Troubleshooting

### Debug Configuration

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG

# Use small data ranges for testing
export START_SLOT=9000000
export END_SLOT=9000100

# Use Parquet for simpler debugging
export STORAGE_BACKEND=parquet
```

### Common Issues and Solutions

#### 1. API Endpoint Not Found
```python
# In BeaconAPI, add error handling
async def get_committees(self, state_id: str = "head") -> Optional[Dict[str, Any]]:
    try:
        return await self.get(f"/eth/v1/beacon/states/{state_id}/committees")
    except Exception as e:
        if "404" in str(e):
            logger.warning("Committees endpoint not supported by beacon node")
            return None
        raise
```

#### 2. Parser Errors
```python
# Add robust error handling in parsers
def parse(self, raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    try:
        # ... parsing logic
        return {"committees": committee_rows}
    except KeyError as e:
        logger.error("Missing required field in data", field=str(e), slot=raw_data.get("slot"))
        return {}
    except Exception as e:
        logger.error("Parser error", error=str(e), slot=raw_data.get("slot"))
        return {}
```

#### 3. Storage Issues
```python
# Add storage validation
def insert_batch(self, table: str, data: List[Dict[str, Any]]):
    if not data:
        logger.warning("Empty data batch", table=table)
        return
    
    # Validate data structure
    required_fields = self._get_required_fields(table)
    for row in data:
        missing_fields = [f for f in required_fields if f not in row]
        if missing_fields:
            logger.error("Missing required fields", 
                        table=table, 
                        missing=missing_fields)
            raise ValueError(f"Missing fields: {missing_fields}")
```

### Development Workflow

1. **Design Phase**
   - Define API endpoint and expected data structure
   - Plan database schema and table structure
   - Consider fork-awareness requirements

2. **Implementation Phase**
   - Add API method to BeaconAPI
   - Create database migration (ClickHouse)
   - Implement loader class
   - Create parser class
   - Register components

3. **Testing Phase**
   - Unit test parser and loader logic
   - Integration test with small dataset
   - Performance test with larger dataset
   - Validate data quality and completeness

4. **Deployment Phase**
   - Update configuration documentation
   - Create production migration
   - Monitor performance and errors
   - Gather user feedback

This development guide provides a comprehensive framework for extending the Beacon Chain Indexer with new functionality while maintaining code quality and system reliability.