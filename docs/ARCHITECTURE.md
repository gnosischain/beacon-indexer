# Architecture

The Beacon Chain Indexer follows an ELT (Extract, Load, Transform) architecture with dual storage backends and comprehensive fork awareness.

## High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Beacon Node   │    │   Beacon Chain   │    │   Storage Backend   │
│      API        │───▶│     Indexer      │───▶│   ClickHouse or     │
│                 │    │                  │    │      Parquet        │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   Analytics &    │
                       │   Monitoring     │
                       └──────────────────┘
```

## Core Components

### 1. Data Loading Layer

#### BeaconAPI Service
- **Purpose**: Interface to beacon node HTTP API
- **Features**: Retry logic, error handling, rate limiting
- **Endpoints**: Blocks, validators, rewards, specs, genesis

```python
class BeaconAPI:
    async def get_block(self, slot: int) -> Optional[Dict[str, Any]]
    async def get_validators(self, state_id: str) -> Optional[Dict[str, Any]]  
    async def get_rewards(self, slot: str) -> Optional[Dict[str, Any]]
```

#### Loader Registry
- **BaseLoader**: Abstract base class with common functionality  
- **Specific Loaders**: BlocksLoader, ValidatorsLoader, RewardsLoader, etc.
- **Payload Hashing**: Deduplication through deterministic hashing

```python
LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader, 
    "rewards": RewardsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader
}
```

### 2. Storage Abstraction Layer

#### Storage Factory Pattern
```python
def create_storage():
    if config.STORAGE_BACKEND == "parquet":
        return ParquetStorage()
    else:
        return ClickHouse()
```

#### Common Storage Interface
Both storage backends implement the same interface:
```python
class StorageInterface:
    def insert_batch(self, table: str, data: List[Dict])
    def execute(self, query: str, params: Dict) -> List[Dict]
    async def init_cache(self)
```

### 3. Data Processing Layer

#### Fork Detection System (ClickHouse Only)
- **Network Detection**: Automatic identification from `CONFIG_NAME`
- **Fork Version Mapping**: Network-specific fork versions
- **Epoch Calculation**: Fork activation epochs from historical data

```python
class ForkDetectionService:
    def get_fork_at_slot(self, slot: int) -> ForkInfo
    def get_network_name(self) -> str
    def is_auto_detected(self) -> bool
```

#### Parser Factory (ClickHouse Only)
- **Fork-Aware Parsing**: Automatic parser selection based on slot
- **Parser Hierarchy**: Inheritance chain for fork evolution
- **Table Support**: Each parser declares supported tables

```python
class ParserFactory:
    def get_parser_for_slot(self, slot: int) -> ForkBaseParser
    def get_supported_tables_for_slot(self, slot: int) -> List[str]
```

### 4. Data Transformation Layer

#### Transform Service
- **Backend Detection**: Switches processing mode based on storage backend
- **ClickHouse Mode**: Fork-aware, chunk-based processing
- **Parquet Mode**: Simple sequential processing

```python
class TransformerService:
    async def _run_clickhouse_transformer(self, batch_size, continuous)
    async def _run_parquet_transformer(self, batch_size, continuous)
```

## Fork-Aware Architecture (ClickHouse)

### Fork Evolution Chain

```
Phase0Parser
    ↓ (inherits)
AltairParser (adds sync committees)
    ↓ (inherits)  
BellatrixParser (adds execution payloads)
    ↓ (inherits)
CapellaParser (adds withdrawals)
    ↓ (inherits)
DenebParser (adds blob sidecars)
    ↓ (inherits)
ElectraParser (adds execution requests)
```

### Parser Responsibilities

Each parser in the chain:
1. **Inherits** all capabilities from previous forks
2. **Extends** `get_supported_tables()` with new tables
3. **Overrides** `parse_fork_specific()` for new data structures
4. **Maintains** backward compatibility

### Fork Detection Flow

```
Raw Block Data → Version Analysis → Network Detection → Fork Mapping → Parser Selection
     ↓               ↓                    ↓               ↓              ↓
  Slot 5000000   "0x04000000"         "mainnet"      Deneb Fork    DenebParser
```

## Data Flow Architecture

### ClickHouse Backend Data Flow

```
┌─────────────┐   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ Beacon API  │──▶│ Raw Tables      │──▶│ Fork Detection   │──▶│ Structured      │
│             │   │ - raw_blocks    │   │ & Parsing        │   │ Tables          │
│             │   │ - raw_validators│   │                  │   │ - blocks        │
│             │   │ - raw_rewards   │   │                  │   │ - attestations  │
└─────────────┘   └─────────────────┘   └──────────────────┘   │ - validators    │
                                                                │ - rewards       │
                                                                │ - (fork tables) │
                                                                └─────────────────┘
       ↓                      ↓                      ↓                     ↓
┌─────────────┐   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ LoaderService│   │ Chunk Manager   │   │ ParserFactory    │   │ SQL Queries     │
│ Multi-worker │   │ Work Distribution│   │ Version-aware    │   │ Real-time       │
│ Backfill    │   │ Progress Track  │   │ Parser Selection │   │ Analytics       │
└─────────────┘   └─────────────────┘   └──────────────────┘   └─────────────────┘
```

### Parquet Backend Data Flow

```
┌─────────────┐   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ Beacon API  │──▶│ Raw Parquet     │──▶│ Simple Parsing   │──▶│ Structured      │
│             │   │ Files           │   │                  │   │ Parquet Files   │
│             │   │ - raw_blocks/   │   │                  │   │ - blocks/       │
│             │   │ - raw_validators│   │                  │   │ - validators/   │
│             │   │ - raw_rewards/  │   │                  │   │ - rewards/      │
└─────────────┘   └─────────────────┘   └──────────────────┘   └─────────────────┘
       ↓                      ↓                      ↓                     ↓
┌─────────────┐   ┌─────────────────┐   ┌──────────────────┐   ┌─────────────────┐
│ LoaderService│   │ File Processing │   │ Basic Parsers    │   │ pandas/DuckDB   │
│ Sequential  │   │ processed/ dirs │   │ No Fork Aware    │   │ File-based      │
│ Processing  │   │ Progress Track  │   │ Parser Selection │   │ Analytics       │
└─────────────┘   └─────────────────┘   └──────────────────┘   └─────────────────┘
```

## Network Detection System

### Auto-Detection Process

1. **Foundation Data Loading**
   ```
   Genesis Loader → specs_loader → time_helpers table
        ↓               ↓               ↓
   Genesis time    Timing params   Network config
   ```

2. **Network Identification**
   ```
   CONFIG_NAME analysis → Network mapping → Fork schedule
   "mainnet" → mainnet → Mainnet fork epochs
   "gnosis"  → gnosis  → Gnosis fork epochs  
   ```

3. **Fork Schedule Detection**
   ```
   Historical blocks → Version analysis → Fork activation epochs
   Version patterns → Fork mapping → Network-specific schedule
   ```

### Network Configuration

```python
class NetworkConfig:
    name: str           # "mainnet", "gnosis", etc.
    genesis_time: int   # Unix timestamp
    seconds_per_slot: int  # 12 for Ethereum, 5 for Gnosis
    slots_per_epoch: int   # 32 for Ethereum, 16 for Gnosis
```

### Fork Schedule Example

| Network | Phase0 | Altair | Bellatrix | Capella | Deneb | Electra |
|---------|--------|--------|-----------|---------|-------|---------|
| Mainnet | 0 | 74240 | 144896 | 194048 | 269568 | 364032 |
| Gnosis | 0 | 512 | 385536 | 648704 | 889856 | 1337856 |

## Chunk Management System (ClickHouse)

### Chunk Lifecycle

```
┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│   Pending   │──▶│   Claimed   │──▶│  Completed  │   │   Failed    │
│             │   │             │   │             │   │             │
│ Available   │   │ Worker has  │   │ Successfully│   │ Processing  │
│ for work    │   │ claimed it  │   │ processed   │   │ failed      │
└─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘
       ↑                                                      │
       └──────────────────────────────────────────────────────┘
                        Reset on worker failure
```

### Worker Distribution

```python
# Smart chunk assignment ensures even distribution
worker_num = int(worker_id.split('_')[1])  # e.g., "worker_5" → 5
target_chunks = chunks.filter(
    chunk_row_num % total_workers == worker_num
)
```

### Chunk Table Schema

```sql
CREATE TABLE load_state_chunks (
    chunk_id String,              -- "blocks_9000000_9000999"  
    start_slot UInt64,           -- 9000000
    end_slot UInt64,             -- 9000999
    loader_name String,          -- "blocks"
    status Enum8,                -- pending/claimed/completed/failed
    worker_id String,            -- "worker_3"
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY chunk_id;
```