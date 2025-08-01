import os
import json
import hashlib
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from src.config import config
from src.utils.logger import logger

class ParquetStorage:
    """Parquet-based storage backend that mirrors ClickHouse functionality."""
    
    def __init__(self):
        self.output_dir = Path(config.PARQUET_OUTPUT_DIR)
        self.compression = config.PARQUET_COMPRESSION
        self.rows_per_file = config.PARQUET_ROWS_PER_FILE
        
        # Create output directory structure
        self._ensure_directories()
        
        # Genesis cache for timing calculations
        self._genesis_time = None
        self._seconds_per_slot = None
        self._slots_per_epoch = None
        
        logger.info("Parquet storage initialized", output_dir=self.output_dir)
    
    def _ensure_directories(self):
        """Create necessary directory structure."""
        directories = [
            "genesis", "specs", "time_helpers",
            "raw_blocks", "raw_validators", "raw_rewards", "raw_specs", "raw_genesis",
            "blocks", "attestations", "validators", "rewards",
            "execution_payloads", "transactions", "withdrawals", "bls_changes",
            "blob_sidecars", "blob_commitments", "sync_aggregates", "sync_committees",
            "deposits", "voluntary_exits", "proposer_slashings", "attester_slashings",
            "execution_requests",
            "load_state_chunks", "transformer_progress"
        ]
        
        for directory in directories:
            (self.output_dir / directory).mkdir(parents=True, exist_ok=True)
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]], **kwargs):
        """Insert batch of data into Parquet files."""
        if not data:
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Add timestamp columns for partitioning
            df = self._add_timestamp_columns(df, table)
            
            # Get file path with partitioning
            file_path = self._get_file_path(table, df)
            
            # Append to existing file or create new one
            self._write_parquet(file_path, df)
            
            logger.debug("Inserted parquet data", 
                        table=table, 
                        rows=len(data), 
                        file=file_path.name)
            
        except Exception as e:
            logger.error("Parquet insert failed", 
                        table=table, 
                        rows=len(data), 
                        error=str(e))
            raise
    
    def _add_timestamp_columns(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        """Add timestamp columns for better partitioning and querying."""
        # Add insert timestamp
        df['inserted_at'] = datetime.now(timezone.utc)
        
        # Add slot timestamp if applicable
        if 'slot' in df.columns and self._genesis_time and self._seconds_per_slot:
            df['slot_timestamp'] = pd.to_datetime(
                self._genesis_time + df['slot'] * self._seconds_per_slot, 
                unit='s', 
                utc=True
            )
            df['date'] = df['slot_timestamp'].dt.date
        
        return df
    
    def _get_file_path(self, table: str, df: pd.DataFrame) -> Path:
        """Get file path with partitioning strategy."""
        table_dir = self.output_dir / table
        
        # Simple partitioning by date if slot data exists
        if 'date' in df.columns and not df['date'].isna().all():
            date = df['date'].iloc[0]
            file_dir = table_dir / f"date={date}"
        else:
            # No date partitioning for foundational tables
            file_dir = table_dir
        
        file_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table}_{timestamp}.parquet"
        
        return file_dir / filename
    
    def _write_parquet(self, file_path: Path, df: pd.DataFrame):
        """Write DataFrame to Parquet file."""
        df.to_parquet(
            file_path,
            compression=self.compression,
            index=False,
            engine='pyarrow'
        )
    
    def execute(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute a query against Parquet files (simplified implementation)."""
        try:
            # Handle common queries
            if "SELECT COUNT(*) as count FROM genesis" in query:
                return self._count_table("genesis")
            elif "SELECT COUNT(*) as count FROM specs" in query:
                return self._count_table("specs")
            elif "SELECT COUNT(*) as count FROM time_helpers" in query:
                return self._count_table("time_helpers")
            elif "FROM time_helpers" in query and "genesis_time_unix" in query:
                return self._get_time_helpers()
            elif "toUnixTimestamp(genesis_time)" in query:
                return self._get_genesis_unix()
            elif "FROM genesis" in query:
                return self._get_genesis()
            elif "max(slot)" in query:
                return self._get_max_slot(query)
            elif "load_state_chunks" in query:
                return self._query_chunks(query, params)
            elif "SELECT payload FROM raw_genesis" in query:
                return self._get_raw_table_payload("raw_genesis")
            elif "SELECT payload FROM raw_specs" in query:
                return self._get_raw_table_payload("raw_specs")
            elif "SELECT now() as now" in query:
                return [{"now": datetime.now(timezone.utc)}]
            else:
                logger.warning("Unsupported query for Parquet backend", query=query[:100])
                return []
                
        except Exception as e:
            logger.error("Parquet query failed", query=query[:100], error=str(e))
            return []
    
    def _count_table(self, table: str) -> List[Dict]:
        """Count rows in a table."""
        table_dir = self.output_dir / table
        if not table_dir.exists():
            return [{"count": 0}]
        
        count = 0
        for parquet_file in table_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                count += len(df)
            except Exception as e:
                logger.warning("Failed to read parquet file", file=parquet_file, error=str(e))
        
        return [{"count": count}]
    
    def _get_time_helpers(self) -> List[Dict]:
        """Get time helpers data."""
        table_dir = self.output_dir / "time_helpers"
        if not table_dir.exists():
            return []
        
        for parquet_file in table_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                if not df.empty:
                    # Return latest row
                    latest = df.iloc[-1]
                    return [{
                        "genesis_time_unix": int(latest.get("genesis_time_unix", 0)),
                        "seconds_per_slot": int(latest.get("seconds_per_slot", 12))
                    }]
            except Exception as e:
                logger.warning("Failed to read time_helpers", file=parquet_file, error=str(e))
        
        return []
    
    def _get_genesis(self) -> List[Dict]:
        """Get genesis data."""
        table_dir = self.output_dir / "genesis"
        if not table_dir.exists():
            return []
        
        for parquet_file in table_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                if not df.empty:
                    latest = df.iloc[-1]
                    genesis_time = latest.get("genesis_time")
                    if pd.notna(genesis_time):
                        # Convert to Unix timestamp
                        if hasattr(genesis_time, 'timestamp'):
                            unix_ts = int(genesis_time.timestamp())
                        else:
                            # Already a timestamp
                            unix_ts = int(genesis_time)
                        return [{"genesis_time_unix": unix_ts}]
            except Exception as e:
                logger.warning("Failed to read genesis", file=parquet_file, error=str(e))
        
        return []
        """Get genesis time as Unix timestamp."""
        table_dir = self.output_dir / "genesis"
        if not table_dir.exists():
            return []
        
        for parquet_file in table_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                if not df.empty:
                    latest = df.iloc[-1]
                    genesis_time = latest.get("genesis_time")
                    if pd.notna(genesis_time):
                        # Convert to Unix timestamp
                        if hasattr(genesis_time, 'timestamp'):
                            unix_ts = int(genesis_time.timestamp())
                        else:
                            # Already a timestamp
                            unix_ts = int(genesis_time)
                        return [{"genesis_time_unix": unix_ts}]
            except Exception as e:
                logger.warning("Failed to read genesis for unix timestamp", file=parquet_file, error=str(e))
        
        return []
    
    def _get_raw_table_payload(self, table_name: str) -> List[Dict]:
        """Get payload from raw table (latest entry first)."""
        table_dir = self.output_dir / table_name
        if not table_dir.exists():
            return []
        
        # Find all parquet files and sort by modification time (newest first)
        parquet_files = list(table_dir.rglob("*.parquet"))
        if not parquet_files:
            return []
        
        # Sort by modification time, newest first
        parquet_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        for parquet_file in parquet_files:
            try:
                df = pd.read_parquet(parquet_file)
                if not df.empty:
                    # Sort by retrieved_at if available, otherwise take last row
                    if 'retrieved_at' in df.columns:
                        df = df.sort_values('retrieved_at', ascending=False)
                    
                    latest = df.iloc[0]  # First row after sorting
                    payload = latest.get("payload")
                    
                    # Handle different payload formats
                    if isinstance(payload, str):
                        try:
                            # Try to parse JSON string
                            payload = json.loads(payload)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    
                    return [{"payload": payload}]
            except Exception as e:
                logger.warning("Failed to read raw table payload", 
                              table=table_name, 
                              file=parquet_file, 
                              error=str(e))
        
        return []
    
    def _get_max_slot(self, query: str) -> List[Dict]:
        """Get maximum slot from relevant tables."""
        max_slot = 0
        
        # Determine table from query
        if "raw_blocks" in query:
            table = "raw_blocks"
        elif "raw_validators" in query:
            table = "raw_validators"
        else:
            return [{"max_slot": None}]
        
        table_dir = self.output_dir / table
        if not table_dir.exists():
            return [{"max_slot": None}]
        
        for parquet_file in table_dir.rglob("*.parquet"):
            try:
                df = pd.read_parquet(parquet_file)
                if 'slot' in df.columns and not df.empty:
                    file_max = df['slot'].max()
                    if pd.notna(file_max):
                        max_slot = max(max_slot, int(file_max))
            except Exception as e:
                logger.warning("Failed to read max slot", file=parquet_file, error=str(e))
        
        return [{"max_slot": max_slot if max_slot > 0 else None}]
    
    def _query_chunks(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Handle chunk-related queries."""
        # For simplicity, return empty for chunk queries in Parquet mode
        # In a real implementation, you'd store chunk state in Parquet files too
        logger.debug("Chunk query not implemented for Parquet backend")
        return []
    
    def claim_chunk(self, worker_id: str, loader_name: str) -> Optional[tuple]:
        """Claim a chunk (not implemented for Parquet - use simpler approach)."""
        logger.debug("Chunk claiming not implemented for Parquet backend")
        return None
    
    def update_chunk_status(self, start_slot: int, end_slot: int, loader_name: str, status: str):
        """Update chunk status (not implemented for Parquet)."""
        logger.debug("Chunk status update not implemented for Parquet backend")
        pass
    
    def get_chunk_counts(self):
        """Get chunk counts (not implemented for Parquet)."""
        return {}
    
    def get_chunk_overview(self):
        """Get chunk overview (not implemented for Parquet)."""
        return []
    
    async def init_cache(self):
        """Initialize cache for timing parameters."""
        try:
            # Try to load genesis time from files
            genesis_data = self._get_genesis()
            if genesis_data:
                self._genesis_time = genesis_data[0].get("genesis_time_unix")
            
            # Try to load timing parameters
            time_helpers = self._get_time_helpers()
            if time_helpers:
                self._seconds_per_slot = time_helpers[0].get("seconds_per_slot", 12)
                self._slots_per_epoch = 32  # Default
                
            logger.debug("Parquet cache initialized", 
                        genesis_time=self._genesis_time,
                        seconds_per_slot=self._seconds_per_slot)
                        
        except Exception as e:
            logger.warning("Failed to initialize Parquet cache", error=str(e))