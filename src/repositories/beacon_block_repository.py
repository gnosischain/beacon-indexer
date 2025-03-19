from typing import List, Optional, Dict, Any

from src.models.beacon_block import BeaconBlock
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class BeaconBlockRepository:
    """Repository for handling beacon block data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, block: BeaconBlock) -> None:
        """Save a single beacon block."""
        query = """
        INSERT INTO blocks (
            slot, block_root, parent_root, state_root, proposer_index,
            eth1_block_hash, eth1_deposit_root, eth1_deposit_count,
            graffiti, signature, randao_reveal, timestamp, is_canonical, fork_version
        ) VALUES (
            %(slot)s, %(block_root)s, %(parent_root)s, %(state_root)s, %(proposer_index)s,
            %(eth1_block_hash)s, %(eth1_deposit_root)s, %(eth1_deposit_count)s,
            %(graffiti)s, %(signature)s, %(randao_reveal)s, %(timestamp)s, %(is_canonical)s, %(fork_version)s
        )
        """
        
        self.clickhouse.execute(query, block.to_db_dict())
    
    def save_many(self, blocks: List[BeaconBlock]) -> None:
        """Save multiple beacon blocks."""
        if not blocks:
            return
            
        query = """
        INSERT INTO blocks (
            slot, block_root, parent_root, state_root, proposer_index,
            eth1_block_hash, eth1_deposit_root, eth1_deposit_count,
            graffiti, signature, randao_reveal, timestamp, is_canonical, fork_version
        ) VALUES
        """
        
        params = [block.to_db_dict() for block in blocks]
        self.clickhouse.execute_many(query, params)
    
    def get_by_slot(self, slot: int) -> Optional[BeaconBlock]:
        """Get a canonical block by slot."""
        query = """
        SELECT *
        FROM blocks
        WHERE slot = %(slot)s AND is_canonical = 1
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"slot": slot})
        
        if results:
            return BeaconBlock(**results[0])
        return None
    
    def get_by_root(self, block_root: str) -> Optional[BeaconBlock]:
        """Get a block by root."""
        query = """
        SELECT *
        FROM blocks
        WHERE block_root = %(block_root)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"block_root": block_root})
        
        if results:
            return BeaconBlock(**results[0])
        return None
    
    def get_by_slot_range(self, start_slot: int, end_slot: int, canonical_only: bool = True) -> List[BeaconBlock]:
        """Get blocks within a slot range."""
        query = """
        SELECT *
        FROM blocks
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        """
        
        if canonical_only:
            query += " AND is_canonical = 1"
            
        query += " ORDER BY slot"
        
        results = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [BeaconBlock(**row) for row in results]
    
    def get_by_month(self, month: str, canonical_only: bool = True) -> List[BeaconBlock]:
        """Get blocks for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM blocks
        WHERE month = %(month)s
        """
        
        if canonical_only:
            query += " AND is_canonical = 1"
            
        query += " ORDER BY slot"
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [BeaconBlock(**row) for row in results]
    
    def get_by_proposer(self, proposer_index: int, limit: int = 100) -> List[BeaconBlock]:
        """Get blocks proposed by a specific validator."""
        query = """
        SELECT *
        FROM blocks
        WHERE proposer_index = %(proposer_index)s AND is_canonical = 1
        ORDER BY slot DESC
        LIMIT %(limit)s
        """
        
        results = self.clickhouse.execute(query, {
            "proposer_index": proposer_index,
            "limit": limit
        })
        
        return [BeaconBlock(**row) for row in results]
    
    def get_latest_slot(self) -> Optional[int]:
        """Get the latest slot number in the database."""
        query = """
        SELECT MAX(slot) as max_slot
        FROM blocks
        WHERE is_canonical = 1
        """
        
        results = self.clickhouse.execute(query)
        
        if results and 'max_slot' in results[0]:
            return int(results[0]['max_slot'])
        return None
    
    def mark_blocks_non_canonical(self, start_slot: int, end_slot: int) -> None:
        """Mark blocks as non-canonical in a slot range."""
        query = """
        ALTER TABLE blocks UPDATE is_canonical = 0
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        """
        
        self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })