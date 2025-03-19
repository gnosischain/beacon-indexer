from typing import List, Optional, Dict, Any

from src.models.blob_sidecar import BlobSidecar
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class BlobSidecarRepository:
    """Repository for handling blob sidecar data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, blob_sidecar: BlobSidecar) -> None:
        """Save a single blob sidecar."""
        query = """
        INSERT INTO blob_sidecars (
            slot, block_root, blob_index, kzg_commitment, 
            kzg_proof, blob_data
        ) VALUES (
            %(slot)s, %(block_root)s, %(blob_index)s, %(kzg_commitment)s, 
            %(kzg_proof)s, %(blob_data)s
        )
        """
        
        self.clickhouse.execute(query, blob_sidecar.to_db_dict())
    
    def save_many(self, blob_sidecars: List[BlobSidecar]) -> None:
        """Save multiple blob sidecars."""
        if not blob_sidecars:
            return
            
        query = """
        INSERT INTO blob_sidecars (
            slot, block_root, blob_index, kzg_commitment, 
            kzg_proof, blob_data
        ) VALUES
        """
        
        params = [blob.to_db_dict() for blob in blob_sidecars]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[BlobSidecar]:
        """Get blob sidecars for a specific block."""
        query = """
        SELECT *
        FROM blob_sidecars
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY blob_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [BlobSidecar(**row) for row in results]
    
    def get_by_slot_range(self, start_slot: int, end_slot: int) -> List[BlobSidecar]:
        """Get blob sidecars within a slot range."""
        query = """
        SELECT *
        FROM blob_sidecars
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        ORDER BY slot, blob_index
        """
        
        results = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [BlobSidecar(**row) for row in results]
    
    def get_by_month(self, month: str) -> List[BlobSidecar]:
        """Get blob sidecars for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM blob_sidecars
        WHERE month = %(month)s
        ORDER BY slot, blob_index
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [BlobSidecar(**row) for row in results]
    
    def get_by_kzg_commitment(self, kzg_commitment: str) -> List[BlobSidecar]:
        """Get blob sidecars by KZG commitment."""
        query = """
        SELECT *
        FROM blob_sidecars
        WHERE kzg_commitment = %(kzg_commitment)s
        ORDER BY slot, blob_index
        """
        
        results = self.clickhouse.execute(query, {
            "kzg_commitment": kzg_commitment
        })
        
        return [BlobSidecar(**row) for row in results]