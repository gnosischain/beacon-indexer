from typing import List, Optional, Dict, Any

from src.models.attestation import Attestation
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class AttestationRepository:
    """Repository for handling attestation data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, attestation: Attestation) -> None:
        """Save a single attestation."""
        query = """
        INSERT INTO attestations (
            slot, block_root, attestation_slot, attestation_index, aggregation_bits,
            committee_bits, beacon_block_root, source_epoch, source_root, target_epoch,
            target_root, signature, validators
        ) VALUES (
            %(slot)s, %(block_root)s, %(attestation_slot)s, %(attestation_index)s, %(aggregation_bits)s,
            %(committee_bits)s, %(beacon_block_root)s, %(source_epoch)s, %(source_root)s, %(target_epoch)s,
            %(target_root)s, %(signature)s, %(validators)s
        )
        """
        
        self.clickhouse.execute(query, attestation.to_db_dict())
    
    def save_many(self, attestations: List[Attestation]) -> None:
        """Save multiple attestations."""
        if not attestations:
            return
            
        query = """
        INSERT INTO attestations (
            slot, block_root, attestation_slot, attestation_index, aggregation_bits,
            committee_bits, beacon_block_root, source_epoch, source_root, target_epoch,
            target_root, signature, validators
        ) VALUES
        """
        
        params = [attestation.to_db_dict() for attestation in attestations]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[Attestation]:
        """Get attestations for a specific block."""
        query = """
        SELECT *
        FROM attestations
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY attestation_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [Attestation(**row) for row in results]
    
    def get_by_slot_range(self, start_slot: int, end_slot: int) -> List[Attestation]:
        """Get attestations within a slot range."""
        query = """
        SELECT *
        FROM attestations
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        ORDER BY slot, attestation_index
        """
        
        results = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [Attestation(**row) for row in results]
    
    def get_by_month(self, month: str) -> List[Attestation]:
        """Get attestations for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM attestations
        WHERE month = %(month)s
        ORDER BY slot, attestation_index
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [Attestation(**row) for row in results]
    
    def get_by_validator(self, validator_index: int, limit: int = 100) -> List[Attestation]:
        """Get attestations by a specific validator."""
        query = """
        SELECT *
        FROM attestations
        WHERE hasElement(validators, %(validator_index)s) = 1
        ORDER BY slot DESC
        LIMIT %(limit)s
        """
        
        results = self.clickhouse.execute(query, {
            "validator_index": validator_index,
            "limit": limit
        })
        
        return [Attestation(**row) for row in results]