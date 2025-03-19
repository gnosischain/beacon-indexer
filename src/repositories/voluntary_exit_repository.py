from typing import List, Optional, Dict, Any

from src.models.voluntary_exit import VoluntaryExit, ProposerSlashing, AttesterSlashing
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class VoluntaryExitRepository:
    """Repository for handling voluntary exit data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, exit: VoluntaryExit) -> None:
        """Save a single voluntary exit."""
        query = """
        INSERT INTO voluntary_exits (
            slot, block_root, validator_index, epoch, signature
        ) VALUES (
            %(slot)s, %(block_root)s, %(validator_index)s, %(epoch)s, %(signature)s
        )
        """
        
        self.clickhouse.execute(query, exit.to_db_dict())
    
    def save_many(self, exits: List[VoluntaryExit]) -> None:
        """Save multiple voluntary exits."""
        if not exits:
            return
            
        query = """
        INSERT INTO voluntary_exits (
            slot, block_root, validator_index, epoch, signature
        ) VALUES
        """
        
        params = [exit.to_db_dict() for exit in exits]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[VoluntaryExit]:
        """Get voluntary exits for a specific block."""
        query = """
        SELECT *
        FROM voluntary_exits
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY validator_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [VoluntaryExit(**row) for row in results]
    
    def get_by_validator(self, validator_index: int) -> List[VoluntaryExit]:
        """Get voluntary exits for a specific validator."""
        query = """
        SELECT *
        FROM voluntary_exits
        WHERE validator_index = %(validator_index)s
        ORDER BY slot
        """
        
        results = self.clickhouse.execute(query, {"validator_index": validator_index})
        
        return [VoluntaryExit(**row) for row in results]
    
    def get_by_epoch(self, epoch: int) -> List[VoluntaryExit]:
        """Get voluntary exits for a specific epoch."""
        query = """
        SELECT *
        FROM voluntary_exits
        WHERE epoch = %(epoch)s
        ORDER BY slot, validator_index
        """
        
        results = self.clickhouse.execute(query, {"epoch": epoch})
        
        return [VoluntaryExit(**row) for row in results]


class ProposerSlashingRepository:
    """Repository for handling proposer slashing data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, slashing: ProposerSlashing) -> None:
        """Save a single proposer slashing."""
        query = """
        INSERT INTO proposer_slashings (
            slot, block_root, proposer_index, header_1_slot, header_1_proposer,
            header_1_root, header_1_signature, header_2_slot, header_2_proposer,
            header_2_root, header_2_signature
        ) VALUES (
            %(slot)s, %(block_root)s, %(proposer_index)s, %(header_1_slot)s, %(header_1_proposer)s,
            %(header_1_root)s, %(header_1_signature)s, %(header_2_slot)s, %(header_2_proposer)s,
            %(header_2_root)s, %(header_2_signature)s
        )
        """
        
        self.clickhouse.execute(query, slashing.to_db_dict())
    
    def save_many(self, slashings: List[ProposerSlashing]) -> None:
        """Save multiple proposer slashings."""
        if not slashings:
            return
            
        query = """
        INSERT INTO proposer_slashings (
            slot, block_root, proposer_index, header_1_slot, header_1_proposer,
            header_1_root, header_1_signature, header_2_slot, header_2_proposer,
            header_2_root, header_2_signature
        ) VALUES
        """
        
        params = [slashing.to_db_dict() for slashing in slashings]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[ProposerSlashing]:
        """Get proposer slashings for a specific block."""
        query = """
        SELECT *
        FROM proposer_slashings
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY proposer_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [ProposerSlashing(**row) for row in results]
    
    def get_by_proposer(self, proposer_index: int) -> List[ProposerSlashing]:
        """Get proposer slashings for a specific proposer."""
        query = """
        SELECT *
        FROM proposer_slashings
        WHERE proposer_index = %(proposer_index)s
        ORDER BY slot
        """
        
        results = self.clickhouse.execute(query, {"proposer_index": proposer_index})
        
        return [ProposerSlashing(**row) for row in results]


class AttesterSlashingRepository:
    """Repository for handling attester slashing data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, slashing: AttesterSlashing) -> None:
        """Save a single attester slashing."""
        query = """
        INSERT INTO attester_slashings (
            slot, block_root, slashing_index, attestation_1_indices,
            attestation_1_slot, attestation_1_index, attestation_1_root, attestation_1_sig,
            attestation_2_indices, attestation_2_slot, attestation_2_index,
            attestation_2_root, attestation_2_sig
        ) VALUES (
            %(slot)s, %(block_root)s, %(slashing_index)s, %(attestation_1_indices)s,
            %(attestation_1_slot)s, %(attestation_1_index)s, %(attestation_1_root)s, %(attestation_1_sig)s,
            %(attestation_2_indices)s, %(attestation_2_slot)s, %(attestation_2_index)s,
            %(attestation_2_root)s, %(attestation_2_sig)s
        )
        """
        
        self.clickhouse.execute(query, slashing.to_db_dict())
    
    def save_many(self, slashings: List[AttesterSlashing]) -> None:
        """Save multiple attester slashings."""
        if not slashings:
            return
            
        query = """
        INSERT INTO attester_slashings (
            slot, block_root, slashing_index, attestation_1_indices,
            attestation_1_slot, attestation_1_index, attestation_1_root, attestation_1_sig,
            attestation_2_indices, attestation_2_slot, attestation_2_index,
            attestation_2_root, attestation_2_sig
        ) VALUES
        """
        
        params = [slashing.to_db_dict() for slashing in slashings]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[AttesterSlashing]:
        """Get attester slashings for a specific block."""
        query = """
        SELECT *
        FROM attester_slashings
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY slashing_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [AttesterSlashing(**row) for row in results]
    
    def get_by_validator(self, validator_index: int) -> List[AttesterSlashing]:
        """Get attester slashings involving a specific validator."""
        query = """
        SELECT *
        FROM attester_slashings
        WHERE hasElement(attestation_1_indices, %(validator_index)s) = 1 
           OR hasElement(attestation_2_indices, %(validator_index)s) = 1
        ORDER BY slot, slashing_index
        """
        
        results = self.clickhouse.execute(query, {"validator_index": validator_index})
        
        return [AttesterSlashing(**row) for row in results]