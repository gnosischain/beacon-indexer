from typing import List, Optional, Dict, Any

from src.models.reward import BlockReward, AttestationReward, SyncCommitteeReward
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class BlockRewardRepository:
    """Repository for handling block reward data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, reward: BlockReward) -> None:
        """Save a single block reward."""
        query = """
        INSERT INTO block_rewards (
            slot, block_root, proposer_index, total, attestations,
            sync_aggregate, proposer_slashings, attester_slashings
        ) VALUES (
            %(slot)s, %(block_root)s, %(proposer_index)s, %(total)s, %(attestations)s,
            %(sync_aggregate)s, %(proposer_slashings)s, %(attester_slashings)s
        )
        """
        
        self.clickhouse.execute(query, reward.to_db_dict())
    
    def get_by_block(self, slot: int, block_root: str) -> Optional[BlockReward]:
        """Get block reward for a specific block."""
        query = """
        SELECT *
        FROM block_rewards
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        if results:
            return BlockReward(**results[0])
        return None
    
    def get_by_month(self, month: str) -> List[BlockReward]:
        """Get block rewards for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM block_rewards
        WHERE month = %(month)s
        ORDER BY slot
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [BlockReward(**row) for row in results]
    
    def get_by_proposer(self, proposer_index: int, limit: int = 100) -> List[BlockReward]:
        """Get block rewards for a specific proposer."""
        query = """
        SELECT *
        FROM block_rewards
        WHERE proposer_index = %(proposer_index)s
        ORDER BY slot DESC
        LIMIT %(limit)s
        """
        
        results = self.clickhouse.execute(query, {
            "proposer_index": proposer_index,
            "limit": limit
        })
        
        return [BlockReward(**row) for row in results]


class AttestationRewardRepository:
    """Repository for handling attestation reward data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, reward: AttestationReward) -> None:
        """Save a single attestation reward."""
        query = """
        INSERT INTO attestation_rewards (
            epoch, validator_index, head, target, source, inclusion_delay, inactivity
        ) VALUES (
            %(epoch)s, %(validator_index)s, %(head)s, %(target)s, %(source)s, 
            %(inclusion_delay)s, %(inactivity)s
        )
        """
        
        self.clickhouse.execute(query, reward.to_db_dict())
    
    def save_many(self, rewards: List[AttestationReward]) -> None:
        """Save multiple attestation rewards."""
        if not rewards:
            return
            
        query = """
        INSERT INTO attestation_rewards (
            epoch, validator_index, head, target, source, inclusion_delay, inactivity
        ) VALUES
        """
        
        params = [reward.to_db_dict() for reward in rewards]
        self.clickhouse.execute_many(query, params)
    
    def get_by_month(self, month: str) -> List[AttestationReward]:
        """Get attestation rewards for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM attestation_rewards
        WHERE month = %(month)s
        ORDER BY epoch, validator_index
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [AttestationReward(**row) for row in results]
    
    def get_by_epoch_validator(self, epoch: int, validator_index: int) -> Optional[AttestationReward]:
        """Get attestation reward for a specific epoch and validator."""
        query = """
        SELECT *
        FROM attestation_rewards
        WHERE epoch = %(epoch)s AND validator_index = %(validator_index)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {
            "epoch": epoch,
            "validator_index": validator_index
        })
        
        if results:
            return AttestationReward(**results[0])
        return None
    
    def get_by_validator(self, validator_index: int, limit: int = 100) -> List[AttestationReward]:
        """Get attestation rewards for a specific validator."""
        query = """
        SELECT *
        FROM attestation_rewards
        WHERE validator_index = %(validator_index)s
        ORDER BY epoch DESC
        LIMIT %(limit)s
        """
        
        results = self.clickhouse.execute(query, {
            "validator_index": validator_index,
            "limit": limit
        })
        
        return [AttestationReward(**row) for row in results]
    
    def get_by_epoch(self, epoch: int) -> List[AttestationReward]:
        """Get attestation rewards for a specific epoch."""
        query = """
        SELECT *
        FROM attestation_rewards
        WHERE epoch = %(epoch)s
        ORDER BY validator_index
        """
        
        results = self.clickhouse.execute(query, {"epoch": epoch})
        
        return [AttestationReward(**row) for row in results]


class SyncCommitteeRewardRepository:
    """Repository for handling sync committee reward data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, reward: SyncCommitteeReward) -> None:
        """Save a single sync committee reward."""
        query = """
        INSERT INTO sync_committee_rewards (
            slot, block_root, validator_index, reward
        ) VALUES (
            %(slot)s, %(block_root)s, %(validator_index)s, %(reward)s
        )
        """
        
        self.clickhouse.execute(query, reward.to_db_dict())
    
    def save_many(self, rewards: List[SyncCommitteeReward]) -> None:
        """Save multiple sync committee rewards."""
        if not rewards:
            return
            
        query = """
        INSERT INTO sync_committee_rewards (
            slot, block_root, validator_index, reward
        ) VALUES
        """
        
        params = [reward.to_db_dict() for reward in rewards]
        self.clickhouse.execute_many(query, params)
    
    def get_by_month(self, month: str) -> List[SyncCommitteeReward]:
        """Get sync committee rewards for a specific month (format: YYYY-MM)."""
        query = """
        SELECT *
        FROM sync_committee_rewards
        WHERE month = %(month)s
        ORDER BY slot, validator_index
        """
        
        results = self.clickhouse.execute(query, {"month": month})
        
        return [SyncCommitteeReward(**row) for row in results]
    
    def get_by_block_validator(self, slot: int, block_root: str, validator_index: int) -> Optional[SyncCommitteeReward]:
        """Get sync committee reward for a specific block and validator."""
        query = """
        SELECT *
        FROM sync_committee_rewards
        WHERE slot = %(slot)s AND block_root = %(block_root)s AND validator_index = %(validator_index)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root,
            "validator_index": validator_index
        })
        
        if results:
            return SyncCommitteeReward(**results[0])
        return None
    
    def get_by_block(self, slot: int, block_root: str) -> List[SyncCommitteeReward]:
        """Get sync committee rewards for a specific block."""
        query = """
        SELECT *
        FROM sync_committee_rewards
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY validator_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [SyncCommitteeReward(**row) for row in results]
    
    def get_by_validator(self, validator_index: int, limit: int = 100) -> List[SyncCommitteeReward]:
        """Get sync committee rewards for a specific validator."""
        query = """
        SELECT *
        FROM sync_committee_rewards
        WHERE validator_index = %(validator_index)s
        ORDER BY slot DESC
        LIMIT %(limit)s
        """
        
        results = self.clickhouse.execute(query, {
            "validator_index": validator_index,
            "limit": limit
        })
        
        return [SyncCommitteeReward(**row) for row in results]