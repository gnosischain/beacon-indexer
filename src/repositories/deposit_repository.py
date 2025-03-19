from typing import List, Optional, Dict, Any

from src.models.deposit import Deposit
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class DepositRepository:
    """Repository for handling deposit data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, deposit: Deposit) -> None:
        """Save a single deposit."""
        query = """
        INSERT INTO deposits (
            slot, block_root, deposit_index, pubkey,
            withdrawal_credentials, amount, signature
        ) VALUES (
            %(slot)s, %(block_root)s, %(deposit_index)s, %(pubkey)s,
            %(withdrawal_credentials)s, %(amount)s, %(signature)s
        )
        """
        
        self.clickhouse.execute(query, deposit.to_db_dict())
    
    def save_many(self, deposits: List[Deposit]) -> None:
        """Save multiple deposits."""
        if not deposits:
            return
            
        query = """
        INSERT INTO deposits (
            slot, block_root, deposit_index, pubkey,
            withdrawal_credentials, amount, signature
        ) VALUES
        """
        
        params = [deposit.to_db_dict() for deposit in deposits]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[Deposit]:
        """Get deposits for a specific block."""
        query = """
        SELECT *
        FROM deposits
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY deposit_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [Deposit(**row) for row in results]
    
    def get_by_slot_range(self, start_slot: int, end_slot: int) -> List[Deposit]:
        """Get deposits within a slot range."""
        query = """
        SELECT *
        FROM deposits
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        ORDER BY slot, deposit_index
        """
        
        results = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [Deposit(**row) for row in results]
    
    def get_by_pubkey(self, pubkey: str) -> List[Deposit]:
        """Get deposits for a specific validator public key."""
        query = """
        SELECT *
        FROM deposits
        WHERE pubkey = %(pubkey)s
        ORDER BY slot, deposit_index
        """
        
        results = self.clickhouse.execute(query, {"pubkey": pubkey})
        
        return [Deposit(**row) for row in results]
    
    def get_total_deposits_for_pubkey(self, pubkey: str) -> int:
        """Get total deposits amount for a specific validator public key."""
        query = """
        SELECT SUM(amount) as total_amount
        FROM deposits
        WHERE pubkey = %(pubkey)s
        """
        
        results = self.clickhouse.execute(query, {"pubkey": pubkey})
        
        if results and 'total_amount' in results[0]:
            return int(results[0]['total_amount'])
        return 0