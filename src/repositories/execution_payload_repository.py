from typing import List, Optional, Dict, Any

from src.models.execution_payload import ExecutionPayload, Transaction, Withdrawal
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class ExecutionPayloadRepository:
    """Repository for handling execution payload data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, payload: ExecutionPayload) -> None:
        """Save a single execution payload."""
        query = """
        INSERT INTO execution_payloads (
            slot, block_root, parent_hash, fee_recipient, state_root,
            receipts_root, logs_bloom, prev_randao, block_number, gas_limit,
            gas_used, timestamp, extra_data, base_fee_per_gas, block_hash,
            excess_blob_gas, transactions_count, withdrawals_count
        ) VALUES (
            %(slot)s, %(block_root)s, %(parent_hash)s, %(fee_recipient)s, %(state_root)s,
            %(receipts_root)s, %(logs_bloom)s, %(prev_randao)s, %(block_number)s, %(gas_limit)s,
            %(gas_used)s, %(timestamp)s, %(extra_data)s, %(base_fee_per_gas)s, %(block_hash)s,
            %(excess_blob_gas)s, %(transactions_count)s, %(withdrawals_count)s
        )
        """
        
        self.clickhouse.execute(query, payload.to_db_dict())
    
    def get_by_block(self, slot: int, block_root: str) -> Optional[ExecutionPayload]:
        """Get execution payload for a specific block."""
        query = """
        SELECT *
        FROM execution_payloads
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        if results:
            return ExecutionPayload(**results[0])
        return None
    
    def get_by_execution_block_hash(self, block_hash: str) -> Optional[ExecutionPayload]:
        """Get execution payload by execution layer block hash."""
        query = """
        SELECT *
        FROM execution_payloads
        WHERE block_hash = %(block_hash)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"block_hash": block_hash})
        
        if results:
            return ExecutionPayload(**results[0])
        return None
    
    def get_by_execution_block_number(self, block_number: int) -> Optional[ExecutionPayload]:
        """Get execution payload by execution layer block number."""
        query = """
        SELECT *
        FROM execution_payloads
        WHERE block_number = %(block_number)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"block_number": block_number})
        
        if results:
            return ExecutionPayload(**results[0])
        return None
    
    def get_by_slot_range(self, start_slot: int, end_slot: int) -> List[ExecutionPayload]:
        """Get execution payloads within a slot range."""
        query = """
        SELECT *
        FROM execution_payloads
        WHERE slot BETWEEN %(start_slot)s AND %(end_slot)s
        ORDER BY slot
        """
        
        results = self.clickhouse.execute(query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return [ExecutionPayload(**row) for row in results]


class TransactionRepository:
    """Repository for handling transaction data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, transaction: Transaction) -> None:
        """Save a single transaction."""
        query = """
        INSERT INTO transactions (
            slot, block_root, tx_index, transaction_data
        ) VALUES (
            %(slot)s, %(block_root)s, %(tx_index)s, %(transaction_data)s
        )
        """
        
        self.clickhouse.execute(query, transaction.to_db_dict())
    
    def save_many(self, transactions: List[Transaction]) -> None:
        """Save multiple transactions."""
        if not transactions:
            return
            
        query = """
        INSERT INTO transactions (
            slot, block_root, tx_index, transaction_data
        ) VALUES
        """
        
        params = [tx.to_db_dict() for tx in transactions]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[Transaction]:
        """Get transactions for a specific block."""
        query = """
        SELECT *
        FROM transactions
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY tx_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [Transaction(**row) for row in results]


class WithdrawalRepository:
    """Repository for handling withdrawal data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, withdrawal: Withdrawal) -> None:
        """Save a single withdrawal."""
        query = """
        INSERT INTO withdrawals (
            slot, block_root, withdrawal_index, validator_index, address, amount
        ) VALUES (
            %(slot)s, %(block_root)s, %(withdrawal_index)s, %(validator_index)s, %(address)s, %(amount)s
        )
        """
        
        self.clickhouse.execute(query, withdrawal.to_db_dict())
    
    def save_many(self, withdrawals: List[Withdrawal]) -> None:
        """Save multiple withdrawals."""
        if not withdrawals:
            return
            
        query = """
        INSERT INTO withdrawals (
            slot, block_root, withdrawal_index, validator_index, address, amount
        ) VALUES
        """
        
        params = [w.to_db_dict() for w in withdrawals]
        self.clickhouse.execute_many(query, params)
    
    def get_by_block(self, slot: int, block_root: str) -> List[Withdrawal]:
        """Get withdrawals for a specific block."""
        query = """
        SELECT *
        FROM withdrawals
        WHERE slot = %(slot)s AND block_root = %(block_root)s
        ORDER BY withdrawal_index
        """
        
        results = self.clickhouse.execute(query, {
            "slot": slot,
            "block_root": block_root
        })
        
        return [Withdrawal(**row) for row in results]
    
    def get_by_validator(self, validator_index: int) -> List[Withdrawal]:
        """Get withdrawals for a specific validator."""
        query = """
        SELECT *
        FROM withdrawals
        WHERE validator_index = %(validator_index)s
        ORDER BY slot, withdrawal_index
        """
        
        results = self.clickhouse.execute(query, {"validator_index": validator_index})
        
        return [Withdrawal(**row) for row in results]
    
    def get_by_address(self, address: str) -> List[Withdrawal]:
        """Get withdrawals for a specific withdrawal address."""
        query = """
        SELECT *
        FROM withdrawals
        WHERE address = %(address)s
        ORDER BY slot, withdrawal_index
        """
        
        results = self.clickhouse.execute(query, {"address": address})
        
        return [Withdrawal(**row) for row in results]