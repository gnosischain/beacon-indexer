from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from src.models.validator import Validator
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class ValidatorRepository:
    """Repository for handling validator data."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.clickhouse = clickhouse
    
    def save(self, validator: Validator) -> None:
        """Save a single validator."""
        query = """
        INSERT INTO validators (
            slot, validator_index, pubkey, withdrawal_credentials,
            effective_balance, slashed, activation_eligibility_epoch,
            activation_epoch, exit_epoch, withdrawable_epoch, status, balance
        ) VALUES (
            %(slot)s, %(validator_index)s, %(pubkey)s, %(withdrawal_credentials)s,
            %(effective_balance)s, %(slashed)s, %(activation_eligibility_epoch)s,
            %(activation_epoch)s, %(exit_epoch)s, %(withdrawable_epoch)s, %(status)s, %(balance)s
        )
        """
        
        self.clickhouse.execute(query, validator.to_db_dict())
    
    def save_many(self, validators: List[Validator]) -> None:
        """Save multiple validators."""
        if not validators:
            return
            
        query = """
        INSERT INTO validators (
            slot, validator_index, pubkey, withdrawal_credentials,
            effective_balance, slashed, activation_eligibility_epoch,
            activation_epoch, exit_epoch, withdrawable_epoch, status, balance
        ) VALUES
        """
        
        params = [validator.to_db_dict() for validator in validators]
        self.clickhouse.execute_many(query, params)
    
    def get_by_state_validator(self, state_slot: int, validator_index: int) -> Optional[Validator]:
        """Get validator at a specific state slot by validator index."""
        query = """
        SELECT *
        FROM validators
        WHERE state_slot = %(state_slot)s AND validator_index = %(validator_index)s
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {
            "state_slot": state_slot,
            "validator_index": validator_index
        })
        
        if results:
            return Validator(**results[0])
        return None
    
    def get_by_pubkey(self, pubkey: str, state_slot: Optional[int] = None) -> List[Validator]:
        """Get validator by public key."""
        query = """
        SELECT *
        FROM validators
        WHERE pubkey = %(pubkey)s
        """
        
        params = {"pubkey": pubkey}
        
        if state_slot is not None:
            query += " AND state_slot = %(state_slot)s"
            params["state_slot"] = state_slot
        
        query += " ORDER BY state_slot DESC"
        
        results = self.clickhouse.execute(query, params)
        
        return [Validator(**row) for row in results]
    
    def get_latest_by_validator_index(self, validator_index: int) -> Optional[Validator]:
        """Get the latest validator record by validator index."""
        query = """
        SELECT *
        FROM validators
        WHERE validator_index = %(validator_index)s
        ORDER BY state_slot DESC
        LIMIT 1
        """
        
        results = self.clickhouse.execute(query, {"validator_index": validator_index})
        
        if results:
            return Validator(**results[0])
        return None
    
    def get_by_status(self, status: str, state_slot: Optional[int] = None) -> List[Validator]:
        """Get validators by status."""
        query = """
        SELECT *
        FROM validators
        WHERE status = %(status)s
        """
        
        params = {"status": status}
        
        if state_slot is not None:
            query += " AND state_slot = %(state_slot)s"
            params["state_slot"] = state_slot
        else:
            # If no state_slot is provided, we need to get the latest snapshot for each validator
            query = """
            SELECT v.*
            FROM validators v
            JOIN (
                SELECT validator_index, MAX(state_slot) as max_slot
                FROM validators
                WHERE status = %(status)s
                GROUP BY validator_index
            ) latest ON v.validator_index = latest.validator_index AND v.state_slot = latest.max_slot
            """
        
        query += " ORDER BY validator_index"
        
        results = self.clickhouse.execute(query, params)
        
        return [Validator(**row) for row in results]
    
    def get_validators_count_by_status(self, state_slot: int) -> Dict[str, int]:
        """Get count of validators by status at a specific state slot."""
        query = """
        SELECT status, COUNT(*) as count
        FROM validators
        WHERE state_slot = %(state_slot)s
        GROUP BY status
        """
        
        results = self.clickhouse.execute(query, {"state_slot": state_slot})
        
        return {row["status"]: int(row["count"]) for row in results}
    
    def get_total_active_balance(self, state_slot: int) -> int:
        """Get total balance of active validators at a specific state slot."""
        query = """
        SELECT SUM(effective_balance) as total_balance
        FROM validators
        WHERE state_slot = %(state_slot)s AND status = 'active'
        """
        
        results = self.clickhouse.execute(query, {"state_slot": state_slot})
        
        if results and 'total_balance' in results[0]:
            return int(results[0]['total_balance'])
        return 0
    

    def get_latest_validators_for_day(self, day_date: datetime) -> List[Validator]:
        """
        Get the latest validator snapshot for a specific day.
        
        Args:
            day_date: The day to query (datetime)
            
        Returns:
            List of Validator records for the latest slot of the day
        """
        # Calculate day boundaries
        day_start = day_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1, microseconds=-1)
        
        query = """
        WITH day_slots AS (
            SELECT slot
            FROM validators
            WHERE slot_timestamp BETWEEN %(day_start)s AND %(day_end)s
            GROUP BY slot
        ),
        max_slot AS (
            SELECT MAX(slot) as max_slot
            FROM day_slots
        )
        SELECT *
        FROM validators
        WHERE slot = (SELECT max_slot FROM max_slot)
        ORDER BY validator_index
        """
        
        results = self.clickhouse.execute(query, {
            "day_start": day_start,
            "day_end": day_end
        })
        
        return [Validator(**row) for row in results]
    
    def get_validator_daily_history(self, validator_index: int, days: int = 30) -> List[Validator]:
        """
        Get daily snapshots for a specific validator over a period of days.
        
        Args:
            validator_index: The validator index
            days: Number of days of history to retrieve
            
        Returns:
            List of Validator records, one per day
        """
        query = """
        WITH daily_slots AS (
            SELECT date, MAX(slot) as max_slot
            FROM (
                SELECT 
                    slot,
                    toDate(slot_timestamp) as date
                FROM validators
                WHERE validator_index = %(validator_index)s
                  AND slot_timestamp >= subtractDays(now(), %(days)s)
            ) 
            GROUP BY date
        )
        SELECT v.*
        FROM validators v
        JOIN daily_slots ds ON v.slot = ds.max_slot
        WHERE v.validator_index = %(validator_index)s
        ORDER BY slot_timestamp DESC
        """
        
        results = self.clickhouse.execute(query, {
            "validator_index": validator_index,
            "days": days
        })
        
        return [Validator(**row) for row in results]
    
    def get_validators_performance_summary(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """
        Get a performance summary of validators between two dates.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            Summary statistics
        """
        # Get slots for the first and last day
        start_day_query = """
        SELECT MAX(slot) as slot
        FROM validators
        WHERE toDate(slot_timestamp) = toDate(%(start_date)s)
        """
        
        end_day_query = """
        SELECT MAX(slot) as slot
        FROM validators
        WHERE toDate(slot_timestamp) = toDate(%(end_date)s)
        """
        
        start_result = self.clickhouse.execute(start_day_query, {"start_date": start_date})
        end_result = self.clickhouse.execute(end_day_query, {"end_date": end_date})
        
        if not start_result or not end_result:
            return {}
            
        start_slot = start_result[0]["slot"]
        end_slot = end_result[0]["slot"]
        
        # Get stats
        stats_query = """
        WITH start_vals AS (
            SELECT 
                validator_index,
                balance as start_balance,
                effective_balance as start_effective_balance,
                status as start_status,
                slashed as start_slashed
            FROM validators
            WHERE slot = %(start_slot)s
        ),
        end_vals AS (
            SELECT 
                validator_index,
                balance as end_balance,
                effective_balance as end_effective_balance,
                status as end_status,
                slashed as end_slashed
            FROM validators
            WHERE slot = %(end_slot)s
        )
        SELECT
            COUNT(end_vals.validator_index) as total_validators,
            SUM(end_vals.balance) as total_balance,
            SUM(end_vals.effective_balance) as total_effective_balance,
            AVG(end_vals.balance - start_vals.balance) as avg_balance_change,
            SUM(if(end_vals.slashed = 1, 1, 0)) as total_slashed,
            SUM(if(end_vals.slashed = 1 AND start_vals.slashed = 0, 1, 0)) as new_slashed,
            SUM(if(end_vals.status = 'active', 1, 0)) as active_validators,
            SUM(if(start_vals.status != 'active' AND end_vals.status = 'active', 1, 0)) as new_active
        FROM end_vals
        LEFT JOIN start_vals ON end_vals.validator_index = start_vals.validator_index
        """
        
        results = self.clickhouse.execute(stats_query, {
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        if results:
            return results[0]
        return {}