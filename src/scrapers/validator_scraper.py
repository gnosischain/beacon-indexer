from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import asyncio
import math
import time

from src.scrapers.base_scraper import BaseScraper
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger
from src.utils.time_utils import is_last_slot_of_day, calculate_slot_timestamp, get_day_boundary_slots

class ValidatorScraper(BaseScraper):
    """
    Scraper for validators that processes them daily 
    (on the last slot of each day) for storage efficiency.
    """
    
    def __init__(self, beacon_api: BeaconAPIService, clickhouse: ClickHouseService):
        super().__init__("validator_scraper", beacon_api, clickhouse)
        self.batch_size = 200  # Process validators in batches of 200
        self.last_processed_day = None
        
    async def process(self, block_data: Dict) -> None:
        """
        Process a block and store validator information, but only for the last slot of each day.
        """
        # Extract block information
        data = block_data.get("data", {})
        message = data.get("message", {})
        slot = int(message.get("slot", 0))
        
        try:
            # Get time parameters
            genesis_time = self.clickhouse.get_genesis_time()
            time_params = self.clickhouse.get_time_parameters()
            
            if not genesis_time or not time_params:
                logger.warning(f"Cannot determine slot timing parameters. Skipping slot {slot}")
                return
                
            seconds_per_slot = time_params.get('seconds_per_slot')
            if not seconds_per_slot:
                logger.warning(f"Missing seconds_per_slot parameter. Skipping slot {slot}")
                return
            
            # Check if this is the last slot of the day
            should_process = is_last_slot_of_day(slot, genesis_time, seconds_per_slot)
            
            # For realtime mode, also check if we've already processed today
            slot_date = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot).date()
            if self.last_processed_day == slot_date:
                # Already processed a slot for this day
                should_process = False
            
            if not should_process:
                logger.debug(f"Skipping validator processing for slot {slot} (not last slot of day)")
                return
                
            logger.info(f"Processing validators for slot {slot} (last slot of day {slot_date})")
            
            # Get state validators
            validators_data = await self.beacon_api.get_state_validators(str(slot))
            
            if not validators_data:
                logger.debug(f"No validators found for state at slot {slot}")
                return
            
            # Calculate the slot timestamp for partitioning
            slot_timestamp = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
            month_partition = slot_timestamp.strftime('%Y-%m')
            
            # Process validators in batches
            total_validators = len(validators_data)
            logger.info(f"Processing {total_validators} validators for slot {slot} in partition {month_partition}")
            
            # Split into manageable batches
            batch_count = math.ceil(total_validators / self.batch_size)
            
            for batch_index in range(batch_count):
                start_idx = batch_index * self.batch_size
                end_idx = min(start_idx + self.batch_size, total_validators)
                
                batch = validators_data[start_idx:end_idx]
                
                # Prepare batch params
                params = []
                
                for validator_data in batch:
                    validator_index = int(validator_data.get("index", 0))
                    balance = int(validator_data.get("balance", 0))
                    status = validator_data.get("status", "")
                    
                    validator = validator_data.get("validator", {})
                    pubkey = validator.get("pubkey", "")
                    withdrawal_credentials = validator.get("withdrawal_credentials", "")
                    effective_balance = int(validator.get("effective_balance", 0))
                    slashed = 1 if validator.get("slashed", False) else 0
                    activation_eligibility_epoch = int(validator.get("activation_eligibility_epoch", 0))
                    activation_epoch = int(validator.get("activation_epoch", 0))
                    exit_epoch = int(validator.get("exit_epoch", 0))
                    withdrawable_epoch = int(validator.get("withdrawable_epoch", 0))
                    
                    params.append({
                        "slot": slot,
                        "validator_index": validator_index,
                        "pubkey": pubkey,
                        "withdrawal_credentials": withdrawal_credentials,
                        "effective_balance": effective_balance,
                        "slashed": slashed,
                        "activation_eligibility_epoch": activation_eligibility_epoch,
                        "activation_epoch": activation_epoch,
                        "exit_epoch": exit_epoch,
                        "withdrawable_epoch": withdrawable_epoch,
                        "status": status,
                        "balance": balance
                    })
                
                # Insert validators batch
                try:
                    query = """
                    INSERT INTO validators (
                        slot, validator_index, pubkey, withdrawal_credentials,
                        effective_balance, slashed, activation_eligibility_epoch,
                        activation_epoch, exit_epoch, withdrawable_epoch, status, balance
                    ) VALUES
                    """
                    
                    self.clickhouse.execute_many(query, params)
                    logger.info(f"Processed batch {batch_index+1}/{batch_count} ({len(params)} validators) for slot {slot}")
                except Exception as e:
                    logger.error(f"Error inserting validators batch {batch_index+1}: {e}")
                
                # Add a small delay between batches to avoid overwhelming the database
                await asyncio.sleep(0.5)
                
            # Record that we've processed this day
            self.last_processed_day = slot_date
        
        except Exception as e:
            logger.error(f"Error processing validators for slot {slot}: {e}")