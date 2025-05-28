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
from src.services.bulk_insertion_service import BulkInsertionService

class ValidatorScraper(BaseScraper):
    """
    Scraper for validators that processes them daily 
    (on the last slot of each day) for storage efficiency.
    Now with smart slot detection to avoid processing unnecessary slots.
    """
    
    def __init__(self, beacon_api: BeaconAPIService, clickhouse: ClickHouseService):
        super().__init__("validator_scraper", beacon_api, clickhouse)
        self.batch_size = 20000  # Process validators in batches
        self.last_processed_day = None
        self._bulk_inserter = None
        self._processed_slots = set()  # Track which slots we've already processed
        
    def get_bulk_inserter(self) -> Optional[BulkInsertionService]:
        """Get the bulk inserter from the parent worker if available."""
        if not self._bulk_inserter:
            # Try to find it in the global context
            import inspect
            frame = inspect.currentframe()
            try:
                while frame:
                    if 'self' in frame.f_locals and hasattr(frame.f_locals['self'], 'bulk_inserter'):
                        self._bulk_inserter = frame.f_locals['self'].bulk_inserter
                        break
                    frame = frame.f_back
            finally:
                del frame
        return self._bulk_inserter
    
    def should_process_slot(self, slot: int) -> bool:
        """
        Determine if this slot should be processed by the validator scraper.
        Only process slots that are the last slot of their day.
        """
        # If we've already processed this slot, skip it
        if slot in self._processed_slots:
            return False
            
        try:
            # Get time parameters
            genesis_time = self.clickhouse.get_genesis_time()
            time_params = self.clickhouse.get_time_parameters()
            
            if not genesis_time or not time_params:
                logger.warning(f"Cannot determine slot timing parameters for slot {slot}")
                return False
                
            seconds_per_slot = time_params.get('seconds_per_slot')
            if not seconds_per_slot:
                logger.warning(f"Missing seconds_per_slot parameter for slot {slot}")
                return False
            
            # Check if this is the last slot of the day
            is_last_slot = is_last_slot_of_day(slot, genesis_time, seconds_per_slot)
            
            if is_last_slot:
                # Check if we've already processed today
                slot_date = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot).date()
                if self.last_processed_day == slot_date:
                    # Already processed a slot for this day
                    return False
                    
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if slot {slot} should be processed: {e}")
            return False
    
    async def _insert_with_bulk(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Insert data either using bulk inserter or direct method."""
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion(table_name, data)
            return True
        else:
            # Fall back to direct insertion
            query = f"INSERT INTO {table_name} VALUES"
            self.clickhouse.execute(query, data)
            return True
        
    async def process(self, block_data: Dict) -> None:
        """
        Process a block and store validator information, but only for the last slot of each day.
        """
        # Extract block information
        data = block_data.get("data", {})
        message = data.get("message", {})
        slot = int(message.get("slot", 0))
        
        # Quick check: should we process this slot?
        if not self.should_process_slot(slot):
            logger.debug(f"Skipping validator processing for slot {slot} (not target slot)")
            return

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
            
            # Calculate the slot timestamp for partitioning
            slot_timestamp = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
            slot_date = slot_timestamp.date()
            
            logger.info(f"Processing validators for slot {slot} (last slot of day {slot_date})")
            
            # Get state validators with timeout
            logger.debug(f"Fetching validators for slot {slot}...")
            try:
                # Add a timeout to the API call
                validators_data = await asyncio.wait_for(
                    self.beacon_api.get_state_validators(str(slot)), 
                    timeout=300.0  # 5 minute timeout
                )
            except asyncio.TimeoutError:
                logger.error(f"Timeout fetching validators for slot {slot} after 5 minutes")
                return
            except Exception as e:
                logger.error(f"Error fetching validators for slot {slot}: {e}")
                return
            
            if not validators_data:
                logger.warning(f"No validators found for state at slot {slot}")
                return
            
            # Calculate the slot timestamp for partitioning
            month_partition = slot_timestamp.strftime('%Y-%m')
            
            # Process validators in batches
            total_validators = len(validators_data)
            logger.info(f"Processing {total_validators} validators for slot {slot} in partition {month_partition}")
            
            # Split into manageable batches
            batch_count = math.ceil(total_validators / self.batch_size)
            logger.info(f"Will process in {batch_count} batches of size {self.batch_size}")
            
            start_time = time.time()
            
            for batch_index in range(batch_count):
                batch_start_time = time.time()
                start_idx = batch_index * self.batch_size
                end_idx = min(start_idx + self.batch_size, total_validators)
                
                batch = validators_data[start_idx:end_idx]
                
                logger.info(f"Processing batch {batch_index+1}/{batch_count} ({len(batch)} validators) for slot {slot}")
                
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
                    insert_start_time = time.time()
                    bulk_inserter = self.get_bulk_inserter()
                    if bulk_inserter:
                        for param in params:
                            bulk_inserter.queue_for_insertion("validators", param)
                        insert_time = time.time() - insert_start_time
                        logger.info(f"Queued batch {batch_index+1}/{batch_count} ({len(params)} validators) for slot {slot} in {insert_time:.2f}s")
                    else:
                        query = """
                        INSERT INTO validators (
                            slot, validator_index, pubkey, withdrawal_credentials,
                            effective_balance, slashed, activation_eligibility_epoch,
                            activation_epoch, exit_epoch, withdrawable_epoch, status, balance
                        ) VALUES
                        """
                        
                        self.clickhouse.execute_many(query, params)
                        insert_time = time.time() - insert_start_time
                        logger.info(f"Processed batch {batch_index+1}/{batch_count} ({len(params)} validators) for slot {slot} in {insert_time:.2f}s")
                except Exception as e:
                    logger.error(f"Error inserting validators batch {batch_index+1}: {e}")
                    raise  # Re-raise to stop processing if DB insert fails
                
                batch_time = time.time() - batch_start_time
                logger.debug(f"Batch {batch_index+1} total time: {batch_time:.2f}s")
                
                # Add a small delay between batches to avoid overwhelming the database
                await asyncio.sleep(0.1)  # Reduced from 0.5 to 0.1
                
            total_time = time.time() - start_time
            logger.info(f"Completed processing {total_validators} validators for slot {slot} in {total_time:.2f}s")
                
            # Record that we've processed this day and slot
            self.last_processed_day = slot_date
            self._processed_slots.add(slot)
            
            logger.info(f"Successfully completed validator processing for slot {slot}")
        
        except Exception as e:
            logger.error(f"Error processing validators for slot {slot}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise  # Re-raise so the worker knows there was an error

    @classmethod
    def get_target_slots_in_range(cls, start_slot: int, end_slot: int, clickhouse_service) -> List[int]:
        """
        Get all slots in the range that should be processed by the validator scraper.
        These are the last slots of each day.
        """
        try:
            # Get time parameters
            genesis_time = clickhouse_service.get_genesis_time()
            time_params = clickhouse_service.get_time_parameters()
            
            if not genesis_time or not time_params:
                logger.warning(f"Cannot determine slot timing parameters for range {start_slot}-{end_slot}")
                logger.debug(f"genesis_time: {genesis_time}, time_params: {time_params}")
                return []
                
            seconds_per_slot = time_params.get('seconds_per_slot')
            if not seconds_per_slot:
                logger.warning(f"Missing seconds_per_slot parameter for range {start_slot}-{end_slot}")
                logger.debug(f"time_params: {time_params}")
                return []

            # Use the existing utility function
            from src.utils.time_utils import get_relevant_validator_slots_in_range
            target_slots = get_relevant_validator_slots_in_range(
                start_slot, end_slot, genesis_time, seconds_per_slot
            )
            
            if target_slots:
                logger.debug(f"Found {len(target_slots)} validator target slots in range {start_slot}-{end_slot}: {target_slots[:5]}{'...' if len(target_slots) > 5 else ''}")
            else:
                logger.debug(f"No validator target slots found in range {start_slot}-{end_slot}")
            
            return target_slots
            
        except Exception as e:
            logger.error(f"Error getting target validator slots in range {start_slot}-{end_slot}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []