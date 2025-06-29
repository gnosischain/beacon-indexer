from typing import Dict, Tuple, Optional, List, Set
from datetime import datetime, timezone, timedelta
from src.utils.logger import logger

def calculate_slot_timestamp(genesis_time: int, slot: int, seconds_per_slot: int) -> datetime:
    """Calculate UTC timestamp for a given slot."""
    timestamp = genesis_time + (slot * seconds_per_slot)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def get_day_boundary_slots(
    current_slot: int, 
    genesis_time: int, 
    seconds_per_slot: int
) -> Tuple[int, int]:
    """
    Calculate the first and last slot for the current day of the given slot in UTC.
    
    Returns:
        Tuple[int, int]: (start_slot, end_slot) for the day
    """
    # Get timestamp for current slot
    current_time = calculate_slot_timestamp(genesis_time, current_slot, seconds_per_slot)
    
    # Get start of day in UTC
    day_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get end of day in UTC
    day_end = day_start + timedelta(days=1, microseconds=-1)
    
    # Calculate slots
    start_slot = (int(day_start.timestamp()) - genesis_time) // seconds_per_slot
    end_slot = (int(day_end.timestamp()) - genesis_time) // seconds_per_slot
    
    return (start_slot, end_slot)

def is_last_slot_of_day(
    slot: int, 
    genesis_time: int, 
    seconds_per_slot: int
) -> bool:
    """
    Check if the given slot is the last slot of its day in UTC.
    
    Returns:
        bool: True if it's the last slot of the day
    """
    # Get timestamp for current slot
    slot_time = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
    
    # Get timestamp for next slot
    next_slot_time = calculate_slot_timestamp(genesis_time, slot + 1, seconds_per_slot)
    
    # If they're on different days, this is the last slot of the day
    return slot_time.date() != next_slot_time.date()

def slots_to_epoch(slot: int, slots_per_epoch: int) -> int:
    """Convert slot number to epoch number."""
    return slot // slots_per_epoch

def is_epoch_boundary_slot(slot: int, slots_per_epoch: int) -> bool:
    """Check if the slot is the last slot of an epoch."""
    return (slot + 1) % slots_per_epoch == 0

def get_relevant_validator_slots_in_range(
    historical_start_slot: int,
    historical_end_slot: int,
    genesis_time: int,
    seconds_per_slot: int
) -> List[int]:
    """
    Identifies the slots that are the last slot of their respective UTC days
    (verified by is_last_slot_of_day) and fall within the
    [historical_start_slot, historical_end_slot] range.
    """
    if historical_start_slot > historical_end_slot or not seconds_per_slot or seconds_per_slot <= 0:
        return []

    target_slots: List[int] = []
    
    try:
        start_slot_dt = calculate_slot_timestamp(genesis_time, historical_start_slot, seconds_per_slot)
        current_day_start_dt = start_slot_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end_slot_dt = calculate_slot_timestamp(genesis_time, historical_end_slot, seconds_per_slot)
    except ValueError: # Can happen if slot results in invalid timestamp (e.g. negative before epoch)
        return []

    while True:
        current_day_probe_slot_ts = int(current_day_start_dt.timestamp())
        
        if current_day_probe_slot_ts < genesis_time:
            current_day_start_dt += timedelta(days=1)
            if current_day_start_dt.date() > end_slot_dt.date():
                 break
            continue

        probe_slot_for_day_boundaries = (current_day_probe_slot_ts - genesis_time) // seconds_per_slot
        
        try:
            # Recalculate timestamp for probe_slot to ensure it's valid and adjust if needed
            probe_slot_time = calculate_slot_timestamp(genesis_time, probe_slot_for_day_boundaries, seconds_per_slot)
            if probe_slot_time < current_day_start_dt:
                probe_slot_for_day_boundaries += 1
            
            if probe_slot_for_day_boundaries < 0: # Should not happen if current_day_probe_slot_ts >= genesis_time
                 probe_slot_for_day_boundaries = 0
            
            _, day_end_slot = get_day_boundary_slots(
                probe_slot_for_day_boundaries, genesis_time, seconds_per_slot
            )
        except ValueError: # Error from timestamp calculation
            current_day_start_dt += timedelta(days=1)
            if current_day_start_dt.date() > end_slot_dt.date():
                 break
            continue
        except Exception: # Catch other potential errors from get_day_boundary_slots
            current_day_start_dt += timedelta(days=1)
            if current_day_start_dt.date() > end_slot_dt.date():
                 break
            continue

        if day_end_slot >= historical_start_slot and day_end_slot <= historical_end_slot:
            # *** Key Change: Verify with is_last_slot_of_day before adding ***
            if is_last_slot_of_day(day_end_slot, genesis_time, seconds_per_slot):
                target_slots.append(day_end_slot)

        if day_end_slot >= historical_end_slot or current_day_start_dt.date() >= end_slot_dt.date():
            break
            
        current_day_start_dt += timedelta(days=1)

    return sorted(list(set(target_slots)))


def update_time_helpers(clickhouse, genesis_time: int, seconds_per_slot: int, slots_per_epoch: int) -> None:
    """
    Update the time_helpers table with the current time parameters.
    This table is used for materialized column calculations in ClickHouse.
    Uses TRUNCATE to ensure only one row exists.
    """
    try:
        # First truncate the table to remove all existing rows
        truncate_query = "TRUNCATE TABLE time_helpers"
        clickhouse.execute(truncate_query)
        logger.info("Truncated time_helpers table")
        
        # Then insert the new values
        insert_query = """
        INSERT INTO time_helpers (genesis_time_unix, seconds_per_slot, slots_per_epoch)
        VALUES (%(genesis_time)s, %(seconds_per_slot)s, %(slots_per_epoch)s)
        """
        
        clickhouse.execute(insert_query, {
            "genesis_time": genesis_time,
            "seconds_per_slot": seconds_per_slot,
            "slots_per_epoch": slots_per_epoch
        })
        
        logger.info(f"Updated time_helpers: genesis_time={genesis_time}, seconds_per_slot={seconds_per_slot}, slots_per_epoch={slots_per_epoch}")
    except Exception as e:
        logger.error(f"Error updating time_helpers: {e}")

async def get_head_slot_time(beacon_api, clickhouse) -> Optional[datetime]:
    """
    Get the timestamp of the current head slot.
    
    Args:
        beacon_api: BeaconAPIService instance
        clickhouse: ClickHouseService instance
        
    Returns:
        datetime of the head slot, or None if unable to determine
    """
    try:
        # Get the latest block header
        header = await beacon_api.get_block_header("head")
        slot = int(header["header"]["message"]["slot"])
        
        # Get time parameters
        genesis_time = clickhouse.get_genesis_time()
        time_params = clickhouse.get_time_parameters()
        
        if not genesis_time or not time_params:
            return None
            
        seconds_per_slot = time_params.get('seconds_per_slot', 5)
        
        # Calculate timestamp
        return calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
        
    except Exception as e:
        logger.error(f"Error getting head slot time: {e}")
        return None