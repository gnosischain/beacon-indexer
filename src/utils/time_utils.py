from typing import Dict, Tuple, Optional
from datetime import datetime, timezone, timedelta

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