from datetime import datetime, timezone, timedelta
from typing import List, Optional
from src.utils.logger import logger

def calculate_slot_timestamp(genesis_time: int, slot: int, seconds_per_slot: int) -> datetime:
    """Calculate UTC timestamp for a given slot."""
    timestamp = genesis_time + (slot * seconds_per_slot)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def is_last_slot_of_day(slot: int, genesis_time: int, seconds_per_slot: int) -> bool:
    """
    Check if the given slot is the last slot of its day in UTC.
    
    Returns:
        bool: True if it's the last slot of the day
    """
    try:
        # Get timestamp for current slot
        slot_time = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
        
        # Get timestamp for next slot
        next_slot_time = calculate_slot_timestamp(genesis_time, slot + 1, seconds_per_slot)
        
        # If they're on different days, this is the last slot of the day
        return slot_time.date() != next_slot_time.date()
    except Exception as e:
        logger.error("Error checking if slot is last of day", slot=slot, error=str(e))
        return False

def get_last_slot_of_day(date_str: str, genesis_time: int, seconds_per_slot: int) -> Optional[int]:
    """
    Get the last slot number for a given date (YYYY-MM-DD format).
    
    Args:
        date_str: Date in YYYY-MM-DD format
        genesis_time: Genesis timestamp
        seconds_per_slot: Seconds per slot
        
    Returns:
        Slot number or None if date is before genesis
    """
    try:
        # Parse the date and get end of day
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        end_of_day = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        # Calculate approximate slot
        end_timestamp = int(end_of_day.timestamp())
        if end_timestamp < genesis_time:
            return None
            
        approximate_slot = (end_timestamp - genesis_time) // seconds_per_slot
        
        # Search backwards to find the actual last slot of the day
        for slot in range(approximate_slot, approximate_slot - 200, -1):  # Check up to ~200 slots back
            if slot < 0:
                break
                
            slot_time = calculate_slot_timestamp(genesis_time, slot, seconds_per_slot)
            if slot_time.date() == target_date:
                # Check if this is the last slot of the day
                if is_last_slot_of_day(slot, genesis_time, seconds_per_slot):
                    return slot
                    
        return None
        
    except Exception as e:
        logger.error("Error getting last slot of day", date=date_str, error=str(e))
        return None

def get_validator_target_slots_in_range(
    start_slot: int, 
    end_slot: int, 
    genesis_time: int, 
    seconds_per_slot: int
) -> List[int]:
    """
    Get all slots in the range that should be processed for validators (last slot of each day).
    
    Args:
        start_slot: Start slot (inclusive)
        end_slot: End slot (inclusive) 
        genesis_time: Genesis timestamp
        seconds_per_slot: Seconds per slot
        
    Returns:
        List of target slots for validator processing
    """
    target_slots = []
    
    try:
        # Get the date range
        start_date = calculate_slot_timestamp(genesis_time, start_slot, seconds_per_slot).date()
        end_date = calculate_slot_timestamp(genesis_time, end_slot, seconds_per_slot).date()
        
        # Iterate through each day and find the last slot
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            last_slot = get_last_slot_of_day(date_str, genesis_time, seconds_per_slot)
            
            if last_slot is not None and start_slot <= last_slot <= end_slot:
                target_slots.append(last_slot)
                
            current_date += timedelta(days=1)
            
        target_slots.sort()
        logger.debug("Found validator target slots", 
                    count=len(target_slots),
                    first_few=target_slots[:5] if target_slots else [])
        
        return target_slots
        
    except Exception as e:
        logger.error("Error getting validator target slots", 
                    start_slot=start_slot, 
                    end_slot=end_slot,
                    error=str(e))
        return []