#!/usr/bin/env python3
# File: scripts/update_specs.py

import asyncio
import sys
import os
import traceback

# Add the parent directory to sys.path to allow importing
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.scrapers.specs_scraper import SpecsScraper
from src.utils.logger import setup_logger, logger

async def update_specs():
    """Run specs updater as a standalone script."""
    logger = setup_logger("specs_updater", log_level=20)  # INFO level
    logger.info("Starting specs updater")
    
    try:
        # Initialize services
        beacon_api = BeaconAPIService()
        clickhouse = ClickHouseService()
        
        # Start the beacon API
        await beacon_api.start()
        
        # Initialize and force-run specs scraper
        specs_scraper = SpecsScraper(beacon_api, clickhouse)
        
        # Force run by processing regardless of previous state
        seconds_per_slot, slots_per_epoch = await specs_scraper.process()
        
        # Mark as processed in the scraper state
        await specs_scraper.update_scraper_state(
            last_processed_slot=1,  # Just needs to be a non-zero value
            mode="one_time"
        )
        
        logger.info(f"Specs updated: seconds_per_slot={seconds_per_slot}, slots_per_epoch={slots_per_epoch}")
        
    except Exception as e:
        logger.error(f"Error updating specs: {e}")
        logger.error(traceback.format_exc())
        return 1
    finally:
        if 'beacon_api' in locals():
            await beacon_api.stop()
            
    logger.info("Specs updater completed successfully")
    return 0

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(update_specs())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Specs updater interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)