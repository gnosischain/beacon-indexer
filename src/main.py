#!/usr/bin/env python3
import asyncio
import argparse
import sys
import traceback
from typing import List
from datetime import datetime

from src.config import config
from src.utils.logger import logger, setup_logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService

# Import specs manager after services
from src.utils.specs_manager import SpecsManager

# Import scrapers
from src.scrapers.block_scraper import BlockScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.scrapers.blob_sidecar_scraper import BlobSidecarScraper
from src.scrapers.reward_scraper import RewardScraper
from src.scrapers.specs_scraper import SpecsScraper

from src.services.historical_service import HistoricalService
from src.services.realtime_service import RealtimeService
from src.services.parallel_service import ParallelService

def update_time_helpers(clickhouse: ClickHouseService, genesis_time: int, seconds_per_slot: int, slots_per_epoch: int) -> None:
    """Update the time helpers table with current values."""
    # First check if we need to clear existing data
    query = "SELECT COUNT(*) as count FROM time_helpers"
    result = clickhouse.execute(query)
    
    # Convert result to a list if it's a generator
    if hasattr(result, '__iter__') and not isinstance(result, list):
        result = list(result)
    
    count = result[0].get('count', 0) if result and len(result) > 0 else 0
    
    if count > 0:
        # Update existing data
        query = """
        ALTER TABLE time_helpers UPDATE 
        genesis_time_unix = %(genesis_time)s,
        seconds_per_slot = %(seconds_per_slot)s,
        slots_per_epoch = %(slots_per_epoch)s
        WHERE 1=1
        """
        
        clickhouse.execute(query, {
            "genesis_time": genesis_time,
            "seconds_per_slot": seconds_per_slot,
            "slots_per_epoch": slots_per_epoch
        })
    else:
        # Insert new data
        query = """
        INSERT INTO time_helpers (
            genesis_time_unix, seconds_per_slot, slots_per_epoch
        ) VALUES (
            %(genesis_time)s, %(seconds_per_slot)s, %(slots_per_epoch)s
        )
        """
        
        clickhouse.execute(query, {
            "genesis_time": genesis_time,
            "seconds_per_slot": seconds_per_slot,
            "slots_per_epoch": slots_per_epoch
        })
    
    logger.info("Updated time helpers table with current values")
    
async def setup_genesis(beacon_api: BeaconAPIService, clickhouse: ClickHouseService):
    """Setup genesis information if not already exists."""
    try:
        # Check if genesis information already exists
        query = "SELECT COUNT(*) as count FROM genesis"
        result = clickhouse.execute(query)
        
        # Convert the result to a list if it's a generator
        if hasattr(result, '__iter__') and not isinstance(result, list):
            result = list(result)
        
        if result and len(result) > 0 and result[0].get('count', 0) > 0:
            # Get existing genesis time for time_helpers
            query = "SELECT toUnixTimestamp(genesis_time) as genesis_unix FROM genesis LIMIT 1"
            result = clickhouse.execute(query)
            
            # Convert the result to a list if it's a generator
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            genesis_time = result[0].get('genesis_unix', 0) if result and len(result) > 0 else 0
            logger.info(f"Genesis information already exists in the database (unix time: {genesis_time})")
            return genesis_time
            
        # If not, fetch and store genesis information
        genesis_data = await beacon_api.get_genesis()
        
        # Convert genesis_time from string to timestamp
        genesis_time = int(genesis_data.get("genesis_time", 0))
        
        query = """
        INSERT INTO genesis (
            genesis_time, genesis_validators_root, genesis_fork_version
        ) VALUES (
            %(genesis_time)s, %(genesis_validators_root)s, %(genesis_fork_version)s
        )
        """
        
        clickhouse.execute(query, {
            "genesis_time": datetime.fromtimestamp(genesis_time),
            "genesis_validators_root": genesis_data.get("genesis_validators_root", ""),
            "genesis_fork_version": genesis_data.get("genesis_fork_version", "")
        })
        
        logger.info(f"Genesis information stored in the database (unix time: {genesis_time})")
        return genesis_time
    except Exception as e:
        logger.error(f"Error setting up genesis information: {e}")
        logger.debug(traceback.format_exc())
        return 0

async def main():
    # Ensure our logger is properly set up
    debug_logger = setup_logger("beacon_scraper", log_level=config.scraper.log_level)  # 10: DEBUG 20: INFO level
    debug_logger.debug("Starting beacon chain scraper")
    
    parser = argparse.ArgumentParser(description="Beacon Chain Scraper")
    parser.add_argument("--mode", choices=["historical", "realtime", "parallel"], default=config.scraper.mode,
                  help="Scraper mode: historical, realtime, or parallel")
    parser.add_argument("--scrapers", type=str, default=config.scraper.enabled_scrapers,
                  help="Comma-separated list of scrapers to enable (block,validator,reward,blob,specs)")
    parser.add_argument("--start-slot", type=int, default=config.scraper.historical_start_slot,
                      help="Starting slot for historical mode")
    parser.add_argument("--end-slot", type=int, default=config.scraper.historical_end_slot,
                      help="Ending slot for historical mode")
    parser.add_argument("--batch-size", type=int, default=config.scraper.batch_size,
                      help="Batch size for processing slots")
    parser.add_argument("--bulk-insert", action="store_true", 
                      help="Use bulk insert mode for better performance")
    parser.add_argument("--workers", type=int, default=4,
                      help="Number of parallel workers (for parallel mode)")
    parser.add_argument("--update-specs", action="store_true",
                      help="Force update specs even if they have been processed before")
    

    args = parser.parse_args()
    debug_logger.debug(f"Parsed arguments: {args}")
    
    # Parse enabled scrapers
    enabled_scrapers = [s.strip() for s in args.scrapers.split(',')]
    debug_logger.info(f"Enabled scrapers: {enabled_scrapers}")
    try:
        # Initialize services
        debug_logger.debug(f"Initializing services with config: {config}")
        beacon_api = BeaconAPIService()
        clickhouse = ClickHouseService()
        
        debug_logger.debug("Starting beacon API service")
        # Start the beacon API
        await beacon_api.start()
        
        # Setup genesis information
        debug_logger.debug("Setting up genesis information")
        genesis_time = await setup_genesis(beacon_api, clickhouse)
        
        # Initialize specs scraper
        debug_logger.debug("Running specs scraper")
        specs_scraper = SpecsScraper(beacon_api, clickhouse)
        
        # If specs are already in database and scraper has run before, skip the API call
        # unless --update-specs flag is set
        has_specs = await specs_scraper.has_data()
        already_ran = await specs_scraper.get_last_processed_slot() > 0
        
        if args.update_specs or not has_specs or not already_ran:
            debug_logger.info("Updating specs from beacon node")
            seconds_per_slot, slots_per_epoch = await specs_scraper.process()
        else:
            debug_logger.info("Using existing specs from database")
            await specs_scraper._load_current_specs()
            seconds_per_slot = int(specs_scraper.current_specs.get("SECONDS_PER_SLOT", 12))
            slots_per_epoch = int(specs_scraper.current_specs.get("SLOTS_PER_EPOCH", 32))
            
        # Update time helpers
        debug_logger.debug("Updating time helpers")
        update_time_helpers(clickhouse, genesis_time, seconds_per_slot, slots_per_epoch)
        
        # Initialize specs manager
        specs_manager = SpecsManager(clickhouse)
        debug_logger.debug(f"Refreshed specs cache, found {len(specs_manager.get_all_specs())} parameters")
        
        # Initialize only selected scrapers
        debug_logger.debug("Initializing scrapers")
        scrapers = []
        
        if "block" in enabled_scrapers:
            scrapers.append(BlockScraper(beacon_api, clickhouse))
            debug_logger.debug("Added BlockScraper")
            
        if "validator" in enabled_scrapers:
            scrapers.append(ValidatorScraper(beacon_api, clickhouse))
            debug_logger.debug("Added ValidatorScraper")
            
        if "reward" in enabled_scrapers:
            scrapers.append(RewardScraper(beacon_api, clickhouse))
            debug_logger.debug("Added RewardScraper")
            
        if "blob" in enabled_scrapers:
            scrapers.append(BlobSidecarScraper(beacon_api, clickhouse))
            debug_logger.debug("Added BlobSidecarScraper")
        
        # Only include SpecsScraper in the regular scrapers list if:
        # 1. It's explicitly enabled AND
        # 2. Either the --update-specs flag is set OR the specs haven't been processed yet
        if "specs" in enabled_scrapers and (args.update_specs or not already_ran):
            specs_scraper_instance = SpecsScraper(beacon_api, clickhouse)
            scrapers.append(specs_scraper_instance)
            debug_logger.debug("Added SpecsScraper to regular scrapers")
        
        if not scrapers:
            debug_logger.warning("No scrapers enabled. Please enable at least one scraper.")
            return
        
        # Set specs_manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
        
        debug_logger.debug(f"Using dynamic poll interval of {specs_manager.get_seconds_per_slot()} seconds")
        debug_logger.debug(f"Starting service in {args.mode} mode")
        
        # Start the appropriate service based on mode
        if args.mode == "historical":
            service = HistoricalService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                specs_manager=specs_manager,
                start_slot=args.start_slot,
                end_slot=args.end_slot,
                batch_size=args.batch_size
            )
            await service.start()
        elif args.mode == "parallel":
            # Use the new parallel service
            service = ParallelService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                specs_manager=specs_manager,
                start_slot=args.start_slot,
                end_slot=args.end_slot,
                num_workers=args.workers,
                worker_batch_size=args.batch_size
            )
            await service.start()
        else:  # realtime
            service = RealtimeService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                poll_interval=specs_manager.get_seconds_per_slot()
            )
            await service.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        debug_logger.error(f"Error in main process: {e}")
        debug_logger.error(traceback.format_exc())
        print(f"ERROR: {e}")
        print(traceback.format_exc())
    finally:
        if 'beacon_api' in locals():
            debug_logger.debug("Stopping beacon API service")
            await beacon_api.stop()
        debug_logger.info("Scraper stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Fatal error: {e}")
        print(traceback.format_exc())
        sys.exit(1)