#!/usr/bin/env python3
import asyncio
import sys
import traceback
from typing import List
from datetime import datetime
import traceback

from src.config import config
from src.utils.logger import logger, setup_logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager

# Import specs manager after services
from src.utils.specs_manager import SpecsManager

# Import scrapers
from src.scrapers.block_scraper import BlockScraper
from src.scrapers.core_block_scraper import CoreBlockScraper
from src.scrapers.attestation_scraper import AttestationScraper
from src.scrapers.operational_events_scraper import OperationalEventsScraper
from src.scrapers.transaction_scraper import TransactionScraper
from src.scrapers.slashing_scraper import SlashingScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.scrapers.blob_sidecar_scraper import BlobSidecarScraper
from src.scrapers.reward_scraper import RewardScraper
from src.scrapers.specs_scraper import SpecsScraper

# Import services
from src.services.historical_service import HistoricalService
from src.services.realtime_service import RealtimeService
from src.services.thread_pool_parallel_service import ThreadPoolParallelService


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
    debug_logger = setup_logger("beacon_scraper", log_level=config.scraper.log_level)
    debug_logger.info("Starting beacon chain scraper")
    
    # Get configuration from environment variables
    mode = config.scraper.mode
    enabled_scrapers_str = config.scraper.enabled_scrapers
    start_slot = config.scraper.historical_start_slot
    end_slot = config.scraper.historical_end_slot
    batch_size = config.scraper.batch_size
    parallel_workers = config.scraper.parallel_workers
    
    debug_logger.info(f"Configuration loaded from environment:")
    debug_logger.info(f"  Mode: {mode}")
    debug_logger.info(f"  Enabled scrapers: {enabled_scrapers_str}")
    debug_logger.info(f"  Start slot: {start_slot}")
    debug_logger.info(f"  End slot: {end_slot}")
    debug_logger.info(f"  Batch size: {batch_size}")
    debug_logger.info(f"  Parallel workers: {parallel_workers}")
    
    # Parse enabled scrapers
    enabled_scrapers = [s.strip() for s in enabled_scrapers_str.split(',')]
    debug_logger.info(f"Parsed enabled scrapers: {enabled_scrapers}")
    
    try:
        # Initialize services
        debug_logger.debug(f"Initializing services with config: {config}")
        beacon_api = BeaconAPIService()
        clickhouse = ClickHouseService()
        
        # Initialize state manager
        state_manager = StateManager(clickhouse)
        
        # Reset any stale processing jobs at startup
        debug_logger.info("Resetting stale processing jobs...")
        try:
            # Try to use the method if it exists
            if hasattr(state_manager, 'reset_all_processing_jobs'):
                state_manager.reset_all_processing_jobs()
            else:
                # Fallback: reset processing jobs directly
                query = """
                INSERT INTO indexing_state 
                (mode, dataset, start_slot, end_slot, status, version)
                SELECT 
                    mode, dataset, start_slot, end_slot, 
                    'pending' as status, now() as version
                FROM indexing_state FINAL
                WHERE status = 'processing'
                """
                clickhouse.execute(query, {})
                debug_logger.info("Reset stale processing jobs using fallback method")
        except Exception as e:
            debug_logger.warning(f"Could not reset processing jobs: {e}")
            # Continue anyway - this is not critical
        
        debug_logger.debug("Starting beacon API service")
        # Start the beacon API
        await beacon_api.start()
        
        # Setup genesis information
        debug_logger.debug("Setting up genesis information")
        await setup_genesis(beacon_api, clickhouse)
        
        # Get genesis time from database for time calculations
        genesis_query = "SELECT toUnixTimestamp(genesis_time) as genesis_time FROM genesis LIMIT 1"
        genesis_results = clickhouse.execute(genesis_query)
        genesis_time = 0
        if genesis_results and len(genesis_results) > 0:
            genesis_time = genesis_results[0].get("genesis_time", 0)
        
        # Initialize specs scraper
        debug_logger.debug("Running specs scraper")
        specs_scraper = SpecsScraper(beacon_api, clickhouse)
        
        # If specs are already in database and the scraper is not explicitly enabled, skip it
        already_ran = await specs_scraper.get_last_processed_slot() > 0
        if already_ran and "specs" not in enabled_scrapers:
            debug_logger.info("Specs already exist in database and specs scraper not explicitly enabled, skipping specs scraper")
        else:
            debug_logger.info("Running specs scraper to get current network specs")
            mock_block = {"data": {"message": {"slot": 0}}}
            await specs_scraper.process(mock_block)
        
        # Initialize specs manager
        debug_logger.debug("Initializing specs manager")
        specs_manager = SpecsManager(clickhouse)
        
        # Update time helpers
        update_time_helpers(
            clickhouse,
            genesis_time,
            specs_manager.get_seconds_per_slot(),
            specs_manager.get_slots_per_epoch()
        )
        
        # Create scraper instances based on enabled scrapers
        scrapers: List = []
        
        # Map of user-friendly names to scraper classes and IDs
        scraper_mapping = {
            "core_block": (CoreBlockScraper, "core_block_scraper"),
            "attestation": (AttestationScraper, "attestation_scraper"),
            "operational_events": (OperationalEventsScraper, "operational_events_scraper"),
            "transaction": (TransactionScraper, "transaction_scraper"),
            "slashing": (SlashingScraper, "slashing_scraper"),
            "validator": (ValidatorScraper, "validator_scraper"),
            "reward": (RewardScraper, "reward_scraper"),
            "blob": (BlobSidecarScraper, "blob_sidecar_scraper"),
            "specs": (SpecsScraper, "specs_scraper"),
            # Legacy mapping - uses the old comprehensive BlockScraper
            "block": (BlockScraper, "block_scraper")
        }
        
        # Track which scrapers have been added to avoid duplicates
        added_scraper_ids = set()
        
        # Create scrapers based on enabled list
        for scraper_name in enabled_scrapers:
            if scraper_name in scraper_mapping:
                scraper_class, scraper_id = scraper_mapping[scraper_name]
                
                # Avoid duplicates
                if scraper_id not in added_scraper_ids:
                    # Create the scraper instance
                    scraper = scraper_class(beacon_api, clickhouse)
                    # Ensure the scraper_id matches what the dataset registry expects
                    scraper.scraper_id = scraper_id
                    scrapers.append(scraper)
                    added_scraper_ids.add(scraper_id)
                    debug_logger.debug(f"Added {scraper_class.__name__} with ID: {scraper_id}")
            else:
                debug_logger.warning(f"Unknown scraper: {scraper_name}")
        
        # Add specs scraper only if explicitly enabled and not already processed
        if "specs" in enabled_scrapers and not already_ran:
            if "specs_scraper" not in added_scraper_ids:
                specs_scraper_instance = SpecsScraper(beacon_api, clickhouse)
                specs_scraper_instance.scraper_id = "specs_scraper"
                scrapers.append(specs_scraper_instance)
                debug_logger.debug("Added SpecsScraper to regular scrapers")
        
        if not scrapers:
            debug_logger.warning("No scrapers enabled. Please enable at least one scraper.")
            return
        
        # Log the scrapers we're using with their IDs
        debug_logger.info(f"Active scrapers: {[s.scraper_id for s in scrapers]}")
        
        # Set specs_manager and state_manager for all scrapers
        for scraper in scrapers:
            scraper.set_specs_manager(specs_manager)
            if hasattr(scraper, 'set_state_manager'):
                scraper.set_state_manager(state_manager)
        
        debug_logger.debug(f"Using dynamic poll interval of {specs_manager.get_seconds_per_slot()} seconds")
        debug_logger.info(f"Starting service in {mode} mode")
        
        # Start the appropriate service based on mode
        if mode == "historical":
            service = HistoricalService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                specs_manager=specs_manager,
                start_slot=start_slot,
                end_slot=end_slot,
                batch_size=batch_size
            )
            await service.start()
        elif mode == "parallel":
            # Use the thread pool parallel service
            service = ThreadPoolParallelService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                specs_manager=specs_manager,
                start_slot=start_slot,
                end_slot=end_slot,
                num_workers=parallel_workers,
                worker_batch_size=batch_size
            )
            await service.start()
        else:  # realtime (default)
            service = RealtimeService(
                beacon_api=beacon_api,
                clickhouse=clickhouse,
                scrapers=scrapers,
                specs_manager=specs_manager,
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