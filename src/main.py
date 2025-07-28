#!/usr/bin/env python3
import asyncio
import sys
import os
import signal
import traceback
from typing import List, Optional

from src.config import config, OperationType
from src.utils.logger import logger, setup_logger
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.utils.specs_manager import SpecsManager
from src.operations.operation_factory import OperationFactory

# Import scrapers
from src.scrapers.core_block_scraper import CoreBlockScraper
from src.scrapers.operational_events_scraper import OperationalEventsScraper
from src.scrapers.validator_scraper import ValidatorScraper
from src.scrapers.blob_sidecar_scraper import BlobSidecarScraper
from src.scrapers.reward_scraper import RewardScraper
from src.scrapers.specs_scraper import SpecsScraper
from src.scrapers.transaction_scraper import TransactionScraper
from src.scrapers.slashing_scraper import SlashingScraper
from src.scrapers.attestation_scraper import AttestationScraper


# Global shutdown event
shutdown_event = asyncio.Event()


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()


async def setup_genesis(beacon_api: BeaconAPIService, clickhouse: ClickHouseService) -> int:
    """Setup genesis information if not already exists. Returns genesis time."""
    try:
        # Check if genesis data exists
        query = "SELECT genesis_time FROM genesis LIMIT 1"
        result = clickhouse.execute(query)
        
        # Convert result to list if needed
        if hasattr(result, '__iter__') and not isinstance(result, list):
            result = list(result)
            
        if result and result[0]['genesis_time']:
            genesis_time = result[0]['genesis_time']
            logger.info(f"Genesis time loaded from database: {genesis_time}")
            return genesis_time
        
        # No genesis data found, fetch from beacon node
        logger.info("No genesis data found, fetching from beacon node...")
        genesis = await beacon_api.get_genesis()
        
        genesis_time = int(genesis['genesis_time'])
        
        # Insert genesis data
        insert_query = """
        INSERT INTO genesis (
            genesis_time, genesis_fork_version, genesis_validators_root
        ) VALUES (
            %(genesis_time)s, %(genesis_fork_version)s, %(genesis_validators_root)s
        )
        """
        
        clickhouse.execute(insert_query, {
            'genesis_time': genesis_time,
            'genesis_fork_version': genesis['genesis_fork_version'],
            'genesis_validators_root': genesis['genesis_validators_root']
        })
        
        logger.info("Genesis data saved to database")
        return genesis_time
            
    except Exception as e:
        logger.error(f"Error setting up genesis: {e}")
        raise


def update_time_helpers(clickhouse: ClickHouseService, genesis_time: int, 
                       seconds_per_slot: int, slots_per_epoch: int) -> None:
    """Update the time helpers table with current values."""
    try:
        # Check if we need to update
        query = "SELECT COUNT(*) as count FROM time_helpers"
        result = clickhouse.execute(query)
        
        # Convert result to list if needed
        if hasattr(result, '__iter__') and not isinstance(result, list):
            result = list(result)
            
        count = result[0]['count'] if result else 0
        
        if count > 0:
            # Update existing data
            query = """
            ALTER TABLE time_helpers UPDATE 
            genesis_time_unix = %(genesis_time)s,
            seconds_per_slot = %(seconds_per_slot)s,
            slots_per_epoch = %(slots_per_epoch)s
            WHERE 1=1
            """
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
            'genesis_time': genesis_time,
            'seconds_per_slot': seconds_per_slot,
            'slots_per_epoch': slots_per_epoch
        })
        
        logger.info("Updated time helpers table with current values")
        
    except Exception as e:
        logger.error(f"Error updating time helpers: {e}")


def get_enabled_scrapers(
    beacon_api: BeaconAPIService,
    clickhouse: ClickHouseService,
    enabled_names: List[str]
) -> List:
    """Initialize enabled scrapers based on configuration."""
    scraper_map = {
        'block': CoreBlockScraper,
        'core_block': CoreBlockScraper,
        'operational_events': OperationalEventsScraper,
        'validator': ValidatorScraper,
        'blob': BlobSidecarScraper,
        'reward': RewardScraper,
        'specs': SpecsScraper,
        'transaction': TransactionScraper,
        'slashing': SlashingScraper,
        'attestation': AttestationScraper,
    }
    
    scrapers = []
    for name in enabled_names:
        scraper_class = scraper_map.get(name.lower())
        if scraper_class:
            scraper = scraper_class(beacon_api, clickhouse)
            scrapers.append(scraper)
            logger.info(f"Enabled scraper: {scraper.scraper_id}")
        else:
            logger.warning(f"Unknown scraper: {name}")
            
    return scrapers


async def cleanup_disabled_scrapers_state(clickhouse: ClickHouseService, 
                                        enabled_scrapers: List) -> None:
    """Clean up state entries for disabled scrapers."""
    try:
        # Get enabled scraper IDs and their datasets
        from src.core.datasets import DatasetRegistry
        registry = DatasetRegistry()
        
        valid_datasets = set()
        for scraper in enabled_scrapers:
            datasets = registry.get_datasets_for_scraper(scraper.scraper_id)
            for dataset in datasets:
                valid_datasets.add(dataset.name)
                
        logger.info(f"Valid datasets for enabled scrapers: {valid_datasets}")
        
        # Find and remove invalid entries
        query = """
        SELECT DISTINCT dataset, COUNT(*) as count
        FROM indexing_state
        GROUP BY dataset
        """
        
        result = clickhouse.execute(query)
        
        # Convert result to list
        if hasattr(result, '__iter__') and not isinstance(result, list):
            result = list(result)
            
        for row in result:
            dataset = row['dataset']
            count = row['count']
            
            if dataset not in valid_datasets:
                logger.warning(f"Found {count} entries for disabled dataset: {dataset}")
                # Optionally delete them
                # delete_query = f"ALTER TABLE indexing_state DELETE WHERE dataset = '{dataset}'"
                # clickhouse.execute(delete_query)
                
    except Exception as e:
        logger.error(f"Error cleaning up disabled scrapers state: {e}")


async def main():
    """Main entry point."""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get configuration from environment
    mode = os.getenv("SCRAPER_MODE", "realtime").lower()
    enabled_scrapers_str = os.getenv("ENABLED_SCRAPERS", "core_block,operational_events,validator")
    start_slot = int(os.getenv("HISTORICAL_START_SLOT", "0"))
    end_slot_str = os.getenv("HISTORICAL_END_SLOT", "")
    end_slot = int(end_slot_str) if end_slot_str else None
    batch_size = int(os.getenv("BATCH_SIZE", "1000"))
    parallel_workers = int(os.getenv("PARALLEL_WORKERS", "4"))
    
    logger.info("Starting beacon scraper")
    logger.info(f"Configuration loaded from environment:")
    logger.info(f"  Mode: {mode}")
    logger.info(f"  Enabled scrapers: {enabled_scrapers_str}")
    logger.info(f"  Start slot: {start_slot}")
    logger.info(f"  End slot: {end_slot}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info(f"  Parallel workers: {parallel_workers}")
    
    # Parse enabled scrapers
    enabled_scraper_names = [s.strip() for s in enabled_scrapers_str.split(',')]
    
    beacon_api = None
    try:
        # Initialize services
        beacon_api = BeaconAPIService()
        clickhouse = ClickHouseService()
        
        # Initialize state manager
        state_manager = StateManager(clickhouse)
        
        # Start beacon API
        await beacon_api.start()
        
        # Setup genesis and get genesis time
        genesis_time = await setup_genesis(beacon_api, clickhouse)
        
        # Initialize specs manager
        specs_manager = SpecsManager(clickhouse)
        
        # Get enabled scrapers
        enabled_scrapers = get_enabled_scrapers(
            beacon_api, clickhouse, enabled_scraper_names
        )
        
        if not enabled_scrapers:
            logger.error("No scrapers enabled!")
            return
            
        # Set specs manager for each scraper
        for scraper in enabled_scrapers:
            scraper.set_specs_manager(specs_manager)
            
        # Update specs if needed
        specs_scraper = next(
            (s for s in enabled_scrapers if isinstance(s, SpecsScraper)), 
            None
        )
        
        if specs_scraper:
            logger.info("Updating chain specs...")
            processed, _ = await specs_scraper.process()
            logger.info(f"Updated {processed} chain spec parameters")
        else:
            # Create temporary specs scraper to update specs
            temp_specs_scraper = SpecsScraper(beacon_api, clickhouse)
            temp_specs_scraper.set_specs_manager(specs_manager)
            processed, _ = await temp_specs_scraper.process()
            logger.info(f"Updated {processed} chain spec parameters")
            
        # Get time parameters from specs manager
        seconds_per_slot = specs_manager.get_seconds_per_slot()
        slots_per_epoch = specs_manager.get_slots_per_epoch()
        
        logger.info(f"Updated time specs: seconds_per_slot={seconds_per_slot}, "
                   f"slots_per_epoch={slots_per_epoch}")
                   
        # Update time helpers with genesis time from database
        update_time_helpers(clickhouse, genesis_time, seconds_per_slot, slots_per_epoch)
        
        # Display active scrapers
        active_scraper_ids = [s.scraper_id for s in enabled_scrapers]
        logger.info(f"Active scrapers: {active_scraper_ids}")
        
        # Clean up state for disabled scrapers
        logger.info("Cleaning up state entries for disabled scrapers...")
        await cleanup_disabled_scrapers_state(clickhouse, enabled_scrapers)
        
        # Reset stale processing jobs
        logger.info("Resetting stale processing jobs...")
        reset_count = state_manager.reset_stale_processing_jobs()
        if reset_count > 0:
            logger.info(f"Reset {reset_count} stale processing jobs")
            
        # Map mode string to OperationType
        operation_type_map = {
            'realtime': OperationType.CONTINUOUS,
            'continuous': OperationType.CONTINUOUS,
            'historical': OperationType.HISTORICAL,
            'parallel': OperationType.HISTORICAL,  # Parallel is a variant of historical
            'fill_gaps': OperationType.FILL_GAPS,
            'validate': OperationType.VALIDATE,
            'backfill': OperationType.BACKFILL,
            'consolidate': OperationType.CONSOLIDATE,
            'process_failed': OperationType.PROCESS_FAILED,
        }
        
        operation_type = operation_type_map.get(mode)
        if not operation_type:
            raise ValueError(f"Unknown mode: {mode}")
            
        # Create operation configuration
        operation_config = {
            'mode': mode,
            'start_slot': start_slot,
            'end_slot': end_slot,
            'batch_size': batch_size,
            'num_workers': parallel_workers,
            'datasets': [],  # Let operation determine from scrapers
            'force': False
        }
        
        # Special handling for parallel mode
        if mode == 'parallel':
            operation_config['num_workers'] = parallel_workers
        
        # Create and execute operation
        operation = OperationFactory.create(
            operation_type=operation_type,
            beacon_api=beacon_api,
            clickhouse=clickhouse,
            state_manager=state_manager,
            scrapers=enabled_scrapers,
            config=operation_config
        )
        
        # Execute operation
        logger.info(f"Starting service in {mode} mode")
        result = await operation.execute()
        
        # Log results
        logger.info(f"Operation completed: {result}")
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Close beacon API properly if it exists and has session
        if beacon_api and hasattr(beacon_api, 'session') and beacon_api.session:
            await beacon_api.session.close()
        logger.info("Scraper stopped")


if __name__ == "__main__":
    # Setup root logger
    setup_logger("beacon_scraper", log_level=config.scraper.log_level)
    
    # Run main
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)