import asyncio
import math 
import multiprocessing as mp
from datetime import datetime
from typing import List
from src.services.beacon_api import BeaconAPI
from src.services.clickhouse import ClickHouse
from src.loaders import get_enabled_loaders
from src.config import config
from src.utils.logger import logger

class LoaderService:
    """Main service for loading raw beacon data."""
    
    def __init__(self):
        self.beacon_api = None
        self.clickhouse = None
        self.loaders = []
    
    async def initialize(self):
        """Initialize services and loaders."""
        self.beacon_api = BeaconAPI()
        await self.beacon_api.start()
        
        self.clickhouse = ClickHouse()
        
        # Get enabled loaders
        self.loaders = get_enabled_loaders(
            config.ENABLED_LOADERS,
            self.beacon_api,
            self.clickhouse
        )
        
        logger.info("Loader service initialized", 
                   loaders=[loader.name for loader in self.loaders])
    
    async def cleanup(self):
        """Clean up resources."""
        if self.beacon_api:
            await self.beacon_api.close()
    
    async def realtime(self):
        """Run realtime loading."""
        logger.info("Starting realtime loader")
        
        # First, ensure specs and genesis are loaded
        await self._ensure_foundation_data()
        
        # Get the last slot we have in raw data (instead of using sync_progress)
        last_slot = self._get_last_raw_slot()
        
        while True:
            try:
                # Get current head slot
                head_slot = await self.beacon_api.get_head_slot()
                if head_slot is None:
                    logger.warning("Could not get head slot, retrying...")
                    await asyncio.sleep(12)  # Wait one slot
                    continue
                
                # Process new slots
                processed_any = False
                for slot in range(last_slot + 1, head_slot + 1):
                    await self._process_slot(slot)
                    last_slot = slot
                    processed_any = True
                
                if processed_any:
                    logger.info("Processed realtime slots", 
                               last_processed=last_slot,
                               head_slot=head_slot)
                
                # Wait before next check
                await asyncio.sleep(6)  # Half slot time
                
            except Exception as e:
                logger.error("Error in realtime loader", error=str(e))
                await asyncio.sleep(12)
    
    def _get_last_raw_slot(self) -> int:
        """Get the highest slot we have in raw data across all tables."""
        try:
            # Check both raw_blocks and raw_validators for the highest slot
            blocks_query = "SELECT max(slot) as max_slot FROM raw_blocks"
            validators_query = "SELECT max(slot) as max_slot FROM raw_validators FINAL"
            
            blocks_result = self.clickhouse.execute(blocks_query)
            validators_result = self.clickhouse.execute(validators_query)
            
            max_blocks_slot = 0
            max_validators_slot = 0
            
            if blocks_result and blocks_result[0]["max_slot"] is not None:
                max_blocks_slot = blocks_result[0]["max_slot"]
            
            if validators_result and validators_result[0]["max_slot"] is not None:
                max_validators_slot = validators_result[0]["max_slot"]
            
            last_slot = max(max_blocks_slot, max_validators_slot)
            logger.info("Starting realtime from last raw slot", 
                       last_slot=last_slot,
                       max_blocks=max_blocks_slot,
                       max_validators=max_validators_slot)
            
            return last_slot
            
        except Exception as e:
            logger.warning("Could not determine last raw slot, starting from 0", error=str(e))
            return 0
    
    async def _process_slot(self, slot: int):
        """Process a single slot with all enabled loaders."""
        for loader in self.loaders:
            try:
                # Skip one-time loaders in slot processing
                if loader.name in ["genesis", "specs"]:
                    continue
                
                # Check if this loader should process this slot
                if hasattr(loader, 'should_process_slot'):
                    if not loader.should_process_slot(slot):
                        logger.debug("Skipping slot for loader", 
                                   slot=slot, 
                                   loader=loader.name)
                        continue
                
                # For blocks loader, always process
                if loader.name == "blocks":
                    await loader.load_single(slot)
                # For validators loader, process based on mode
                elif loader.name == "validators":
                    await loader.load_single(slot)  # Pass slot directly
                # Other loaders can decide their own logic
                else:
                    await loader.load_single(slot)
                    
            except Exception as e:
                logger.error("Loader failed", 
                           loader=loader.name, 
                           slot=slot, 
                           error=str(e))
    
    async def backfill(self, start_slot: int, end_slot: int):
        """Run backfill loading with multiple workers."""
        logger.info("Starting backfill loader", 
                   start_slot=start_slot, 
                   end_slot=end_slot,
                   workers=config.BACKFILL_WORKERS)
        
        # FIRST: Ensure foundation data is loaded
        await self._ensure_foundation_data()
        
        # Generate chunks intelligently for each loader
        slot_based_loaders = [name for name in config.ENABLED_LOADERS 
                             if name not in ["genesis", "specs"]]
        
        logger.info("Generating smart chunks for loaders", 
                   slot_loaders=slot_based_loaders,
                   excluded=["genesis", "specs"])
        
        self._generate_smart_chunks(start_slot, end_slot, slot_based_loaders)
        
        # Start worker processes
        with mp.Pool(config.BACKFILL_WORKERS) as pool:
            tasks = []
            for i in range(config.BACKFILL_WORKERS):
                worker_id = f"worker_{i}"
                task = pool.apply_async(self._worker_process, (worker_id,))
                tasks.append(task)
            
            # Wait for all workers to complete
            for task in tasks:
                task.get()
        
        logger.info("Backfill completed")

    def _generate_smart_chunks(self, start_slot: int, end_slot: int, enabled_loaders: list):
        """Generate chunks with bulk existing chunk detection to avoid memory issues."""
        chunks_to_create = []
        current_time = datetime.now()
        
        # Pre-load existing completed chunks for all loaders to avoid repeated FINAL queries
        existing_chunks_cache = {}
        for loader_name in enabled_loaders:
            if loader_name in ["blocks", "validators", "rewards"]:
                try:
                    logger.info("Pre-loading existing chunks", loader=loader_name)
                    
                    # Use a single FINAL query per loader instead of thousands of individual queries
                    existing_query = """
                    SELECT DISTINCT start_slot, end_slot 
                    FROM load_state_chunks FINAL
                    WHERE loader_name = {loader_name:String}
                    AND status = 'completed'
                    AND start_slot >= {start_slot:UInt64}
                    AND end_slot < {end_slot:UInt64}
                    """
                    existing_results = self.clickhouse.execute(existing_query, {
                        "loader_name": loader_name,
                        "start_slot": start_slot,
                        "end_slot": end_slot
                    })
                    
                    # Cache as set of (start_slot, end_slot) tuples for fast lookup
                    existing_chunks_cache[loader_name] = {
                        (row["start_slot"], row["end_slot"]) for row in existing_results
                    }
                    logger.info("Cached existing chunks", 
                            loader=loader_name, 
                            existing_count=len(existing_chunks_cache[loader_name]))
                    
                except Exception as e:
                    logger.warning("Failed to pre-load existing chunks, assuming none exist", 
                                loader=loader_name, error=str(e))
                    existing_chunks_cache[loader_name] = set()
            else:
                # For non-slot based loaders, no caching needed
                existing_chunks_cache[loader_name] = set()
        
        # Now generate chunks using the cache (no more individual database queries)
        for loader_name in enabled_loaders:
            logger.info("Generating chunks for loader", loader=loader_name)
            
            if loader_name in ["blocks", "rewards"]:  # Both are slot-based
                existing_chunks = existing_chunks_cache.get(loader_name, set())
                
                # Blocks and Rewards: process all slots (FIXED: end_slot is now exclusive)
                for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                    chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                    chunk_id = f"{loader_name}_{i}_{chunk_end}"
                    
                    # Fast cache lookup instead of expensive database query
                    if (i, chunk_end) in existing_chunks:
                        logger.debug("Range already completed, skipping", 
                                loader=loader_name, 
                                start_slot=i, 
                                end_slot=chunk_end)
                        continue
                    
                    # New chunk - add to creation list
                    chunks_to_create.append({
                        "chunk_id": chunk_id,
                        "start_slot": i,
                        "end_slot": chunk_end,
                        "loader_name": loader_name,
                        "status": "pending",
                        "worker_id": "",
                        "created_at": current_time,
                        "updated_at": current_time
                    })
            
            elif loader_name == "validators":
                existing_chunks = existing_chunks_cache.get(loader_name, set())
                
                # Validators: only create chunks for target slots based on mode
                if config.VALIDATOR_MODE == "all_slots":
                    # Process all slots (FIXED: end_slot is now exclusive)
                    for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                        chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                        chunk_id = f"{loader_name}_{i}_{chunk_end}"
                        
                        # Fast cache lookup instead of database query
                        if (i, chunk_end) in existing_chunks:
                            logger.debug("Range already completed, skipping", 
                                    loader=loader_name, 
                                    start_slot=i, 
                                    end_slot=chunk_end)
                            continue
                        
                        chunks_to_create.append({
                            "chunk_id": chunk_id,
                            "start_slot": i,
                            "end_slot": chunk_end,
                            "loader_name": loader_name,
                            "status": "pending",
                            "worker_id": "",
                            "created_at": current_time,
                            "updated_at": current_time
                        })
                else:
                    # Daily mode: get target slots and create smart chunks
                    target_slots = self._get_validator_target_slots(start_slot, end_slot)
                    
                    if target_slots:
                        # Create one chunk per target slot to avoid duplicates
                        for target_slot in target_slots:
                            chunk_id = f"{loader_name}_{target_slot}_{target_slot}"
                            
                            # Fast cache lookup for single-slot chunks
                            if (target_slot, target_slot) in existing_chunks:
                                logger.debug("Slot already completed, skipping", 
                                        loader=loader_name, 
                                        slot=target_slot)
                                continue
                            
                            chunks_to_create.append({
                                "chunk_id": chunk_id,
                                "start_slot": target_slot,
                                "end_slot": target_slot,
                                "loader_name": loader_name,
                                "status": "pending",
                                "worker_id": "",
                                "created_at": current_time,
                                "updated_at": current_time
                            })
                            
                        logger.info("Generated validator chunks for daily mode", 
                                total_target_slots=len(target_slots),
                                chunks_created=len([c for c in chunks_to_create if c["loader_name"] == "validators"]))
            
            else:
                # Unknown loader - treat as slot-based by default with cache lookup
                existing_chunks = existing_chunks_cache.get(loader_name, set())
                logger.warning("Unknown loader type, treating as slot-based", loader=loader_name)
                
                for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                    chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                    chunk_id = f"{loader_name}_{i}_{chunk_end}"
                    
                    # Fast cache lookup
                    if (i, chunk_end) in existing_chunks:
                        logger.debug("Range already completed, skipping", 
                                loader=loader_name, 
                                start_slot=i, 
                                end_slot=chunk_end)
                        continue
                    
                    chunks_to_create.append({
                        "chunk_id": chunk_id,
                        "start_slot": i,
                        "end_slot": chunk_end,
                        "loader_name": loader_name,
                        "status": "pending",
                        "worker_id": "",
                        "created_at": current_time,
                        "updated_at": current_time
                    })
        
        # Insert only new chunks in batches to avoid memory limits
        if chunks_to_create:
            batch_size = 10000  # Insert 10K chunks at a time
            total_chunks = len(chunks_to_create)
            
            logger.info("Inserting chunks in batches", 
                    total_chunks=total_chunks, 
                    batch_size=batch_size,
                    batches=math.ceil(total_chunks / batch_size))
            
            for i in range(0, total_chunks, batch_size):
                batch = chunks_to_create[i:i + batch_size]
                try:
                    self.clickhouse.insert_batch("load_state_chunks", batch)
                    logger.info("Inserted chunk batch", 
                            batch_num=i//batch_size + 1,
                            batch_size=len(batch),
                            progress=f"{min(i + batch_size, total_chunks)}/{total_chunks}")
                except Exception as e:
                    logger.error("Failed to insert chunk batch", 
                                batch_num=i//batch_size + 1,
                                batch_size=len(batch),
                                error=str(e))
                    raise
            
            blocks_chunks = len([c for c in chunks_to_create if c["loader_name"] == "blocks"])
            validator_chunks = len([c for c in chunks_to_create if c["loader_name"] == "validators"])
            rewards_chunks = len([c for c in chunks_to_create if c["loader_name"] == "rewards"])
            
            logger.info("Generated new chunks for backfill", 
                    new_chunks=len(chunks_to_create),
                    blocks_chunks=blocks_chunks,
                    validator_chunks=validator_chunks,
                    rewards_chunks=rewards_chunks)
        else:
            logger.info("No new chunks needed - all chunks in range already exist")
        
        # Show summary of chunk status using cache counts + database counts
        for loader_name in enabled_loaders:
            try:
                # Use a simple non-FINAL query for status summary
                status_query = """
                SELECT status, count() as count
                FROM load_state_chunks
                WHERE loader_name = {loader_name:String}
                AND start_slot >= {start_slot:UInt64}
                AND end_slot < {end_slot:UInt64}
                GROUP BY status
                ORDER BY status
                """
                status_result = self.clickhouse.execute(status_query, {
                    "loader_name": loader_name,
                    "start_slot": start_slot,
                    "end_slot": end_slot
                })
                
                status_summary = {row["status"]: row["count"] for row in status_result}
                
                # Add cached completed count for more accurate reporting
                cached_completed = len(existing_chunks_cache.get(loader_name, set()))
                if cached_completed > 0:
                    status_summary["completed_cached"] = cached_completed
                
                logger.info("Chunk status summary", 
                        loader=loader_name,
                        status_counts=status_summary)
                        
            except Exception as e:
                logger.warning("Failed to get chunk status summary", 
                            loader=loader_name, error=str(e))

    def _get_validator_target_slots(self, start_slot: int, end_slot: int) -> list:
        """Get target slots for validators based on mode."""
        try:
            # Get a validators loader instance to calculate target slots
            from src.loaders.validators import ValidatorsLoader
            validators_loader = ValidatorsLoader(self.beacon_api, self.clickhouse)
            return validators_loader.get_target_slots_in_range(start_slot, end_slot)
        except Exception as e:
            logger.error("Error getting validator target slots", error=str(e))
            return []
    
    async def _ensure_foundation_data(self):
        """Ensure genesis and specs data are loaded before processing slots - regardless of enabled loaders."""
        logger.info("Ensuring foundation data is loaded (required for all operations)")
        
        # Always create genesis and specs loaders, regardless of ENABLED_LOADERS
        # These are fundamental requirements for any beacon chain operations
        from src.loaders.genesis import GenesisLoader
        from src.loaders.specs import SpecsLoader
        
        genesis_loader = GenesisLoader(self.beacon_api, self.clickhouse)
        specs_loader = SpecsLoader(self.beacon_api, self.clickhouse)
        
        # Load genesis first (required for timing calculations)
        try:
            success = await genesis_loader.load_single(None)
            if not success:
                raise Exception("Genesis loading failed")
            logger.info("Genesis data ensured")
        except Exception as e:
            logger.error("Critical: Genesis loader failed", error=str(e))
            raise
        
        # Load specs second (depends on genesis for time_helpers)
        try:
            success = await specs_loader.load_single(None)
            if not success:
                raise Exception("Specs loading failed")
            logger.info("Specs data ensured")
        except Exception as e:
            logger.error("Critical: Specs loader failed", error=str(e))
            raise
        
        # Verify foundation data is properly loaded
        try:
            genesis_count = self.clickhouse.execute("SELECT COUNT(*) as count FROM genesis")[0]["count"]
            time_helpers_count = self.clickhouse.execute("SELECT COUNT(*) as count FROM time_helpers")[0]["count"]
            
            if genesis_count == 0:
                raise Exception("Genesis table is empty after loading")
            if time_helpers_count == 0:
                raise Exception("Time helpers table is empty after loading")
            
            # Get timing parameters to verify they're valid
            time_helpers = self.clickhouse.execute("SELECT genesis_time_unix, seconds_per_slot FROM time_helpers ORDER BY genesis_time_unix DESC LIMIT 1")
            if not time_helpers or not time_helpers[0]["genesis_time_unix"]:
                raise Exception("Invalid timing parameters in time_helpers")
            
            logger.info("Foundation data verified and available for all loaders", 
                       genesis_count=genesis_count,
                       time_helpers_count=time_helpers_count,
                       genesis_time_unix=time_helpers[0]["genesis_time_unix"],
                       seconds_per_slot=time_helpers[0]["seconds_per_slot"])
                       
        except Exception as e:
            logger.error("Foundation data verification failed", error=str(e))
            raise
        
        # Initialize cache after loading foundation data
        await self.clickhouse.init_cache()
        logger.info("ClickHouse cache initialized with timing parameters")
    
    @staticmethod
    def _worker_process(worker_id: str):
        """Bulletproof worker process that never leaves stuck chunks."""
        import asyncio
        
        async def worker():
            # Initialize services in worker
            beacon_api = BeaconAPI()
            await beacon_api.start()
            clickhouse = ClickHouse()
            
            logger.info("Worker started", worker=worker_id)
            
            # Track all chunks this worker has claimed
            claimed_chunks = []  # List of (start_slot, end_slot, loader_name)
            
            # Get enabled loaders
            loaders = get_enabled_loaders(
                config.ENABLED_LOADERS,
                beacon_api,
                clickhouse
            )
            
            # Get only slot-based loaders for backfill (exclude genesis, specs)
            slot_based_loader_names = [name for name in config.ENABLED_LOADERS 
                          if name not in ["genesis", "specs"]]
            relevant_loaders = [l for l in loaders if l.name in slot_based_loader_names]
            if not relevant_loaders:
                logger.warning("No relevant loaders found for backfill", worker=worker_id)
                return
            
            logger.info("Worker initialized", worker=worker_id, loaders=[l.name for l in relevant_loaders])
            
            try:
                chunk_count = 0
                consecutive_no_work = 0
                max_consecutive_no_work = 3  # Exit after 3 rounds of no work
                
                while consecutive_no_work < max_consecutive_no_work:
                    work_found_this_round = False
                    
                    for loader in relevant_loaders:
                        logger.debug("Attempting to claim chunk for loader", 
                                   worker=worker_id, 
                                   loader=loader.name)
                        
                        # Claim a chunk for this specific loader
                        chunk = clickhouse.claim_chunk(worker_id, loader.name)
                        if chunk is None:
                            logger.debug("No chunks available for loader", 
                                       worker=worker_id, 
                                       loader=loader.name)
                            continue
                        
                        work_found_this_round = True
                        consecutive_no_work = 0  # Reset counter
                        start_slot, end_slot = chunk
                        chunk_count += 1
                        
                        # Track this claimed chunk
                        claimed_chunks.append((start_slot, end_slot, loader.name))
                        
                        logger.info("Processing chunk", 
                                   worker=worker_id,
                                   loader=loader.name,
                                   chunk_number=chunk_count,
                                   start_slot=start_slot,
                                   end_slot=end_slot)
                        
                        # Process the chunk
                        try:
                            success_count = await loader.load_batch(list(range(start_slot, end_slot + 1)))
                            
                            # Mark as completed
                            clickhouse.update_chunk_status(start_slot, end_slot, loader.name, "completed")
                            
                            logger.info("Chunk completed", 
                                       worker=worker_id,
                                       loader=loader.name,
                                       chunk_number=chunk_count,
                                       start_slot=start_slot,
                                       end_slot=end_slot,
                                       success_count=success_count)
                            
                            claimed_chunks.remove((start_slot, end_slot, loader.name))
                        
                        except Exception as e:
                            logger.error("Chunk processing failed", 
                                        worker=worker_id,
                                        loader=loader.name,
                                        start_slot=start_slot,
                                        end_slot=end_slot,
                                        error=str(e))
                            
                            # Mark as failed
                            clickhouse.update_chunk_status(start_slot, end_slot, loader.name, "failed")
                            claimed_chunks.remove((start_slot, end_slot, loader.name))
                    
                    # If no work was found this round, increment counter
                    if not work_found_this_round:
                        consecutive_no_work += 1
                        logger.debug("No work found this round", 
                                   worker=worker_id, 
                                   consecutive_no_work=consecutive_no_work,
                                   max_consecutive_no_work=max_consecutive_no_work)
                        
                        # Short sleep before trying again
                        import time
                        time.sleep(1)
                
                logger.info("No more chunks to process for any loader", 
                           worker=worker_id, 
                           processed_chunks=chunk_count)
            
            finally:
                # CRITICAL: Clean up any chunks this worker claimed but didn't complete
                if claimed_chunks:
                    logger.warning("Worker finishing with uncompleted claimed chunks - cleaning up", 
                                 worker=worker_id,
                                 uncompleted_chunks=len(claimed_chunks))
                    
                    for start_slot, end_slot, loader_name in claimed_chunks:
                        try:
                            # Reset to pending so another worker can pick it up
                            clickhouse.update_chunk_status(start_slot, end_slot, loader_name, "pending")
                            logger.info("Reset uncompleted chunk to pending", 
                                       worker=worker_id,
                                       loader=loader_name,
                                       start_slot=start_slot,
                                       end_slot=end_slot)
                        except Exception as cleanup_error:
                            logger.error("CRITICAL: Failed to reset chunk to pending during cleanup", 
                                       worker=worker_id,
                                       loader=loader_name,
                                       start_slot=start_slot,
                                       end_slot=end_slot,
                                       error=str(cleanup_error))
                
                await beacon_api.close()
                logger.info("Worker finished", worker=worker_id, total_chunks=chunk_count)
        
        # Run the async worker
        try:
            asyncio.run(worker())
        except Exception as e:
            logger.error("Worker process failed", worker=worker_id, error=str(e))
            import traceback
            logger.error("Worker traceback", worker=worker_id, traceback=traceback.format_exc())