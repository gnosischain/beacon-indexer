import asyncio
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
        
        # Get last processed slot
        last_slot = self.clickhouse.get_last_processed_slot("loader_realtime")
        
        while True:
            try:
                # Get current head slot
                head_slot = await self.beacon_api.get_head_slot()
                if head_slot is None:
                    logger.warning("Could not get head slot, retrying...")
                    await asyncio.sleep(12)  # Wait one slot
                    continue
                
                # Process new slots
                for slot in range(last_slot + 1, head_slot + 1):
                    await self._process_slot(slot)
                    last_slot = slot
                
                # Update progress
                if head_slot > last_slot:
                    self.clickhouse.update_last_processed_slot("loader_realtime", head_slot)
                
                # Wait before next check
                await asyncio.sleep(6)  # Half slot time
                
            except Exception as e:
                logger.error("Error in realtime loader", error=str(e))
                await asyncio.sleep(12)
    
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
        """Generate chunks with unique IDs, preserving existing completed chunks. Range is [start_slot, end_slot)."""
        chunks_to_create = []
        current_time = datetime.now()
        
        for loader_name in enabled_loaders:
            logger.info("Generating chunks for loader", loader=loader_name)
            
            if loader_name == "blocks":
                # Blocks: process all slots (FIXED: end_slot is now exclusive)
                for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                    chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                    chunk_id = f"{loader_name}_{i}_{chunk_end}"
                    
                    # Check if this range already has a completed chunk (any chunk_id)
                    existing_query = """
                    SELECT status FROM load_state_chunks FINAL
                    WHERE loader_name = {loader_name:String}
                      AND start_slot = {start_slot:UInt64} 
                      AND end_slot = {end_slot:UInt64}
                      AND status = 'completed'
                    """
                    existing = self.clickhouse.execute(existing_query, {
                        "loader_name": loader_name,
                        "start_slot": i,
                        "end_slot": chunk_end
                    })
                    
                    if existing:
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
                # Validators: only create chunks for target slots based on mode
                if config.VALIDATOR_MODE == "all_slots":
                    # Process all slots (FIXED: end_slot is now exclusive)
                    for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                        chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                        chunk_id = f"{loader_name}_{i}_{chunk_end}"
                        
                        # Check if this range already has a completed chunk
                        existing_query = """
                        SELECT status FROM load_state_chunks FINAL
                        WHERE loader_name = {loader_name:String}
                          AND start_slot = {start_slot:UInt64} 
                          AND end_slot = {end_slot:UInt64}
                          AND status = 'completed'
                        """
                        existing = self.clickhouse.execute(existing_query, {
                            "loader_name": loader_name,
                            "start_slot": i,
                            "end_slot": chunk_end
                        })
                        
                        if existing:
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
                            
                            # Check if this slot already has a completed chunk
                            existing_query = """
                            SELECT status FROM load_state_chunks FINAL
                            WHERE loader_name = {loader_name:String}
                              AND start_slot = {start_slot:UInt64} 
                              AND end_slot = {end_slot:UInt64}
                              AND status = 'completed'
                            """
                            existing = self.clickhouse.execute(existing_query, {
                                "loader_name": loader_name,
                                "start_slot": target_slot,
                                "end_slot": target_slot
                            })
                            
                            if existing:
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
        
        # Insert only new chunks
        if chunks_to_create:
            self.clickhouse.insert_batch("load_state_chunks", chunks_to_create)
            blocks_chunks = len([c for c in chunks_to_create if c["loader_name"] == "blocks"])
            validator_chunks = len([c for c in chunks_to_create if c["loader_name"] == "validators"])
            
            logger.info("Generated new chunks for backfill", 
                       new_chunks=len(chunks_to_create),
                       blocks_chunks=blocks_chunks,
                       validator_chunks=validator_chunks)
        else:
            logger.info("No new chunks needed - all chunks in range already exist")
        
        # Show summary of chunk status
        for loader_name in enabled_loaders:
            status_query = """
            SELECT status, count() as count
            FROM load_state_chunks FINAL
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
            logger.info("Chunk status summary", 
                       loader=loader_name,
                       status_counts=status_summary)

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
            relevant_loaders = [l for l in loaders if l.name in ["blocks", "validators"]]
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