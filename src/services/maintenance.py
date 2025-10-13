import gc
import asyncio
import time
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from src.services.storage_factory import create_storage
from src.services.loader import LoaderService
from src.services.transformer import TransformerService
from src.utils.logger import logger
from src.config import config

class MaintenanceService:
    """Service for maintaining and fixing failed chunks."""
    
    # Mutation queue management constants
    MAX_MUTATIONS_PER_TABLE = 800  # Stay below 1000 limit
    MUTATION_CHECK_INTERVAL = 10   # Check every 10 seconds
    MUTATION_WAIT_TIMEOUT = 600    # Max 10 minutes wait
    
    def __init__(self):
        self.storage = create_storage()
        self.loader_service = None
        self.transformer_service = None
        
        # Table mappings for each loader type
        self.loader_table_mapping = {
            "blocks": {
                "raw_table": "raw_blocks",
                "transformed_tables": [
                    "blocks", "attestations", "deposits", "voluntary_exits",
                    "proposer_slashings", "attester_slashings", "sync_aggregates",
                    "execution_payloads", "transactions", "withdrawals", 
                    "bls_changes", "blob_sidecars", "blob_commitments",
                    "execution_requests"
                ]
            },
            "validators": {
                "raw_table": "raw_validators", 
                "transformed_tables": ["validators"]
            },
            "rewards": {
                "raw_table": "raw_rewards",
                "transformed_tables": ["rewards"]
            }
        }
        
    async def initialize(self):
        """Initialize services."""
        logger.info("Initializing maintenance service")
        
        self.loader_service = LoaderService()
        await self.loader_service.initialize()
        
        self.transformer_service = TransformerService()
        await self.transformer_service.initialize()
        
        logger.info("Maintenance service initialized")
        
    async def fix_failed_chunks(self, start_slot: int, end_slot: int, 
                            loaders: Optional[List[str]] = None, 
                            force: bool = False):
        """Fix failed chunks in the specified range."""
        logger.info("Starting maintenance fix", 
                start_slot=start_slot, 
                end_slot=end_slot,
                loaders=loaders,
                force=force)
        
        chunks_to_fix = await self._identify_chunks_to_fix(
            start_slot, end_slot, loaders, force
        )
        
        if not chunks_to_fix:
            logger.info("No chunks need fixing in the specified range")
            return
        
        logger.info("Found chunks to fix", count=len(chunks_to_fix))
        
        chunks_by_loader = self._group_chunks_by_loader(chunks_to_fix)
        
        fixed_count = 0
        for loader_name, loader_chunks in chunks_by_loader.items():
            logger.info("Processing chunks for loader", 
                    loader=loader_name, 
                    chunks=len(loader_chunks))
            
            # Clean up data in small batches with mutation management
            CLEANUP_BATCH = 10  # Process 10 chunks at a time
            for i in range(0, len(loader_chunks), CLEANUP_BATCH):
                batch = loader_chunks[i:i + CLEANUP_BATCH]
                
                logger.info(f"Cleanup batch {i//CLEANUP_BATCH + 1}/{(len(loader_chunks)-1)//CLEANUP_BATCH + 1}")
                
                # WAIT FOR MUTATION QUEUE TO CLEAR
                await self._wait_for_mutations()
                
                # Clean up this batch
                await self._cleanup_chunks_batch(batch)
            
            # Reset chunk states
            await self._reset_chunk_states(loader_chunks)
            
            # Reload in batches
            RELOAD_BATCH = 20
            for i in range(0, len(loader_chunks), RELOAD_BATCH):
                batch = loader_chunks[i:i + RELOAD_BATCH]
                logger.info(f"Reloading batch {i//RELOAD_BATCH + 1}/{(len(loader_chunks)-1)//RELOAD_BATCH + 1}")
                
                success = await self._reload_chunks(loader_name, batch)
                if success:
                    fixed_count += len(batch)
        
        # Transform in smaller batches
        if fixed_count > 0:
            logger.info("Running transformer for fixed chunks", fixed_chunks=fixed_count)
            await self._retransform_chunks(chunks_to_fix)
        
        logger.info("Maintenance fix completed", 
                total_chunks=len(chunks_to_fix),
                fixed_chunks=fixed_count)
    
    async def _wait_for_mutations(self, max_mutations: int = None):
        """Wait for pending mutations to complete before proceeding."""
        if max_mutations is None:
            max_mutations = self.MAX_MUTATIONS_PER_TABLE
        
        start_time = time.time()
        wait_count = 0
        
        while True:
            try:
                # Check mutation queue across all tables
                query = """
                SELECT 
                    table,
                    COUNT(*) as mutation_count
                FROM system.mutations
                WHERE database = currentDatabase()
                AND is_done = 0
                GROUP BY table
                ORDER BY mutation_count DESC
                LIMIT 1
                """
                
                result = self.storage.execute(query)
                
                if not result:
                    # No pending mutations
                    if wait_count > 0:
                        logger.info("Mutation queue cleared, continuing")
                    return
                
                max_table_mutations = result[0]['mutation_count']
                table_name = result[0]['table']
                
                if max_table_mutations < max_mutations:
                    # Safe to proceed
                    if wait_count > 0:
                        logger.info("Mutation queue below threshold, continuing",
                                   pending=max_table_mutations)
                    return
                
                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > self.MUTATION_WAIT_TIMEOUT:
                    logger.warning("Mutation wait timeout exceeded, proceeding anyway",
                                 max_mutations=max_table_mutations,
                                 table=table_name,
                                 elapsed=elapsed)
                    return
                
                # Log waiting message
                wait_count += 1
                logger.info("Waiting for mutations to complete",
                           pending=max_table_mutations,
                           table=table_name,
                           max_allowed=max_mutations,
                           elapsed=int(elapsed))
                
                # Wait before checking again
                await asyncio.sleep(self.MUTATION_CHECK_INTERVAL)
                
            except Exception as e:
                logger.error("Error checking mutations", error=str(e))
                # If we can't check, just wait a bit and continue
                await asyncio.sleep(self.MUTATION_CHECK_INTERVAL)
                return
    
    async def _cleanup_chunks_batch(self, chunks: List[Dict]):
        """Clean up multiple chunks with optimized multi-range deletions."""
        if not chunks:
            return
        
        logger.info(f"Cleaning up {len(chunks)} chunks")
        
        # Group by loader to batch deletes
        loader_groups = {}
        for chunk in chunks:
            loader_name = chunk['loader_name']
            if loader_name not in loader_groups:
                loader_groups[loader_name] = []
            loader_groups[loader_name].append(chunk)
        
        # Process each loader group
        for loader_name, loader_chunks in loader_groups.items():
            if loader_name not in self.loader_table_mapping:
                logger.warning("Unknown loader type", loader=loader_name)
                continue
            
            mapping = self.loader_table_mapping[loader_name]
            
            # Build slot ranges for batched delete
            slot_conditions = []
            for chunk in loader_chunks:
                slot_conditions.append(
                    f"(slot >= {chunk['start_slot']} AND slot <= {chunk['end_slot']})"
                )
            
            where_clause = " OR ".join(slot_conditions)
            
            # Delete from raw table
            raw_table = mapping['raw_table']
            try:
                query = f"ALTER TABLE {raw_table} DELETE WHERE {where_clause}"
                self.storage.execute(query, {})
                logger.debug(f"Deleted from {raw_table}", chunks=len(loader_chunks))
            except Exception as e:
                logger.debug(f"Failed to delete from {raw_table}: {e}")
            
            # Delete from transformed tables
            for table in mapping['transformed_tables']:
                try:
                    query = f"ALTER TABLE {table} DELETE WHERE {where_clause}"
                    self.storage.execute(query, {})
                    logger.debug(f"Deleted from {table}", chunks=len(loader_chunks))
                except Exception as e:
                    # Table might not exist for certain forks - that's ok
                    logger.debug(f"Failed to delete from {table}: {e}")
            
            # Clear transformer_progress for these ranges
            for chunk in loader_chunks:
                try:
                    self.storage.execute("""
                        INSERT INTO transformer_progress
                        (raw_table_name, start_slot, end_slot, status, 
                        processed_count, failed_count, error_message, processed_at)
                        VALUES ({raw_table:String}, {start:UInt64}, {end:UInt64}, 
                                'failed', 0, 0, 'Reset by maintenance', now())
                    """, {
                        "raw_table": raw_table,
                        "start": chunk['start_slot'],
                        "end": chunk['end_slot']
                    })
                except Exception as e:
                    logger.debug(f"Failed to update transformer_progress: {e}")
        
        logger.info(f"Cleanup completed for {len(chunks)} chunks")
    
    async def _reset_chunk_states(self, chunks: List[Dict]):
        """Reset chunk states to pending."""
        logger.debug("Resetting chunk states", count=len(chunks))
        
        reset_data = []
        current_time = datetime.now()
        
        for chunk in chunks:
            reset_data.append({
                "chunk_id": chunk['chunk_id'],
                "start_slot": chunk['start_slot'],
                "end_slot": chunk['end_slot'],
                "loader_name": chunk['loader_name'],
                "status": "pending",
                "worker_id": "",
                "created_at": current_time,
                "updated_at": current_time
            })
        
        try:
            self.storage.insert_batch("load_state_chunks", reset_data)
            logger.info("Reset chunk states to pending", count=len(reset_data))
        except Exception as e:
            logger.error("Failed to reset chunk states", error=str(e))
            raise
    
    async def _reload_chunks(self, loader_name: str, chunks: List[Dict]) -> bool:
        """Reload specific chunks."""
        logger.info("Reloading chunks", loader=loader_name, count=len(chunks))
        
        loader = self._get_loader_by_name(loader_name)
        if not loader:
            logger.error("Loader not found", loader=loader_name)
            return False
        
        success_count = 0
        failed_chunks = []
        
        for chunk in chunks:
            start_slot = chunk['start_slot']
            end_slot = chunk['end_slot']
            
            try:
                # Mark as claimed
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "claimed"
                )
                
                # Process the chunk
                slots = list(range(start_slot, end_slot + 1))
                result = await loader.load_batch(slots)
                
                if result > 0:
                    # Mark as completed
                    self.storage.update_chunk_status(
                        start_slot, end_slot, loader_name, "completed"
                    )
                    success_count += 1
                else:
                    # Mark as failed
                    self.storage.update_chunk_status(
                        start_slot, end_slot, loader_name, "failed"
                    )
                    failed_chunks.append(chunk)
                    
            except Exception as e:
                logger.error("Failed to reload chunk",
                            loader=loader_name,
                            start=start_slot,
                            end=end_slot,
                            error=str(e))
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "failed"
                )
                failed_chunks.append(chunk)
        
        logger.info("Reload completed",
                loader=loader_name,
                total=len(chunks),
                successful=success_count,
                failed=len(failed_chunks))
        
        return len(failed_chunks) == 0
    
    def _get_loader_by_name(self, loader_name: str):
        """Get loader instance by name."""
        if not self.loader_service or not self.loader_service.loaders:
            logger.error("Loader service not initialized")
            return None
            
        for loader in self.loader_service.loaders:
            if loader.name == loader_name:
                return loader
        
        logger.error("Loader not found in loader service", 
                    loader=loader_name,
                    available=[l.name for l in self.loader_service.loaders])
        return None

    async def _retransform_chunks(self, chunks: List[Dict]):
        """Re-run transformation for fixed chunks."""
        logger.info("Retransforming chunks", count=len(chunks))
        
        success_count = 0
        failed_count = 0
        
        for chunk in chunks:
            try:
                success = await self._retransform_single_chunk(chunk)
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    
            except Exception as e:
                failed_count += 1
                logger.error("Chunk retransform exception",
                        loader=chunk['loader_name'],
                        start_slot=chunk['start_slot'],
                        end_slot=chunk['end_slot'],
                        error=str(e))
        
        logger.info("Retransformation completed",
                total_chunks=len(chunks),
                successful=success_count,
                failed=failed_count)
        
        if failed_count > 0:
            raise Exception(f"Retransformation failed for {failed_count} chunks")

    async def _retransform_single_chunk(self, chunk: Dict) -> bool:
        """Retransform a single chunk."""
        loader_name = chunk['loader_name']
        start_slot = chunk['start_slot']
        end_slot = chunk['end_slot']
        
        if loader_name not in self.loader_table_mapping:
            logger.error("Unknown loader for chunk", loader=loader_name)
            return False
        
        raw_table = self.loader_table_mapping[loader_name]["raw_table"]
        
        try:
            # Clear any existing transformer progress
            clear_progress_query = """
            INSERT INTO transformer_progress
            (raw_table_name, start_slot, end_slot, status, 
             processed_count, failed_count, error_message, processed_at)
            VALUES ({raw_table:String}, {start:UInt64}, {end:UInt64}, 
                    'processing', 0, 0, 'Retransforming fixed chunk', now())
            """
            
            self.storage.execute(clear_progress_query, {
                "raw_table": raw_table,
                "start": start_slot,
                "end": end_slot
            })
            
            # Process using transformer
            chunk_data = {
                "chunk_id": chunk['chunk_id'],
                "start_slot": start_slot,
                "end_slot": end_slot,
                "loader_name": loader_name
            }
            
            success = await self.transformer_service._process_chunk_simple(loader_name, chunk_data)
            
            if success:
                logger.debug("Chunk retransformed successfully",
                           loader=loader_name,
                           start_slot=start_slot,
                           end_slot=end_slot)
            else:
                logger.error("Chunk retransform failed",
                           loader=loader_name,
                           start_slot=start_slot,
                           end_slot=end_slot)
            
            return success
            
        except Exception as e:
            # Mark as failed
            failed_progress_query = """
            INSERT INTO transformer_progress
            (raw_table_name, start_slot, end_slot, status, 
             processed_count, failed_count, error_message, processed_at)
            VALUES ({raw_table:String}, {start:UInt64}, {end:UInt64}, 
                    'failed', 0, 1, {error:String}, now())
            """
            
            try:
                self.storage.execute(failed_progress_query, {
                    "raw_table": raw_table,
                    "start": start_slot,
                    "end": end_slot,
                    "error": str(e)[:500]
                })
            except Exception as db_error:
                logger.error("Failed to record transform failure", error=str(db_error))
            
            logger.error("Chunk retransform exception",
                       loader=loader_name,
                       start_slot=start_slot,
                       end_slot=end_slot,
                       error=str(e))
            return False
    
    def _group_chunks_by_loader(self, chunks: List[Dict]) -> Dict[str, List[Dict]]:
        """Group chunks by loader type."""
        groups = {}
        for chunk in chunks:
            loader = chunk['loader_name']
            if loader not in groups:
                groups[loader] = []
            groups[loader].append(chunk)
        return groups
    
    async def _identify_chunks_to_fix(self, start_slot: int, end_slot: int,
                                      loaders: Optional[List[str]] = None,
                                      force: bool = False) -> List[Dict]:
        """Identify chunks that need fixing."""
        
        conditions = [
            "start_slot >= {start:UInt64}",
            "end_slot <= {end:UInt64}"
        ]
        params = {"start": start_slot, "end": end_slot}
        
        # Status filter
        if force:
            conditions.append("status IN ('completed', 'failed')")
        else:
            conditions.append("status = 'failed'")
        
        # Loader filter
        if loaders:
            valid_loaders = [l for l in loaders if l in config.ENABLED_LOADERS]
            if valid_loaders:
                loader_placeholders = []
                for i, loader in enumerate(valid_loaders):
                    placeholder = f"loader_{i}"
                    loader_placeholders.append(f"{{{placeholder}:String}}")
                    params[placeholder] = loader
                
                conditions.append(f"loader_name IN ({','.join(loader_placeholders)})")
        else:
            if config.ENABLED_LOADERS:
                loader_placeholders = []
                for i, loader in enumerate(config.ENABLED_LOADERS):
                    if loader not in ["genesis", "specs"]:
                        placeholder = f"loader_{i}"
                        loader_placeholders.append(f"{{{placeholder}:String}}")
                        params[placeholder] = loader
                
                if loader_placeholders:
                    conditions.append(f"loader_name IN ({','.join(loader_placeholders)})")
        
        query = f"""
        SELECT DISTINCT
            chunk_id,
            loader_name,
            start_slot,
            end_slot,
            status,
            worker_id,
            updated_at
        FROM load_state_chunks FINAL
        WHERE {' AND '.join(conditions)}
        ORDER BY loader_name, start_slot
        """
        
        try:
            chunks = self.storage.execute(query, params)
            logger.debug("Identified chunks to fix", 
                        count=len(chunks),
                        force=force,
                        loaders=loaders)
            return chunks
        except Exception as e:
            logger.error("Failed to identify chunks to fix", error=str(e))
            return []
    
    async def preview_fix(self, start_slot: int, end_slot: int,
                          loaders: Optional[List[str]] = None,
                          force: bool = False) -> List[Dict]:
        """Preview what would be fixed (dry run)."""
        chunks_to_fix = await self._identify_chunks_to_fix(
            start_slot, end_slot, loaders, force
        )
        
        return [{
            'chunk_id': chunk['chunk_id'],
            'loader_name': chunk['loader_name'],
            'start_slot': chunk['start_slot'],
            'end_slot': chunk['end_slot'],
            'status': chunk['status'],
            'actions': ['Delete all data', 'Reload from API', 'Transform']
        } for chunk in chunks_to_fix]
    
    async def check_integrity(self, start_slot: int, end_slot: int, 
                              detailed: bool = False) -> Dict[str, List[Dict]]:
        """Check data integrity."""
        logger.info("Checking data integrity",
                   start_slot=start_slot,
                   end_slot=end_slot)
        
        issues = {
            "failed_chunks": [],
            "untransformed_data": []
        }
        
        # Check failed chunks
        failed_query = """
        SELECT loader_name, start_slot, end_slot, status, updated_at
        FROM load_state_chunks FINAL
        WHERE start_slot >= {start:UInt64}
        AND end_slot <= {end:UInt64}
        AND status = 'failed'
        ORDER BY loader_name, start_slot
        """
        
        try:
            failed_result = self.storage.execute(failed_query, {
                "start": start_slot,
                "end": end_slot
            })
            
            for row in failed_result:
                issues["failed_chunks"].append({
                    "loader": row["loader_name"],
                    "issue": "failed_chunk",
                    "start_slot": row["start_slot"],
                    "end_slot": row["end_slot"],
                    "description": f"Failed chunk: {row['start_slot']}-{row['end_slot']}"
                })
                
        except Exception as e:
            logger.error("Failed to check for failed chunks", error=str(e))
        
        # Check untransformed data
        untransformed_query = """
        SELECT 
            lsc.loader_name,
            lsc.start_slot,
            lsc.end_slot
        FROM load_state_chunks lsc FINAL
        LEFT JOIN transformer_progress tp FINAL ON (
            tp.raw_table_name = CONCAT('raw_', lsc.loader_name)
            AND tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
            AND tp.status = 'completed'
        )
        WHERE lsc.start_slot >= {start:UInt64}
        AND lsc.end_slot <= {end:UInt64}
        AND lsc.status = 'completed'
        AND tp.start_slot IS NULL
        ORDER BY lsc.loader_name, lsc.start_slot
        LIMIT 100
        SETTINGS join_use_nulls = 1
        """
        
        try:
            untransformed_result = self.storage.execute(untransformed_query, {
                "start": start_slot,
                "end": end_slot
            })
            
            for row in untransformed_result:
                issues["untransformed_data"].append({
                    "loader": row["loader_name"],
                    "issue": "untransformed_chunk",
                    "start_slot": row["start_slot"],
                    "end_slot": row["end_slot"],
                    "description": f"Completed but not transformed: {row['start_slot']}-{row['end_slot']}"
                })
                
        except Exception as e:
            logger.error("Failed to check untransformed data", error=str(e))
        
        issues = {k: v for k, v in issues.items() if v}
        
        if issues:
            total_issues = sum(len(v) for v in issues.values())
            logger.warning("Integrity check found issues",
                         total_categories=len(issues),
                         total_issues=total_issues)
        else:
            logger.info("Integrity check passed - no issues found")
        
        return issues
    
    async def reset_chunks(self, start_slot: int, end_slot: int,
                           loaders: Optional[List[str]] = None,
                           current_status: str = 'failed') -> int:
        """Reset chunks with specific status back to pending."""
        
        conditions = [
            "start_slot >= {start:UInt64}",
            "end_slot <= {end:UInt64}",
            f"status = '{current_status}'"
        ]
        params = {"start": start_slot, "end": end_slot}
        
        if loaders:
            valid_loaders = [l for l in loaders if l in config.ENABLED_LOADERS and l not in ["genesis", "specs"]]
            if valid_loaders:
                loader_placeholders = []
                for i, loader in enumerate(valid_loaders):
                    placeholder = f"loader_{i}"
                    loader_placeholders.append(f"{{{placeholder}:String}}")
                    params[placeholder] = loader
                
                conditions.append(f"loader_name IN ({','.join(loader_placeholders)})")
        
        find_query = f"""
        SELECT DISTINCT chunk_id, loader_name, start_slot, end_slot
        FROM load_state_chunks FINAL
        WHERE {' AND '.join(conditions)}
        """
        
        try:
            chunks_to_reset = self.storage.execute(find_query, params)
            
            if not chunks_to_reset:
                logger.info("No chunks found to reset", status=current_status)
                return 0
            
            await self._reset_chunk_states(chunks_to_reset)
            
            logger.info("Reset chunks to pending",
                       count=len(chunks_to_reset),
                       from_status=current_status)
            
            return len(chunks_to_reset)
            
        except Exception as e:
            logger.error("Failed to reset chunks", error=str(e))
            return 0