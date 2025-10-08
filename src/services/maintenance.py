import gc
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from src.services.storage_factory import create_storage
from src.services.loader import LoaderService
from src.services.transformer import TransformerService
from src.utils.logger import logger
from src.config import config

class MaintenanceService:
    """Service for maintaining and fixing failed chunks."""
    
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
            
            # Clean up data in parallel batches
            CLEANUP_BATCH = 50
            for i in range(0, len(loader_chunks), CLEANUP_BATCH):
                batch = loader_chunks[i:i + CLEANUP_BATCH]
                logger.info(f"Cleaning batch {i//CLEANUP_BATCH + 1}/{(len(loader_chunks)-1)//CLEANUP_BATCH + 1}")
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
    
    async def preview_fix(self, start_slot: int, end_slot: int,
                          loaders: Optional[List[str]] = None,
                          force: bool = False) -> List[Dict]:
        """Preview what chunks would be fixed (dry run)."""
        chunks_to_fix = await self._identify_chunks_to_fix(
            start_slot, end_slot, loaders, force
        )
        
        return [{
            'chunk_id': chunk['chunk_id'],
            'loader_name': chunk['loader_name'],
            'start_slot': chunk['start_slot'],
            'end_slot': chunk['end_slot'],
            'status': chunk['status'],
            'actions': self._get_actions_for_chunk(chunk)
        } for chunk in chunks_to_fix]
    
    def _get_actions_for_chunk(self, chunk: Dict) -> List[str]:
        """Get list of actions that would be performed on a chunk."""
        actions = []
        
        loader_name = chunk['loader_name']
        if loader_name in self.loader_table_mapping:
            mapping = self.loader_table_mapping[loader_name]
            
            actions.append(f"Delete raw data from {mapping['raw_table']}")
            
            for table in mapping['transformed_tables']:
                actions.append(f"Delete transformed data from {table}")
            
            actions.append("Reset chunk status to pending")
            actions.append("Reload raw data")
            actions.append("Transform data")
        
        return actions
    
    async def _identify_chunks_to_fix(self, start_slot: int, end_slot: int,
                                      loaders: Optional[List[str]] = None,
                                      force: bool = False) -> List[Dict]:
        """Identify chunks that need fixing."""
        
        # Build query conditions
        conditions = [
            "start_slot >= {start:UInt64}",
            "end_slot <= {end:UInt64}"
        ]
        params = {"start": start_slot, "end": end_slot}
        
        # Status filter
        if force:
            # Force mode: get all completed chunks (assuming they might have issues)
            conditions.append("status IN ('completed', 'failed')")
        else:
            # Normal mode: only failed chunks
            conditions.append("status = 'failed'")
        
        # Loader filter
        if loaders:
            # Filter to only enabled loaders that were specified
            valid_loaders = [l for l in loaders if l in config.ENABLED_LOADERS]
            if valid_loaders:
                loader_placeholders = []
                for i, loader in enumerate(valid_loaders):
                    placeholder = f"loader_{i}"
                    loader_placeholders.append(f"{{{placeholder}:String}}")
                    params[placeholder] = loader
                
                conditions.append(f"loader_name IN ({','.join(loader_placeholders)})")
            else:
                logger.warning("No valid loaders specified", loaders=loaders)
                return []
        else:
            # No specific loaders - filter to only enabled loaders
            if config.ENABLED_LOADERS:
                loader_placeholders = []
                for i, loader in enumerate(config.ENABLED_LOADERS):
                    if loader not in ["genesis", "specs"]:  # Skip foundation loaders
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
    
    def _group_chunks_by_loader(self, chunks: List[Dict]) -> Dict[str, List[Dict]]:
        """Group chunks by loader type."""
        groups = {}
        for chunk in chunks:
            loader = chunk['loader_name']
            if loader not in groups:
                groups[loader] = []
            groups[loader].append(chunk)
        return groups
    
    async def _cleanup_chunk_data(self, chunk: Dict):
        """Clean up all data for a chunk - SEQUENTIAL VERSION."""
        loader_name = chunk['loader_name']
        start_slot = chunk['start_slot']
        end_slot = chunk['end_slot']
        
        if loader_name not in self.loader_table_mapping:
            logger.warning("Unknown loader type", loader=loader_name)
            return
        
        mapping = self.loader_table_mapping[loader_name]
        
        # Execute DELETEs sequentially - ClickHouse doesn't allow concurrent queries on same connection
        raw_table = mapping['raw_table']
        try:
            self.storage.execute(f"ALTER TABLE {raw_table} DELETE WHERE slot >= {{start:UInt64}} AND slot <= {{end:UInt64}}", {
                "start": start_slot,
                "end": end_slot
            })
        except Exception as e:
            logger.debug(f"Failed to delete from {raw_table}: {e}")
        
        # Delete from transformed tables
        for table in mapping['transformed_tables']:
            try:
                self.storage.execute(f"ALTER TABLE {table} DELETE WHERE slot >= {{start:UInt64}} AND slot <= {{end:UInt64}}", {
                    "start": start_slot,
                    "end": end_slot
                })
            except Exception as e:
                # Some tables might not exist for certain forks - that's ok
                pass
        
        # Clear transformer_progress
        try:
            self.storage.execute("""
                INSERT INTO transformer_progress
                (raw_table_name, start_slot, end_slot, status, 
                processed_count, failed_count, error_message, processed_at)
                VALUES ({raw_table:String}, {start:UInt64}, {end:UInt64}, 
                        'failed', 0, 0, 'Reset by maintenance', now())
            """, {
                "raw_table": raw_table,
                "start": start_slot,
                "end": end_slot
            })
        except Exception as e:
            logger.debug(f"Failed to update transformer_progress: {e}")


    async def _execute_delete_async(self, query: str):
        """Execute DELETE asynchronously."""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.storage.execute, query, {}
            )
        except Exception as e:
            # Some tables might not exist - that's ok
            pass
    
    async def _cleanup_chunks_batch(self, chunks: List[Dict]):
        """Clean up multiple chunks sequentially."""
        logger.info(f"Cleaning up {len(chunks)} chunks")
        
        for i, chunk in enumerate(chunks):
            if i % 10 == 0:
                logger.debug(f"Cleanup progress: {i}/{len(chunks)}")
            await self._cleanup_chunk_data(chunk)
        
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
        """Reload specific chunks without subprocess overhead."""
        logger.info("Reloading chunks directly", loader=loader_name, count=len(chunks))
        
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
                    logger.info("Chunk reloaded", 
                            loader=loader_name,
                            start=start_slot, 
                            end=end_slot)
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
    
    async def _process_single_chunk_reload(self, loader, chunk: Dict) -> bool:
        """Process a single chunk with explicit cleanup."""
        start_slot = chunk['start_slot']
        end_slot = chunk['end_slot']
        loader_name = chunk['loader_name']
        
        try:
            # Mark chunk as claimed
            self.storage.update_chunk_status(
                start_slot, end_slot, loader_name, "claimed"
            )
            
            # Generate slots to process
            if loader_name == "validators" and hasattr(loader, 'get_target_slots_in_range'):
                target_slots = loader.get_target_slots_in_range(start_slot, end_slot + 1)
                slots_to_process = target_slots
            else:
                slots_to_process = list(range(start_slot, end_slot + 1))
            
            if not slots_to_process:
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "completed"
                )
                return True
            
            # Load the data
            batch_success_count = await loader.load_batch(slots_to_process)
            
            if batch_success_count > 0:
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "completed"
                )
                
                logger.debug("Reloaded chunk successfully",
                        loader=loader_name,
                        start_slot=start_slot,
                        end_slot=end_slot,
                        success_count=batch_success_count,
                        total_slots=len(slots_to_process))
                return True
            else:
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "failed"
                )
                return False
        
        except Exception as e:
            # Mark as failed
            try:
                self.storage.update_chunk_status(
                    start_slot, end_slot, loader_name, "failed"
                )
            except:
                pass  # Don't fail on status update failure
            
            logger.error("Failed to reload chunk",
                    loader=loader_name,
                    start_slot=start_slot,
                    end_slot=end_slot,
                    error=str(e))
            return False
    
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
        """Re-run transformation for the specific chunks that were fixed."""
        logger.info("Retransforming specific chunks", count=len(chunks))
        
        # Process chunks individually using existing logic
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
        """Retransform a single specific chunk."""
        loader_name = chunk['loader_name']
        start_slot = chunk['start_slot']
        end_slot = chunk['end_slot']
        
        if loader_name not in self.loader_table_mapping:
            logger.error("Unknown loader for chunk", loader=loader_name)
            return False
        
        raw_table = self.loader_table_mapping[loader_name]["raw_table"]
        
        # Create a targeted transformer service instance
        # We'll use the existing transformer but process only this specific chunk
        try:
            # Clear any existing transformer progress for this range
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
            
            # Process the chunk using the transformer's chunk processing method
            chunk_data = {
                "chunk_id": chunk['chunk_id'],
                "start_slot": start_slot,
                "end_slot": end_slot,
                "loader_name": loader_name
            }
            
            # Use transformer's existing chunk processing logic
            success = await self.transformer_service._process_chunk_simple(loader_name, chunk_data)
            
            if success:
                logger.info("Single chunk retransformed successfully",
                           loader=loader_name,
                           start_slot=start_slot,
                           end_slot=end_slot)
            else:
                logger.error("Single chunk retransform failed",
                           loader=loader_name,
                           start_slot=start_slot,
                           end_slot=end_slot)
            
            return success
            
        except Exception as e:
            # Mark as failed in transformer progress
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
                    "error": str(e)[:500]  # Limit error message length
                })
            except Exception as db_error:
                logger.error("Failed to record transform failure", error=str(db_error))
            
            logger.error("Single chunk retransform exception",
                       loader=loader_name,
                       start_slot=start_slot,
                       end_slot=end_slot,
                       error=str(e))
            return False
        
    async def check_integrity(self, start_slot: int, end_slot: int, 
                              detailed: bool = False) -> Dict[str, List[Dict]]:
        """Simple integrity check using basic queries."""
        logger.info("Checking data integrity",
                   start_slot=start_slot,
                   end_slot=end_slot)
        
        issues = {
            "failed_chunks": [],
            "missing_chunks": [],
            "untransformed_data": []
        }
        
        # Check 1: Failed chunks
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
        
        # Check 2: Untransformed data
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
        
        # Check 3: Missing chunks (simple check - look for big gaps)
        if detailed:
            try:
                for loader_name in config.ENABLED_LOADERS:
                    if loader_name in ["genesis", "specs"]:
                        continue
                    
                    # Simple gap detection - find ranges with no chunks
                    gap_query = f"""
                    SELECT 
                        '{loader_name}' as loader_name,
                        {start_slot} + (number * {config.CHUNK_SIZE}) as missing_start,
                        {start_slot} + ((number + 1) * {config.CHUNK_SIZE}) - 1 as missing_end
                    FROM numbers({(end_slot - start_slot) // config.CHUNK_SIZE})
                    WHERE NOT EXISTS (
                        SELECT 1 FROM load_state_chunks FINAL lsc
                        WHERE lsc.loader_name = '{loader_name}'
                        AND lsc.start_slot = {start_slot} + (number * {config.CHUNK_SIZE})
                    )
                    LIMIT 50
                    """
                    
                    missing_result = self.storage.execute(gap_query)
                    
                    for row in missing_result:
                        issues["missing_chunks"].append({
                            "loader": row["loader_name"],
                            "issue": "missing_chunk",
                            "start_slot": row["missing_start"],
                            "end_slot": row["missing_end"],
                            "description": f"Missing chunk: {row['missing_start']}-{row['missing_end']}"
                        })
                        
            except Exception as e:
                logger.error("Failed to check missing chunks", error=str(e))
        
        # Filter out empty categories
        issues = {k: v for k, v in issues.items() if v}
        
        if issues:
            total_issues = sum(len(v) for v in issues.values())
            logger.warning("Integrity check found issues",
                         total_categories=len(issues),
                         total_issues=total_issues)
        else:
            logger.info("Integrity check passed - no issues found")
        
        return issues
    
    async def _check_failed_chunks(self, start_slot: int, end_slot: int) -> List[Dict]:
        """Check for failed chunks in range."""
        query = """
        SELECT loader_name, start_slot, end_slot, status, updated_at
        FROM load_state_chunks FINAL
        WHERE start_slot >= {start:UInt64}
        AND end_slot <= {end:UInt64}
        AND status = 'failed'
        ORDER BY loader_name, start_slot
        """
        
        try:
            result = self.storage.execute(query, {
                "start": start_slot,
                "end": end_slot
            })
            
            return [{
                "loader": row["loader_name"],
                "issue": "failed_chunk",
                "start_slot": row["start_slot"],
                "end_slot": row["end_slot"],
                "description": f"Chunk {row['start_slot']}-{row['end_slot']} failed",
                "updated_at": row["updated_at"]
            } for row in result]
            
        except Exception as e:
            logger.error("Failed to check for failed chunks", error=str(e))
            return []
    
    async def _check_missing_chunks(self, start_slot: int, end_slot: int, 
                                    loaders: List[str]) -> List[Dict]:
        """Check for missing chunks (slot ranges that should have chunks)."""
        missing = []
        
        for loader_name in loaders:
            try:
                # Calculate expected chunks based on CHUNK_SIZE
                expected_chunks = []
                for i in range(start_slot, end_slot, config.CHUNK_SIZE):
                    chunk_end = min(i + config.CHUNK_SIZE - 1, end_slot - 1)
                    expected_chunks.append((i, chunk_end))
                
                # Get existing chunks
                existing_query = """
                SELECT DISTINCT start_slot, end_slot
                FROM load_state_chunks FINAL
                WHERE loader_name = {loader:String}
                AND start_slot >= {start:UInt64}
                AND end_slot < {end:UInt64}
                """
                
                existing_result = self.storage.execute(existing_query, {
                    "loader": loader_name,
                    "start": start_slot,
                    "end": end_slot
                })
                
                existing_ranges = {(row["start_slot"], row["end_slot"]) for row in existing_result}
                
                # Find missing chunks
                for start, end in expected_chunks:
                    if (start, end) not in existing_ranges:
                        missing.append({
                            "loader": loader_name,
                            "issue": "missing_chunk",
                            "start_slot": start,
                            "end_slot": end,
                            "description": f"Missing chunk for slots {start}-{end}",
                            "count": 1
                        })
                        
            except Exception as e:
                logger.error("Failed to check missing chunks for loader",
                           loader=loader_name, error=str(e))
        
        return missing
    
    async def _check_untransformed_data(self, start_slot: int, end_slot: int) -> List[Dict]:
        """Check for data that has been loaded but not transformed."""
        query = """
        SELECT 
            lsc.loader_name,
            COUNT(*) as untransformed_count
        FROM load_state_chunks lsc FINAL
        LEFT JOIN (
            SELECT DISTINCT 
                raw_table_name, 
                start_slot, 
                end_slot
            FROM transformer_progress FINAL
            WHERE status = 'completed'
            AND start_slot >= {start:UInt64}
            AND end_slot <= {end:UInt64}
        ) tp ON (
            tp.raw_table_name = CONCAT('raw_', lsc.loader_name)
            AND tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
        )
        WHERE lsc.start_slot >= {start:UInt64}
        AND lsc.end_slot <= {end:UInt64}
        AND lsc.status = 'completed'
        AND tp.start_slot IS NULL
        GROUP BY lsc.loader_name
        SETTINGS join_use_nulls = 1
        """
        
        try:
            result = self.storage.execute(query, {
                "start": start_slot,
                "end": end_slot
            })
            
            return [{
                "loader": row["loader_name"],
                "issue": "untransformed_chunks",
                "count": row["untransformed_count"],
                "description": f"Found {row['untransformed_count']} completed chunks that haven't been transformed"
            } for row in result]
            
        except Exception as e:
            logger.error("Failed to check untransformed data", error=str(e))
            return []
    
    async def _check_data_gaps(self, start_slot: int, end_slot: int, 
                               loaders: List[str]) -> List[Dict]:
        """Check for gaps in raw data tables."""
        gaps = []
        
        for loader_name in loaders:
            if loader_name not in self.loader_table_mapping:
                continue
                
            raw_table = self.loader_table_mapping[loader_name]["raw_table"]
            
            try:
                # Find gaps in slot sequence
                gap_query = f"""
                WITH slots AS (
                    SELECT DISTINCT slot
                    FROM {raw_table}
                    WHERE slot >= {{start:UInt64}}
                    AND slot <= {{end:UInt64}}
                    ORDER BY slot
                ),
                gaps AS (
                    SELECT 
                        slot,
                        slot - ROW_NUMBER() OVER (ORDER BY slot) as grp
                    FROM slots
                ),
                ranges AS (
                    SELECT 
                        MIN(slot) as range_start,
                        MAX(slot) as range_end,
                        COUNT(*) as slot_count
                    FROM gaps
                    GROUP BY grp
                    ORDER BY range_start
                )
                SELECT 
                    range_start,
                    range_end,
                    CASE 
                        WHEN LAG(range_end) OVER (ORDER BY range_start) IS NULL 
                        THEN NULL
                        ELSE LAG(range_end) OVER (ORDER BY range_start) + 1
                    END as gap_start,
                    CASE 
                        WHEN LAG(range_end) OVER (ORDER BY range_start) IS NULL 
                        THEN NULL
                        ELSE range_start - 1
                    END as gap_end
                FROM ranges
                """
                
                result = self.storage.execute(gap_query, {
                    "start": start_slot,
                    "end": end_slot
                })
                
                for row in result:
                    if row["gap_start"] is not None and row["gap_end"] is not None:
                        gap_size = row["gap_end"] - row["gap_start"] + 1
                        if gap_size > 0:
                            gaps.append({
                                "loader": loader_name,
                                "issue": "data_gap",
                                "start_slot": row["gap_start"],
                                "end_slot": row["gap_end"],
                                "count": gap_size,
                                "description": f"Missing {gap_size} slots from {row['gap_start']} to {row['gap_end']}"
                            })
                            
            except Exception as e:
                logger.error("Failed to check data gaps for loader",
                           loader=loader_name, error=str(e))
        
        return gaps
    
    async def reset_chunks(self, start_slot: int, end_slot: int,
                           loaders: Optional[List[str]] = None,
                           current_status: str = 'failed') -> int:
        """Reset chunks with specific status back to pending."""
        
        # Build query conditions similar to _identify_chunks_to_fix
        conditions = [
            "start_slot >= {start:UInt64}",
            "end_slot <= {end:UInt64}",
            f"status = '{current_status}'"
        ]
        params = {"start": start_slot, "end": end_slot}
        
        # Loader filter
        if loaders:
            valid_loaders = [l for l in loaders if l in config.ENABLED_LOADERS and l not in ["genesis", "specs"]]
            if valid_loaders:
                loader_placeholders = []
                for i, loader in enumerate(valid_loaders):
                    placeholder = f"loader_{i}"
                    loader_placeholders.append(f"{{{placeholder}:String}}")
                    params[placeholder] = loader
                
                conditions.append(f"loader_name IN ({','.join(loader_placeholders)})")
        
        # Get chunks to reset
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
            
            # Reset them to pending
            reset_data = []
            current_time = datetime.now()
            
            for chunk in chunks_to_reset:
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
            
            self.storage.insert_batch("load_state_chunks", reset_data)
            
            logger.info("Reset chunks to pending",
                       count=len(reset_data),
                       from_status=current_status)
            
            return len(reset_data)
            
        except Exception as e:
            logger.error("Failed to reset chunks", error=str(e))
            return 0