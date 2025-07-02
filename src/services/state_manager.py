"""
Enhanced state manager with proper dataset filtering and cleanup.
"""
import os
import time
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta

from src.utils.logger import logger
from src.services.clickhouse_service import ClickHouseService
from src.core.datasets import DatasetRegistry, Dataset
from src.core.state import StateStatus


class StateManager:
    """Enhanced state management for beacon chain indexing."""
    
    def __init__(self, db: ClickHouseService):
        self.db = db
        self.dataset_registry = DatasetRegistry()
        
        # Configuration
        self.range_size = int(os.getenv("STATE_RANGE_SIZE", "10000"))
        self.max_retries = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
        self.stale_timeout = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "30"))
        
        # Caching
        self.enable_caching = os.getenv("ENABLE_STATE_CACHING", "true").lower() == "true"
        self.cache_ttl = int(os.getenv("STATE_CACHE_TTL", "300"))
        self._cache = {}
        self._cache_timestamps = {}
        
    def bulk_create_ranges(self, scrapers: List[Any], start_slot: int, end_slot: int) -> int:
        """Create ranges for enabled scrapers with proper filtering."""
        try:
            total_created = 0
            
            # Get scraper IDs from scraper instances
            scraper_ids = [s.scraper_id for s in scrapers]
            logger.info(f"Creating ranges for scrapers: {scraper_ids}")
            
            # Get all datasets for the enabled scrapers
            datasets_to_process = set()
            
            for scraper in scrapers:
                # Get datasets for this scraper from the registry
                scraper_datasets = self.dataset_registry.get_datasets_for_scraper(scraper.scraper_id)
                for dataset in scraper_datasets:
                    if dataset.is_continuous:
                        datasets_to_process.add(dataset.name)
                        logger.info(f"Found dataset '{dataset.name}' for scraper '{scraper.scraper_id}'")
                    elif dataset.is_sparse:
                        logger.info(f"Dataset '{dataset.name}' is sparse, will handle separately")
            
            # Clean up any existing ranges for datasets NOT in our enabled list
            self._cleanup_disabled_datasets(datasets_to_process, start_slot, end_slot)
            
            # Create ranges for each enabled dataset
            for dataset_name in datasets_to_process:
                logger.info(f"Creating ranges for dataset: {dataset_name}")
                created = self.bulk_create_ranges_for_dataset(
                    "historical",
                    dataset_name,
                    start_slot,
                    end_slot
                )
                total_created += created
                logger.info(f"Created {created} ranges for dataset {dataset_name}")
            
            # Handle sparse datasets (like validators)
            for scraper in scrapers:
                if hasattr(scraper, 'get_target_slots_in_range'):
                    # This is a validator scraper
                    datasets = self.dataset_registry.get_datasets_for_scraper(scraper.scraper_id)
                    for dataset in datasets:
                        if dataset.is_sparse:
                            created = self._create_sparse_ranges(
                                "historical",
                                dataset.name,
                                start_slot,
                                end_slot,
                                scraper
                            )
                            total_created += created
                            logger.info(f"Created {created} sparse ranges for dataset {dataset.name}")
            
            return total_created
            
        except Exception as e:
            logger.error(f"Error in bulk_create_ranges: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
            
    def _cleanup_disabled_datasets(self, enabled_datasets: Set[str], start_slot: int, end_slot: int):
        """Remove state entries for datasets not in enabled list."""
        # Get all datasets currently in state
        query = """
        SELECT DISTINCT dataset
        FROM indexing_state FINAL
        WHERE mode = 'historical'
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
        """
        
        results = self.db.execute(query, {
            'start_slot': start_slot,
            'end_slot': end_slot
        })
        
        existing_datasets = {row['dataset'] for row in results}
        datasets_to_remove = existing_datasets - enabled_datasets
        
        if datasets_to_remove:
            logger.info(f"Cleaning up state for disabled datasets: {datasets_to_remove}")
            
            for dataset in datasets_to_remove:
                # Use ALTER TABLE DELETE for immediate removal
                delete_query = """
                ALTER TABLE indexing_state
                DELETE WHERE mode = 'historical' 
                  AND dataset = %(dataset)s
                  AND start_slot >= %(start_slot)s
                  AND end_slot <= %(end_slot)s
                """
                
                self.db.execute(delete_query, {
                    'dataset': dataset,
                    'start_slot': start_slot,
                    'end_slot': end_slot
                })
                
            # Force merge to apply deletes
            self.db.execute("OPTIMIZE TABLE indexing_state FINAL")
            logger.info(f"Cleaned up {len(datasets_to_remove)} disabled datasets")
            
    def _create_sparse_ranges(self, mode: str, dataset: str, start_slot: int, 
                             end_slot: int, scraper) -> int:
        """Create ranges for sparse datasets like validators."""
        # Get target slots from scraper
        target_slots = scraper.get_target_slots_in_range(start_slot, end_slot, self.db)
        
        if not target_slots:
            logger.info(f"No target slots found for {dataset} in range {start_slot}-{end_slot}")
            return 0
            
        logger.info(f"Found {len(target_slots)} target slots for {dataset}")
        
        # Group target slots into ranges
        ranges_to_create = []
        current_range_start = None
        last_slot = None
        
        for slot in sorted(target_slots):
            if current_range_start is None:
                current_range_start = slot
                last_slot = slot
            elif slot - last_slot > self.range_size:
                # Gap too large, close current range
                ranges_to_create.append((current_range_start, last_slot + 1))
                current_range_start = slot
                last_slot = slot
            else:
                last_slot = slot
                
        # Close final range
        if current_range_start is not None:
            ranges_to_create.append((current_range_start, last_slot + 1))
            
        # Create state entries
        created = 0
        for range_start, range_end in ranges_to_create:
            if self.create_range(mode, dataset, range_start, range_end):
                created += 1
                
        return created
        
    def bulk_create_ranges_for_dataset(self, mode: str, dataset: str, 
                                       start_slot: int, end_slot: int) -> int:
        """Create all ranges for a dataset using bulk insert."""
        # Validate dataset name
        if not dataset or not isinstance(dataset, str):
            logger.error(f"Invalid dataset name: {dataset}")
            return 0
            
        logger.info(f"Bulk creating ranges for dataset '{dataset}' from slot {start_slot} to {end_slot}")
        
        # First, get all existing ranges in one query
        check_query = """
        SELECT start_slot, end_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
          AND status IN ('pending', 'processing', 'completed')
        ORDER BY start_slot
        """
        
        existing_ranges = self.db.execute(check_query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        # Convert to set of tuples for fast lookup
        existing_set = {(r['start_slot'], r['end_slot']) for r in existing_ranges}
        
        # Build list of ranges to create
        ranges_to_create = []
        current = start_slot
        
        while current < end_slot:
            range_end = min(current + self.range_size, end_slot)
            
            # Check if this range already exists
            if (current, range_end) not in existing_set:
                ranges_to_create.append({
                    "mode": mode,
                    "dataset": dataset,
                    "start_slot": current,
                    "end_slot": range_end,
                    "status": "pending",
                    "batch_id": ""
                })
            
            current = range_end
        
        # Bulk insert all ranges
        if ranges_to_create:
            logger.info(f"Bulk creating {len(ranges_to_create)} ranges for {dataset}")
            
            # Insert in batches to avoid memory issues
            batch_size = int(os.getenv("STATE_BULK_INSERT_BATCH_SIZE", "5000"))
            
            # Define the insert query template for execute_many
            insert_query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, batch_id, version)
            VALUES
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, %(status)s, %(batch_id)s, now())
            """
            
            for i in range(0, len(ranges_to_create), batch_size):
                batch = ranges_to_create[i:i + batch_size]
                
                try:
                    # Use execute_many for bulk insert
                    self.db.execute_many(insert_query, batch)
                    logger.info(f"Inserted batch of {len(batch)} ranges ({i+1}-{min(i+batch_size, len(ranges_to_create))} of {len(ranges_to_create)})")
                except Exception as e:
                    logger.error(f"Error bulk inserting ranges: {e}")
                    # Fall back to individual inserts for this batch
                    for range_data in batch:
                        self.create_range(
                            range_data["mode"], 
                            range_data["dataset"],
                            range_data["start_slot"], 
                            range_data["end_slot"],
                            range_data["batch_id"]
                        )
        
        return len(ranges_to_create)
        
    def create_range(self, mode: str, dataset: str, start_slot: int, 
                    end_slot: int, batch_id: str = "") -> bool:
        """Create a single range entry."""
        try:
            # Check if already exists
            status = self.get_range_status(mode, dataset, start_slot, end_slot)
            if status:
                return False
                
            query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, batch_id, version)
            VALUES
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 
             'pending', %(batch_id)s, now())
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "batch_id": batch_id
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating range: {e}")
            return False
            
    def get_range_status(self, mode: str, dataset: str, start_slot: int, 
                        end_slot: int) -> Optional[str]:
        """Get status of a specific range."""
        # Check cache first
        cache_key = f"{mode}:{dataset}:{start_slot}:{end_slot}"
        if self.enable_caching and cache_key in self._cache:
            if time.time() - self._cache_timestamps[cache_key] < self.cache_ttl:
                return self._cache[cache_key]
                
        query = """
        SELECT status
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot = %(start_slot)s
          AND end_slot = %(end_slot)s
        """
        
        results = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        status = results[0]['status'] if results else None
        
        # Update cache
        if self.enable_caching:
            self._cache[cache_key] = status
            self._cache_timestamps[cache_key] = time.time()
            
        return status
        
    def claim_range(self, mode: str, dataset: str, start_slot: int, 
                   end_slot: int, worker_id: str, batch_id: str) -> bool:
        """Claim a range for processing by a worker."""
        try:
            # Use INSERT with status update to claim atomically
            query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, worker_id, 
             batch_id, started_at, version)
            SELECT 
                mode, dataset, start_slot, end_slot,
                'processing' as status,
                %(worker_id)s as worker_id,
                %(batch_id)s as batch_id,
                now() as started_at,
                now() as version
            FROM indexing_state FINAL
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
              AND status = 'pending'
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "worker_id": worker_id,
                "batch_id": batch_id
            })
            
            # Invalidate cache
            cache_key = f"{mode}:{dataset}:{start_slot}:{end_slot}"
            if cache_key in self._cache:
                del self._cache[cache_key]
                del self._cache_timestamps[cache_key]
                
            return True
            
        except Exception as e:
            logger.error(f"Error claiming range: {e}")
            return False
            
    def complete_range(self, mode: str, dataset: str, start_slot: int,
                      end_slot: int, rows_indexed: int = 0) -> bool:
        """Mark a range as completed."""
        try:
            query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, 
             completed_at, rows_indexed, version)
            SELECT 
                mode, dataset, start_slot, end_slot,
                'completed' as status,
                now() as completed_at,
                %(rows_indexed)s as rows_indexed,
                now() as version
            FROM indexing_state FINAL
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
              AND status = 'processing'
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "rows_indexed": rows_indexed
            })
            
            # Invalidate cache
            cache_key = f"{mode}:{dataset}:{start_slot}:{end_slot}"
            if cache_key in self._cache:
                del self._cache[cache_key]
                del self._cache_timestamps[cache_key]
                
            return True
            
        except Exception as e:
            logger.error(f"Error completing range: {e}")
            return False
            
    def reset_stale_processing_jobs(self, stale_minutes: Optional[int] = None) -> int:
        """Reset jobs that have been processing for too long."""
        if stale_minutes is None:
            stale_minutes = self.stale_timeout
            
        try:
            stale_time = datetime.now() - timedelta(minutes=stale_minutes)
            
            # First get stale jobs
            query = """
            SELECT mode, dataset, start_slot, end_slot
            FROM indexing_state FINAL
            WHERE status = 'processing'
              AND started_at < %(stale_time)s
            """
            
            stale_jobs = self.db.execute(query, {"stale_time": stale_time})
            
            if not stale_jobs:
                return 0
                
            # Reset each stale job
            reset_query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, retry_count, version)
            SELECT 
                mode, dataset, start_slot, end_slot,
                'pending' as status,
                retry_count + 1 as retry_count,
                now() as version
            FROM indexing_state FINAL
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
              AND status = 'processing'
            """
            
            reset_count = 0
            for job in stale_jobs:
                self.db.execute(reset_query, job)
                reset_count += 1
                
                # Invalidate cache
                cache_key = f"{job['mode']}:{job['dataset']}:{job['start_slot']}:{job['end_slot']}"
                if cache_key in self._cache:
                    del self._cache[cache_key]
                    del self._cache_timestamps[cache_key]
                    
            logger.info(f"Reset {reset_count} stale processing jobs")
            return reset_count
            
        except Exception as e:
            logger.error(f"Error resetting stale jobs: {e}")
            return 0
            
    def get_pending_ranges(self, mode: str, dataset: str, 
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Get pending ranges for a dataset."""
        query = """
        SELECT start_slot, end_slot, retry_count
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND status = 'pending'
          AND retry_count < %(max_retries)s
        ORDER BY start_slot
        LIMIT %(limit)s
        """
        
        return self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "max_retries": self.max_retries,
            "limit": limit
        })
        
    def get_dataset_progress(self, mode: str, dataset: str) -> Dict[str, Any]:
        """Get progress statistics for a dataset."""
        query = """
        SELECT 
            countIf(status = 'pending') as pending,
            countIf(status = 'processing') as processing,
            countIf(status = 'completed') as completed,
            countIf(status = 'failed') as failed,
            sum(rows_indexed) as total_rows,
            min(start_slot) as min_slot,
            max(end_slot) as max_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
        """
        
        results = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset
        })
        
        return results[0] if results else {}