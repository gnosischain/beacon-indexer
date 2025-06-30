"""
Enhanced state management for tracking indexing progress.
"""
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple, Set, Any
from datetime import datetime, timedelta
import threading

from src.utils.logger import logger
from src.services.clickhouse_service import ClickHouseService
from src.core.state import StateStatus, IndexingRange
from src.core.datasets import DatasetRegistry


class StateManager:
    """Enhanced state manager with operation mode support."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.db = clickhouse
        self.range_size = int(os.getenv("STATE_RANGE_SIZE", "10000"))
        self._lock = threading.Lock()
        self._cache = {}  # Simple cache for completed ranges
        self._cache_ttl = 300  # 5 minutes
        self.dataset_registry = DatasetRegistry()
        
    def get_range_status(self, mode: str, dataset: str, 
                        start_slot: int, end_slot: int) -> Optional[StateStatus]:
        """Get the current status of a range."""
        query = """
        SELECT status, worker_id, started_at
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot = %(start_slot)s
          AND end_slot = %(end_slot)s
        ORDER BY version DESC
        LIMIT 1
        """
        
        result = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        if result:
            status_str = result[0]['status']
            
            # Check if processing status is stale
            if status_str == 'processing' and result[0].get('started_at'):
                started_at = result[0]['started_at']
                stale_timeout = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "30"))
                
                if datetime.now() - started_at > timedelta(minutes=stale_timeout):
                    logger.warning(
                        f"Range {dataset} {start_slot}-{end_slot} has stale processing status"
                    )
                    return StateStatus.PENDING
                    
            return StateStatus(status_str)
            
        return None
        
    def claim_range(self, mode: str, dataset: str, start_slot: int, 
                   end_slot: int, worker_id: str, batch_id: str = "",
                   force: bool = False) -> bool:
        """Atomically claim a range for processing."""
        with self._lock:
            try:
                # Check current status
                status = self.get_range_status(mode, dataset, start_slot, end_slot)
                
                # Can we claim it?
                if not force and status == StateStatus.COMPLETED:
                    logger.debug(f"Range {dataset} {start_slot}-{end_slot} already completed")
                    return False
                    
                if not force and status == StateStatus.PROCESSING:
                    logger.debug(f"Range {dataset} {start_slot}-{end_slot} being processed")
                    return False
                    
                # Get attempt count
                attempt_count = 0
                if status:
                    count_query = """
                    SELECT attempt_count
                    FROM indexing_state FINAL
                    WHERE mode = %(mode)s
                      AND dataset = %(dataset)s
                      AND start_slot = %(start_slot)s
                      AND end_slot = %(end_slot)s
                    ORDER BY version DESC
                    LIMIT 1
                    """
                    
                    result = self.db.execute(count_query, {
                        "mode": mode,
                        "dataset": dataset,
                        "start_slot": start_slot,
                        "end_slot": end_slot
                    })
                    
                    if result:
                        attempt_count = result[0].get('attempt_count', 0)
                        
                # Insert new status
                insert_query = """
                INSERT INTO indexing_state 
                (mode, dataset, start_slot, end_slot, status, worker_id, 
                 started_at, batch_id, attempt_count, version)
                VALUES 
                (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 
                 'processing', %(worker_id)s, now(), %(batch_id)s, 
                 %(attempt_count)s, now())
                """
                
                self.db.execute(insert_query, {
                    "mode": mode,
                    "dataset": dataset,
                    "start_slot": start_slot,
                    "end_slot": end_slot,
                    "worker_id": worker_id,
                    "batch_id": batch_id,
                    "attempt_count": attempt_count
                })
                
                logger.debug(f"Claimed range {dataset} {start_slot}-{end_slot} for {worker_id}")
                return True
                    
            except Exception as e:
                logger.error(f"Error claiming range: {e}")
                return False
                
    def complete_range(self, mode: str, dataset: str, start_slot: int, 
                      end_slot: int, rows_indexed: int = 0) -> None:
        """Mark a range as completed."""
        try:
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, 
             completed_at, rows_indexed, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 
             'completed', now(), %(rows_indexed)s, now())
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "rows_indexed": rows_indexed
            })
            
            logger.debug(f"Completed range {dataset} {start_slot}-{end_slot} ({rows_indexed} rows)")
            
        except Exception as e:
            logger.error(f"Error completing range: {e}")
            
    def fail_range(self, mode: str, dataset: str, start_slot: int, 
                   end_slot: int, error_message: str) -> None:
        """Mark a range as failed."""
        try:
            # Get current attempt count
            attempt_count = 0
            count_query = """
            SELECT attempt_count
            FROM indexing_state FINAL
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db.execute(count_query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot
            })
            
            if result:
                attempt_count = result[0].get('attempt_count', 0) + 1
                
            # Determine if we should retry
            max_attempts = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
            status = 'failed' if attempt_count >= max_attempts else 'pending'
            
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, 
             error_message, attempt_count, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 
             %(status)s, %(error_message)s, %(attempt_count)s, now())
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot,
                "status": status,
                "error_message": error_message[:500],  # Truncate long errors
                "attempt_count": attempt_count
            })
            
            logger.error(
                f"Failed range {dataset} {start_slot}-{end_slot} "
                f"(attempt {attempt_count}/{max_attempts}): {error_message}"
            )
            
        except Exception as e:
            logger.error(f"Error marking range as failed: {e}")
            
    def create_range(self, mode: str, dataset: str, start_slot: int, 
                    end_slot: int, batch_id: str = "") -> bool:
        """Create a new range if it doesn't exist."""
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
            
    def create_ranges_for_dataset(self, mode: str, dataset: str, 
                                 start_slot: int, end_slot: int) -> int:
        """Create all ranges for a dataset in a slot range."""
        created = 0
        
        current = start_slot
        while current < end_slot:
            range_end = min(current + self.range_size, end_slot)
            
            if self.create_range(mode, dataset, current, range_end):
                created += 1
                
            current = range_end
            
        return created
    
    def bulk_create_ranges_for_dataset(self, mode: str, dataset: str, 
                                       start_slot: int, end_slot: int) -> int:
        """Create all ranges for a dataset in a slot range using bulk insert."""
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
    
    def bulk_create_ranges(self, scrapers: List[Any], start_slot: int, end_slot: int) -> int:
        """Create ranges for all scrapers in the given slot range."""
        try:
            total_created = 0
            
            # Get all datasets for the enabled scrapers
            datasets_to_process = set()
            
            for scraper in scrapers:
                # Get datasets for this scraper from the registry
                scraper_datasets = self.dataset_registry.get_datasets_for_scraper(scraper.scraper_id)
                for dataset in scraper_datasets:
                    if dataset.is_continuous:
                        datasets_to_process.add(dataset.name)
                        logger.info(f"Found dataset '{dataset.name}' for scraper '{scraper.scraper_id}'")
            
            # Create ranges for each dataset
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
            
            return total_created
            
        except Exception as e:
            logger.error(f"Error in bulk_create_ranges: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 0
    
    def reset_stale_ranges(self, stale_minutes: int = None) -> int:
        """Reset ranges that have been processing for too long."""
        if stale_minutes is None:
            stale_minutes = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "30"))
            
        try:
            # Same as reset_stale_processing_jobs but using the name expected by the service
            return self.reset_stale_processing_jobs(stale_minutes)
        except Exception as e:
            logger.error(f"Error resetting stale ranges: {e}")
            return 0
    
    async def bulk_create_validator_ranges(self, mode: str, dataset: str,
                                          start_slot: int, end_slot: int,
                                          validator_scraper) -> int:
        """Create ranges for validator dataset with target slots using bulk operations."""
        logger.info(f"Creating validator ranges for {dataset} from slot {start_slot} to {end_slot}")
        
        range_size = self.range_size
        
        # Get all target slots in the range at once
        all_target_slots = validator_scraper.get_target_slots_in_range(
            start_slot, end_slot, self.db
        )
        
        if not all_target_slots:
            logger.warning(f"No validator target slots found in range {start_slot}-{end_slot}")
            return 0
        
        # Group target slots by range
        ranges_with_targets = set()
        for slot in all_target_slots:
            range_start = (slot // range_size) * range_size
            range_end = range_start + range_size
            ranges_with_targets.add((range_start, range_end))
        
        # Get all existing ranges in one query
        check_query = """
        SELECT start_slot, end_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
          AND status IN ('pending', 'processing', 'completed')
        """
        
        existing_ranges = self.db.execute(check_query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot - range_size,  # Include boundary
            "end_slot": end_slot + range_size
        })
        
        existing_set = {(r['start_slot'], r['end_slot']) for r in existing_ranges}
        
        # Build list of ranges to create
        ranges_to_create = []
        for range_start, range_end in sorted(ranges_with_targets):
            if (range_start, range_end) not in existing_set:
                ranges_to_create.append({
                    "mode": mode,
                    "dataset": dataset,
                    "start_slot": range_start,
                    "end_slot": range_end,
                    "status": "pending",
                    "batch_id": ""
                })
        
        # Bulk insert
        if ranges_to_create:
            logger.info(f"Bulk creating {len(ranges_to_create)} validator ranges containing target slots")
            
            # Use the same batch size setting
            batch_size = int(os.getenv("STATE_BULK_INSERT_BATCH_SIZE", "5000"))
            
            # Define the insert query template
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
                    logger.info(f"Inserted validator batch of {len(batch)} ranges")
                except Exception as e:
                    logger.error(f"Error bulk inserting validator ranges: {e}")
                    # Fall back to individual inserts for this batch
                    for range_data in batch:
                        self.create_range(
                            range_data["mode"], 
                            range_data["dataset"],
                            range_data["start_slot"], 
                            range_data["end_slot"],
                            ""
                        )
        
        logger.info(f"Created {len(ranges_to_create)} validator ranges containing target slots")
        return len(ranges_to_create)
        
    def get_next_pending_range(self, mode: str, dataset: str, 
                              max_slot: Optional[int] = None) -> Optional[Tuple[int, int]]:
        """Get the next pending range to process."""
        query = """
        SELECT start_slot, end_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND status = 'pending'
        """
        
        params = {
            "mode": mode,
            "dataset": dataset
        }
        
        if max_slot is not None:
            query += " AND start_slot <= %(max_slot)s"
            params["max_slot"] = max_slot
            
        query += " ORDER BY start_slot LIMIT 1"
        
        result = self.db.execute(query, params)
        
        if result:
            return (result[0]['start_slot'], result[0]['end_slot'])
            
        return None
        
    def find_gaps(self, mode: str, dataset: str, start_slot: int, 
                  end_slot: int, min_gap_size: int = 1) -> List[Tuple[int, int]]:
        """Find gaps in indexed data."""
        query = """
        SELECT start_slot, end_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
          AND status = 'completed'
        ORDER BY start_slot
        """
        
        completed_ranges = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        if not completed_ranges:
            return [(start_slot, end_slot)]
            
        gaps = []
        current_pos = start_slot
        
        for range_data in completed_ranges:
            range_start = range_data['start_slot']
            range_end = range_data['end_slot']
            
            # Gap before this range?
            if range_start > current_pos:
                gap_size = range_start - current_pos
                if gap_size >= min_gap_size:
                    gaps.append((current_pos, range_start))
                    
            # Move position
            current_pos = max(current_pos, range_end)
            
        # Final gap?
        if current_pos < end_slot:
            gaps.append((current_pos, end_slot))
            
        return gaps
    
    def reset_all_processing_jobs(self) -> int:
        """Reset all jobs stuck in processing state back to pending."""
        try:
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, version)
            SELECT 
                mode, dataset, start_slot, end_slot, 
                'pending' as status, now() as version
            FROM indexing_state FINAL
            WHERE status = 'processing'
            """
            
            # Get count first
            count_query = """
            SELECT COUNT(*) as count
            FROM indexing_state FINAL
            WHERE status = 'processing'
            """
            
            result = self.db.execute(count_query, {})
            count = result[0]['count'] if result else 0
            
            if count > 0:
                self.db.execute(query, {})
                logger.info(f"Reset {count} processing jobs to pending status")
            else:
                logger.info("No processing jobs to reset")
                
            return count
            
        except Exception as e:
            logger.error(f"Error resetting processing jobs: {e}")
            return 0
    
    def reset_stale_processing_jobs(self, stale_minutes: int = None) -> int:
        """Reset jobs that have been processing for too long."""
        if stale_minutes is None:
            stale_minutes = int(os.getenv("STALE_JOB_TIMEOUT_MINUTES", "30"))
            
        try:
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, version)
            SELECT 
                mode, dataset, start_slot, end_slot, 
                'pending' as status, now() as version
            FROM indexing_state FINAL
            WHERE status = 'processing'
              AND started_at < now() - INTERVAL %(minutes)s MINUTE
            """
            
            # Get count first
            count_query = """
            SELECT COUNT(*) as count
            FROM indexing_state FINAL
            WHERE status = 'processing'
              AND started_at < now() - INTERVAL %(minutes)s MINUTE
            """
            
            result = self.db.execute(count_query, {"minutes": stale_minutes})
            count = result[0]['count'] if result else 0
            
            if count > 0:
                self.db.execute(query, {"minutes": stale_minutes})
                logger.info(f"Reset {count} stale processing jobs to pending status")
            else:
                logger.info("No stale processing jobs to reset")
                
            return count
            
        except Exception as e:
            logger.error(f"Error resetting stale processing jobs: {e}")
            return 0
            
    def get_progress_summary(self, mode: str) -> Dict[str, Dict[str, Any]]:
        """Get progress summary for all datasets in a mode."""
        query = """
        SELECT 
            dataset,
            countIf(status = 'completed') as completed_ranges,
            countIf(status = 'pending') as pending_ranges,
            countIf(status = 'processing') as processing_ranges,
            countIf(status = 'failed') as failed_ranges,
            sum(rows_indexed) as total_rows_indexed,
            max(end_slot) as highest_slot
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
        GROUP BY dataset
        """
        
        result = self.db.execute(query, {"mode": mode})
        
        summary = {}
        for row in result:
            summary[row['dataset']] = {
                'completed_ranges': row['completed_ranges'],
                'pending_ranges': row['pending_ranges'],
                'processing_ranges': row['processing_ranges'],
                'failed_ranges': row['failed_ranges'],
                'total_rows_indexed': row['total_rows_indexed'] or 0,
                'highest_slot': row['highest_slot'] or 0
            }
            
        return summary
        
    def update_sync_position(self, mode: str, dataset: str, last_slot: int) -> None:
        """Update sync position for continuous mode."""
        try:
            query = """
            INSERT INTO sync_position (mode, dataset, last_synced_slot, updated_at)
            VALUES (%(mode)s, %(dataset)s, %(last_slot)s, now())
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "last_slot": last_slot
            })
            
        except Exception as e:
            logger.error(f"Error updating sync position: {e}")
            
    def get_sync_position(self, mode: str, dataset: str) -> Optional[int]:
        """Get the last synced position."""
        query = """
        SELECT last_synced_slot
        FROM sync_position FINAL
        WHERE mode = %(mode)s AND dataset = %(dataset)s
        ORDER BY updated_at DESC
        LIMIT 1
        """
        
        result = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset
        })
        
        if result:
            return result[0]['last_synced_slot']
            
        return None
        
    def get_last_synced_slot(self, mode: str, datasets: List[str]) -> int:
        """Get the minimum last synced slot across datasets."""
        if not datasets:
            return 0
            
        query = """
        SELECT MIN(last_synced_slot) as min_slot
        FROM sync_position FINAL
        WHERE mode = %(mode)s AND dataset IN %(datasets)s
        """
        
        result = self.db.execute(query, {
            "mode": mode,
            "datasets": datasets
        })
        
        if result and result[0]['min_slot'] is not None:
            return result[0]['min_slot']
            
        return 0
        
    def get_failed_ranges(self, mode: str, dataset: str, start_slot: int,
                         end_slot: int, max_attempts: int) -> List[Tuple[int, int, int, str]]:
        """Get failed ranges that can be retried."""
        query = """
        SELECT start_slot, end_slot, attempt_count, error_message
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND start_slot >= %(start_slot)s
          AND end_slot <= %(end_slot)s
          AND status = 'failed'
          AND attempt_count < %(max_attempts)s
        ORDER BY start_slot
        """
        
        result = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "start_slot": start_slot,
            "end_slot": end_slot,
            "max_attempts": max_attempts
        })
        
        return [(r['start_slot'], r['end_slot'], r['attempt_count'], r['error_message']) 
                for r in result]
                
    def reset_failed_range(self, mode: str, dataset: str, start_slot: int, end_slot: int) -> None:
        """Reset a failed range to pending."""
        try:
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'pending', now())
            """
            
            self.db.execute(query, {
                "mode": mode,
                "dataset": dataset,
                "start_slot": start_slot,
                "end_slot": end_slot
            })
            
        except Exception as e:
            logger.error(f"Error resetting failed range: {e}")
            
    def count_completed_by_batch(self, mode: str, dataset: str, batch_id: str) -> int:
        """Count completed ranges for a specific batch."""
        query = """
        SELECT COUNT(*) as count
        FROM indexing_state FINAL
        WHERE mode = %(mode)s
          AND dataset = %(dataset)s
          AND batch_id = %(batch_id)s
          AND status = 'completed'
        """
        
        result = self.db.execute(query, {
            "mode": mode,
            "dataset": dataset,
            "batch_id": batch_id
        })
        
        return result[0]['count'] if result else 0