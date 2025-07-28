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
        created = 0
        
        # Get scraper IDs
        scraper_ids = [scraper.scraper_id for scraper in scrapers]
        logger.info(f"Creating ranges for scrapers: {scraper_ids}")
        
        # Create ranges for each dataset
        for scraper_id in scraper_ids:
            datasets = self.dataset_registry.get_datasets_for_scraper(scraper_id)
            
            for dataset in datasets:
                if dataset.is_continuous:
                    dataset_created = self._create_dataset_ranges(
                        dataset.name, start_slot, end_slot
                    )
                    created += dataset_created
                    logger.info(f"Created {dataset_created} ranges for {dataset.name}")
                    
        return created
        
    def _create_dataset_ranges(self, dataset_name: str, start_slot: int, end_slot: int) -> int:
        """Create ranges for a single dataset."""
        created = 0
        current = start_slot
        
        while current < end_slot:
            range_end = min(current + self.range_size, end_slot)
            
            # Check if range already exists
            status = self.get_range_status("historical", dataset_name, current, range_end)
            
            if status is None:
                if self.create_range("historical", dataset_name, current, range_end):
                    created += 1
                    
            current = range_end
            
        return created
        
    def create_range(self, mode: str, dataset: str, start_slot: int, end_slot: int) -> bool:
        """Create a new range in pending state."""
        try:
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, created_at, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'pending', now(), now())
            """
            
            self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot
            })
            return True
            
        except Exception as e:
            logger.error(f"Error creating range: {e}")
            return False
            
    def get_range_status(self, mode: str, dataset: str, start_slot: int, end_slot: int) -> Optional[StateStatus]:
        """Get the current status of a range."""
        try:
            query = """
            SELECT status
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot
            })
            
            # Convert result to list if it's a generator
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            if result:
                status_str = result[0]['status']
                return StateStatus[status_str.upper()]
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting range status: {e}")
            return None
            
    def claim_range(self, mode: str, dataset: str, start_slot: int, end_slot: int, 
                   worker_id: str, batch_id: str = "") -> bool:
        """Atomically claim a range for processing."""
        try:
            # Check current status
            current_status = self.get_range_status(mode, dataset, start_slot, end_slot)
            
            # Can only claim pending or failed ranges
            if current_status not in [StateStatus.PENDING, StateStatus.FAILED, None]:
                return False
                
            # Insert new status
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, worker_id, started_at, batch_id, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'processing', 
             %(worker_id)s, now(), %(batch_id)s, now())
            """
            
            self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot,
                'worker_id': worker_id,
                'batch_id': batch_id
            })
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
            (mode, dataset, start_slot, end_slot, status, completed_at, rows_indexed, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'completed', 
             now(), %(rows_indexed)s, now())
            """
            
            self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot,
                'rows_indexed': rows_indexed
            })
            logger.info(f"Completed range {dataset} {start_slot}-{end_slot}")
            
        except Exception as e:
            logger.error(f"Error completing range: {e}")
            
    def fail_range(self, mode: str, dataset: str, start_slot: int, 
                   end_slot: int, error_message: str) -> None:
        """Mark a range as failed."""
        try:
            # Get current attempt count
            query = """
            SELECT attempt_count 
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
              AND status = 'processing'
            ORDER BY version DESC
            LIMIT 1
            """
            
            result = self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot
            })
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            current_attempts = result[0]['attempt_count'] if result else 0
            
            # Insert failed status
            query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, error_message, attempt_count, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'failed', 
             %(error_message)s, %(attempt_count)s, now())
            """
            
            self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot,
                'error_message': error_message[:500],
                'attempt_count': current_attempts + 1
            })
            
        except Exception as e:
            logger.error(f"Error marking range as failed: {e}")
            
    def get_next_pending_range(self, mode: str, dataset: str, 
                             max_slot: Optional[int] = None) -> Optional[Tuple[int, int]]:
        """Get next pending range to process."""
        try:
            query = """
            SELECT start_slot, end_slot
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND status IN ('pending', 'failed')
              AND attempt_count < %(max_retries)s
            """
            
            params = {
                'mode': mode,
                'dataset': dataset,
                'max_retries': self.max_retries
            }
            
            if max_slot:
                query += " AND start_slot < %(max_slot)s"
                params['max_slot'] = max_slot
                
            query += " ORDER BY start_slot LIMIT 1"
            
            result = self.db.execute(query, params)
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            if result:
                return (result[0]['start_slot'], result[0]['end_slot'])
                
            return None
            
        except Exception as e:
            logger.error(f"Error getting next pending range: {e}")
            return None
            
    def get_dataset_ranges(self, mode: str, dataset: str, 
                          start_slot: int, end_slot: int) -> List[Dict[str, Any]]:
        """Get all ranges for a dataset in a slot range."""
        try:
            query = """
            SELECT DISTINCT ON (start_slot, end_slot)
                start_slot, end_slot, status, attempt_count, worker_id
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot >= %(start_slot)s
              AND end_slot <= %(end_slot)s
            ORDER BY start_slot, end_slot, version DESC
            """
            
            result = self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot
            })
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
            
            ranges = []
            for row in result:
                ranges.append({
                    'start_slot': row['start_slot'],
                    'end_slot': row['end_slot'],
                    'status': row['status'],
                    'attempt_count': row['attempt_count'],
                    'worker_id': row['worker_id']
                })
                
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting dataset ranges: {e}")
            return []
            
    def reset_stale_jobs(self, mode: str = "historical") -> int:
        """Reset stale processing jobs to pending."""
        try:
            # Find stale jobs
            query = """
            SELECT dataset, start_slot, end_slot, worker_id
            FROM indexing_state
            WHERE mode = %(mode)s
              AND status = 'processing'
              AND started_at < now() - INTERVAL %(timeout)s MINUTE
            """
            
            result = self.db.execute(query, {
                'mode': mode,
                'timeout': self.stale_timeout
            })
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            reset_count = 0
            
            for row in result:
                dataset = row['dataset']
                start_slot = row['start_slot']
                end_slot = row['end_slot']
                worker_id = row['worker_id']
                
                # Reset to pending
                self.fail_range(
                    mode, dataset, start_slot, end_slot,
                    f"Reset stale job from worker {worker_id}"
                )
                reset_count += 1
                logger.info(f"Reset stale job: {dataset} {start_slot}-{end_slot}")
                
            return reset_count
            
        except Exception as e:
            logger.error(f"Error resetting stale jobs: {e}")
            return 0
    
    def reset_stale_processing_jobs(self, mode: str = "historical") -> int:
        """Reset stale processing jobs to pending - alias for compatibility."""
        return self.reset_stale_jobs(mode)
            
    def get_progress_summary(self, mode: str) -> Dict[str, Dict[str, Any]]:
        """Get progress summary for all datasets."""
        try:
            query = """
            WITH latest_status AS (
                SELECT DISTINCT ON (dataset, start_slot, end_slot)
                    dataset, start_slot, end_slot, status, rows_indexed
                FROM indexing_state
                WHERE mode = %(mode)s
                ORDER BY dataset, start_slot, end_slot, version DESC
            )
            SELECT 
                dataset,
                COUNT(*) as total_ranges,
                COUNT(*) FILTER (WHERE status = 'completed') as completed_ranges,
                COUNT(*) FILTER (WHERE status = 'processing') as processing_ranges,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_ranges,
                COUNT(*) FILTER (WHERE status = 'pending') as pending_ranges,
                COALESCE(SUM(rows_indexed) FILTER (WHERE status = 'completed'), 0) as total_rows_indexed
            FROM latest_status
            GROUP BY dataset
            """
            
            result = self.db.execute(query, {'mode': mode})
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
            
            summary = {}
            for row in result:
                dataset = row['dataset']
                summary[dataset] = {
                    'total_ranges': row['total_ranges'],
                    'completed_ranges': row['completed_ranges'],
                    'processing_ranges': row['processing_ranges'],
                    'failed_ranges': row['failed_ranges'],
                    'pending_ranges': row['pending_ranges'],
                    'total_rows_indexed': row['total_rows_indexed'],
                    'range_size': self.range_size
                }
                
            return summary
            
        except Exception as e:
            logger.error(f"Error getting progress summary: {e}")
            return {}

    # New methods for lazy range creation
    
    def get_or_create_next_range(
        self, 
        scraper_id: str, 
        table_name: str,
        target_end_slot: int,
        worker_id: str,
        mode: str = "historical"
    ) -> Optional[Tuple[int, int]]:
        """
        Get next available range or create a new one if needed.
        Returns (start_slot, end_slot) or None if no work available.
        """
        dataset_key = f"{scraper_id}_{table_name}"
        
        logger.debug(f"get_or_create_next_range called for dataset: {dataset_key}, mode: {mode}")
        
        # First, try to claim an existing pending or failed range
        existing_range = self._claim_existing_range(mode, dataset_key, worker_id)
        if existing_range:
            return existing_range
            
        logger.debug(f"No existing range to claim for {dataset_key}, trying to create next")
        
        # No existing range, create next one based on progress
        return self._create_next_range(mode, dataset_key, target_end_slot, worker_id)
    
    def _claim_existing_range(self, mode: str, dataset: str, worker_id: str) -> Optional[Tuple[int, int]]:
        """Try to claim an existing pending or retryable failed range."""
        try:
            # Find claimable ranges
            query = """
            SELECT start_slot, end_slot, status, attempt_count, started_at
            FROM indexing_state
            WHERE mode = %(mode)s
            AND dataset = %(dataset)s
            AND (
                status = 'pending'
                OR (status = 'failed' AND attempt_count < %(max_retries)s)
                OR (status = 'processing' AND started_at < now() - INTERVAL %(timeout)s MINUTE)
            )
            ORDER BY start_slot
            LIMIT 1
            """
            
            logger.debug(f"Looking for claimable range for {dataset} in mode {mode}")
            
            result = self.db.execute(query, {
                'mode': mode,
                'dataset': dataset,
                'max_retries': self.max_retries,
                'timeout': self.stale_timeout
            })
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            if not result:
                logger.debug(f"No claimable ranges found for {dataset}")
                return None
                
            start_slot = result[0]['start_slot']
            end_slot = result[0]['end_slot']
            status = result[0]['status']
            
            logger.debug(f"Found claimable range for {dataset}: {start_slot}-{end_slot} (status: {status})")
            
            # Atomically claim the range
            if self.claim_range(mode, dataset, start_slot, end_slot, worker_id):
                logger.info(f"Worker {worker_id} claimed existing range {start_slot}-{end_slot} for {dataset}")
                return (start_slot, end_slot)
            else:
                logger.warning(f"Failed to claim range {start_slot}-{end_slot} for {dataset}")
                
            return None
            
        except Exception as e:
            logger.error(f"Error claiming existing range: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _create_next_range(self, mode: str, dataset: str, target_end_slot: int, worker_id: str) -> Optional[Tuple[int, int]]:
        """Create the next range based on current progress."""
        try:
            # Find the maximum completed end_slot
            query = """
            SELECT MAX(end_slot) as max_end
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND status = 'completed'
            """
            
            result = self.db.execute(query, {
                'mode': mode,
                'dataset': dataset
            })
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            max_completed = result[0]['max_end'] if result and result[0]['max_end'] is not None else None
            
            # Determine next start slot
            if max_completed is None:
                # No completed ranges, check if any ranges exist
                check_query = """
                SELECT MIN(start_slot) as min_start
                FROM indexing_state
                WHERE mode = %(mode)s
                  AND dataset = %(dataset)s
                """
                check_result = self.db.execute(check_query, {
                    'mode': mode,
                    'dataset': dataset
                })
                
                # Convert result to list
                if hasattr(check_result, '__iter__') and not isinstance(check_result, list):
                    check_result = list(check_result)
                
                if check_result and check_result[0]['min_start'] is not None:
                    # Ranges exist but none completed
                    return None
                else:
                    # No ranges at all, start from beginning
                    next_start = 0
            else:
                next_start = max_completed
            
            # Check if we've reached the target
            if next_start >= target_end_slot:
                return None
                
            # Calculate next end slot
            next_end = min(next_start + self.range_size, target_end_slot)
            
            # Create and claim the new range atomically
            if self._create_and_claim_range(mode, dataset, next_start, next_end, worker_id):
                logger.info(f"Worker {worker_id} created new range {next_start}-{next_end} for {dataset}")
                return (next_start, next_end)
                
            return None
            
        except Exception as e:
            logger.error(f"Error creating next range: {e}")
            return None
    
    def _create_and_claim_range(self, mode: str, dataset: str, start_slot: int, 
                                end_slot: int, worker_id: str) -> bool:
        """Create and claim a new range atomically."""
        try:
            # Check if range already exists (race condition)
            check_query = """
            SELECT 1 FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND start_slot = %(start_slot)s
              AND end_slot = %(end_slot)s
            LIMIT 1
            """
            
            check_result = self.db.execute(check_query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot
            })
            
            # Convert result to list
            if hasattr(check_result, '__iter__') and not isinstance(check_result, list):
                check_result = list(check_result)
                
            if check_result:
                return False  # Range already exists
            
            # Create and claim in one operation
            insert_query = """
            INSERT INTO indexing_state 
            (mode, dataset, start_slot, end_slot, status, worker_id, started_at, attempt_count, version)
            VALUES 
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'processing', %(worker_id)s, 
             now(), 1, now())
            """
            
            self.db.execute(insert_query, {
                'mode': mode,
                'dataset': dataset,
                'start_slot': start_slot,
                'end_slot': end_slot,
                'worker_id': worker_id
            })
            return True
            
        except Exception as e:
            # Likely a race condition
            logger.debug(f"Race condition creating range (expected): {e}")
            return False
    
    def find_gaps(self, dataset: str, min_slot: int = 0, max_slot: Optional[int] = None, 
                  mode: str = "historical") -> List[Tuple[int, int]]:
        """Find gaps in completed ranges."""
        try:
            # Get all completed ranges
            query = """
            SELECT DISTINCT start_slot, end_slot
            FROM indexing_state
            WHERE mode = %(mode)s
              AND dataset = %(dataset)s
              AND status = 'completed'
              AND start_slot >= %(min_slot)s
            """
            
            params = {
                'mode': mode,
                'dataset': dataset,
                'min_slot': min_slot
            }
            
            if max_slot:
                query += " AND end_slot <= %(max_slot)s"
                params['max_slot'] = max_slot
                
            query += " ORDER BY start_slot"
            
            result = self.db.execute(query, params)
            
            # Convert result to list
            if hasattr(result, '__iter__') and not isinstance(result, list):
                result = list(result)
                
            if not result:
                return [(min_slot, max_slot)] if max_slot else []
            
            gaps = []
            expected_start = min_slot
            
            for row in result:
                start = row['start_slot']
                end = row['end_slot']
                
                if start > expected_start:
                    # Found a gap
                    gaps.append((expected_start, start))
                expected_start = max(expected_start, end)
            
            # Check for gap at the end
            if max_slot and expected_start < max_slot:
                gaps.append((expected_start, max_slot))
                
            return gaps
            
        except Exception as e:
            logger.error(f"Error finding gaps: {e}")
            return []