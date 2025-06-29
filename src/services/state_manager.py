"""
State management for tracking indexing progress.
"""
import os
import time
import uuid
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timedelta
import threading

from src.utils.logger import logger
from src.services.clickhouse_service import ClickHouseService


class StateManager:
    """Manages indexing state across scrapers and tables."""
    
    def __init__(self, clickhouse: ClickHouseService):
        self.db = clickhouse
        self.range_size = int(os.getenv("STATE_RANGE_SIZE", "10000"))
        self._lock = threading.Lock()
        self._cache = {}  # Simple cache for completed ranges
        self._cache_ttl = 300  # 5 minutes
        
    def _get_latest_status(self, scraper_id: str, table_name: str, 
                          start_slot: int, end_slot: int) -> Optional[Dict]:
        """Get the latest status for a range using FINAL to handle ReplacingMergeTree."""
        query = """
        SELECT 
            status,
            worker_id,
            rows_indexed,
            updated_at
        FROM indexing_state FINAL
        WHERE scraper_id = %(scraper_id)s
          AND table_name = %(table_name)s
          AND start_slot = %(start_slot)s
          AND end_slot = %(end_slot)s
        ORDER BY updated_at DESC
        LIMIT 1
        """
        
        result = self.db.execute(query, {
            "scraper_id": scraper_id,
            "table_name": table_name,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return result[0] if result else None
    
    def bulk_create_ranges(self, scrapers: Dict, start_slot: int, end_slot: int) -> int:
        """Bulk create ranges for all scrapers and tables."""
        logger.info(f"Bulk creating ranges from {start_slot} to {end_slot}")
        
        # Calculate all ranges
        ranges = []
        current_start = start_slot
        while current_start <= end_slot:
            range_end = min(current_start + self.range_size - 1, end_slot)
            ranges.append((current_start, range_end))
            current_start = range_end + 1
        
        logger.info(f"Calculated {len(ranges)} ranges")
        
        # Prepare all range entries
        all_entries = []
        for scraper_id in scrapers:
            # Get tables for this scraper
            if hasattr(scrapers[scraper_id], 'get_tables_written'):
                temp_scraper = scrapers[scraper_id](None, self.db)
                tables = temp_scraper.get_tables_written()
            else:
                tables = ['default']
            
            for table in tables:
                for range_start, range_end in ranges:
                    all_entries.append({
                        'scraper_id': scraper_id,
                        'table_name': table,
                        'start_slot': range_start,
                        'end_slot': range_end
                    })
        
        logger.info(f"Prepared {len(all_entries)} total entries for bulk insert")
        
        # Check which entries already exist using FINAL
        if all_entries:
            # Check existing ranges
            existing_ranges = set()
            check_query = """
            SELECT DISTINCT scraper_id, table_name, start_slot, end_slot
            FROM indexing_state FINAL
            WHERE (scraper_id, table_name, start_slot, end_slot) IN (%(values)s)
            """
            
            # Build values for IN clause
            values_list = []
            for entry in all_entries:
                values_list.append(f"('{entry['scraper_id']}', '{entry['table_name']}', {entry['start_slot']}, {entry['end_slot']})")
            
            if values_list:
                # Execute in batches to avoid query too long
                batch_size = 100
                for i in range(0, len(values_list), batch_size):
                    batch = values_list[i:i+batch_size]
                    batch_query = f"""
                    SELECT DISTINCT scraper_id, table_name, start_slot, end_slot
                    FROM indexing_state FINAL
                    WHERE (scraper_id, table_name, start_slot, end_slot) IN ({','.join(batch)})
                    """
                    
                    results = self.db.execute(batch_query)
                    for row in results:
                        key = f"{row['scraper_id']}:{row['table_name']}:{row['start_slot']}:{row['end_slot']}"
                        existing_ranges.add(key)
            
            logger.info(f"Found {len(existing_ranges)} existing ranges")
            
            # Filter out existing entries
            entries_to_create = []
            for entry in all_entries:
                key = f"{entry['scraper_id']}:{entry['table_name']}:{entry['start_slot']}:{entry['end_slot']}"
                if key not in existing_ranges:
                    entries_to_create.append(entry)
            
            logger.info(f"Will create {len(entries_to_create)} new ranges")
            
            # Bulk insert new entries
            if entries_to_create:
                # Insert in batches
                insert_batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(entries_to_create), insert_batch_size):
                    batch = entries_to_create[i:i+insert_batch_size]
                    
                    # Build values for batch insert
                    values = []
                    batch_id = str(uuid.uuid4())
                    for entry in batch:
                        values.append(
                            f"('{entry['scraper_id']}', "
                            f"'{entry['table_name']}', "
                            f"{entry['start_slot']}, "
                            f"{entry['end_slot']}, "
                            f"'pending', "  # status
                            f"'', "  # worker_id
                            f"'{batch_id}', "  # batch_id
                            f"0, "  # attempt_count
                            f"0, "  # rows_indexed
                            f"'', "  # error_message
                            f"now(), "  # started_at
                            f"NULL, "  # completed_at
                            f"now(), "  # created_at
                            f"now())"  # updated_at
                        )
                    
                    # Insert with ALL columns explicitly specified
                    insert_query = f"""
                    INSERT INTO indexing_state
                    (scraper_id, table_name, start_slot, end_slot, status, 
                    worker_id, batch_id, attempt_count, rows_indexed, error_message,
                    started_at, completed_at, created_at, updated_at)
                    VALUES {','.join(values)}
                    """
                    
                    try:
                        self.db.execute(insert_query)
                        total_inserted += len(batch)
                        
                        if i > 0 and i % 5000 == 0:
                            logger.info(f"Inserted {total_inserted} ranges so far...")
                    except Exception as e:
                        logger.error(f"Error inserting batch: {e}")
                
                logger.info(f"Successfully bulk inserted {total_inserted} ranges")
                return total_inserted
            else:
                logger.info("All ranges already exist, nothing to create")
                return 0
        
        return 0
    
    def claim_range(self, scraper_id: str, table_name: str, 
                   start_slot: int, end_slot: int, worker_id: str,
                   force: bool = False) -> bool:
        """Try to claim a range. Returns True if successful."""
        with self._lock:
            try:
                # Check current status with FINAL
                current_status = self._get_latest_status(scraper_id, table_name, start_slot, end_slot)
                
                # Can we claim it?
                can_claim = False
                if not current_status:
                    logger.warning(f"No status found for {scraper_id}/{table_name} {start_slot}-{end_slot}, creating new entry")
                    can_claim = True
                elif current_status['status'] == 'pending':
                    can_claim = True
                elif current_status['status'] == 'completed':
                    logger.debug(f"Range {scraper_id}/{table_name} {start_slot}-{end_slot} already completed")
                    return False
                elif force and current_status['status'] in ['failed', 'processing']:
                    can_claim = True
                elif current_status['status'] == 'processing' and current_status['worker_id'] == worker_id:
                    can_claim = True
                
                if not can_claim:
                    logger.debug(f"Cannot claim range {scraper_id}/{table_name} {start_slot}-{end_slot}, status: {current_status['status'] if current_status else 'unknown'}")
                    return False
                
                # Always use the same batch_id for this range
                batch_id = f"{scraper_id}:{table_name}:{start_slot}:{end_slot}"
                
                # Insert new status - this will replace the old one due to ReplacingMergeTree
                insert_query = """
                INSERT INTO indexing_state 
                (scraper_id, table_name, start_slot, end_slot, status, worker_id, 
                started_at, updated_at, batch_id, attempt_count, rows_indexed, 
                error_message, completed_at, created_at)
                VALUES 
                (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 
                'processing', %(worker_id)s, now(), now(), %(batch_id)s, 
                %(attempt_count)s, 0, '', NULL, now())
                """
                
                attempt_count = current_status['attempt_count'] + 1 if current_status and 'attempt_count' in current_status else 1
                
                self.db.execute(insert_query, {
                    "scraper_id": scraper_id,
                    "table_name": table_name,
                    "start_slot": start_slot,
                    "end_slot": end_slot,
                    "worker_id": worker_id,
                    "batch_id": batch_id,
                    "attempt_count": attempt_count
                })
                
                logger.debug(f"Claimed range {scraper_id}/{table_name} {start_slot}-{end_slot} for {worker_id}")
                return True
                    
            except Exception as e:
                logger.error(f"Error claiming range: {e}")
                return False
    
    def complete_range(self, scraper_id: str, table_name: str,
                      start_slot: int, end_slot: int, rows_indexed: int) -> None:
        """Mark a range as completed."""
        with self._lock:
            try:
                # Use consistent batch_id
                batch_id = f"{scraper_id}:{table_name}:{start_slot}:{end_slot}"
                
                query = """
                INSERT INTO indexing_state 
                (scraper_id, table_name, start_slot, end_slot, status, 
                 rows_indexed, completed_at, updated_at, worker_id, batch_id,
                 attempt_count, error_message, started_at, created_at)
                VALUES 
                (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 
                 'completed', %(rows_indexed)s, now(), now(), '', %(batch_id)s,
                 0, '', now(), now())
                """
                
                self.db.execute(query, {
                    "scraper_id": scraper_id,
                    "table_name": table_name,
                    "start_slot": start_slot,
                    "end_slot": end_slot,
                    "rows_indexed": rows_indexed,
                    "batch_id": batch_id
                })
                
                cache_key = f"{scraper_id}:{table_name}:{start_slot}:{end_slot}"
                self._cache[cache_key] = {
                    'status': 'completed',
                    'timestamp': time.time()
                }
                
                logger.debug(f"Completed range {scraper_id}/{table_name} {start_slot}-{end_slot} with {rows_indexed} rows")
                
            except Exception as e:
                logger.error(f"Error completing range: {e}")
    
    def fail_range(self, scraper_id: str, table_name: str,
                  start_slot: int, end_slot: int, error: str) -> None:
        """Mark a range as failed."""
        with self._lock:
            try:
                # Use consistent batch_id
                batch_id = f"{scraper_id}:{table_name}:{start_slot}:{end_slot}"
                
                query = """
                INSERT INTO indexing_state 
                (scraper_id, table_name, start_slot, end_slot, status, 
                 error_message, updated_at, worker_id, batch_id,
                 attempt_count, rows_indexed, started_at, completed_at, created_at)
                VALUES 
                (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 
                 'failed', %(error_message)s, now(), '', %(batch_id)s,
                 0, 0, now(), NULL, now())
                """
                
                self.db.execute(query, {
                    "scraper_id": scraper_id,
                    "table_name": table_name,
                    "start_slot": start_slot,
                    "end_slot": end_slot,
                    "error_message": error[:500],
                    "batch_id": batch_id
                })
                
                logger.debug(f"Failed range {scraper_id}/{table_name} {start_slot}-{end_slot}: {error}")
                
            except Exception as e:
                logger.error(f"Error marking range as failed: {e}")
    
    def get_next_range(self, scraper_id: str, table_name: str, 
                      worker_id: str, max_slot: Optional[int] = None) -> Optional[Tuple[int, int]]:
        """Get next available range for processing."""
        with self._lock:
            try:
                # Find the next pending range using FINAL
                query = """
                SELECT start_slot, end_slot
                FROM indexing_state FINAL
                WHERE scraper_id = %(scraper_id)s
                  AND table_name = %(table_name)s
                  AND status = 'pending'
                """
                
                params = {
                    "scraper_id": scraper_id,
                    "table_name": table_name
                }
                
                if max_slot is not None:
                    query += " AND start_slot <= %(max_slot)s"
                    params["max_slot"] = max_slot
                
                query += " ORDER BY start_slot LIMIT 1"
                
                result = self.db.execute(query, params)
                
                if result:
                    start_slot = result[0]['start_slot']
                    end_slot = result[0]['end_slot']
                    return (start_slot, end_slot)
                
                return None
                
            except Exception as e:
                logger.error(f"Error getting next range: {e}")
                return None
    
    def get_progress(self, scraper_id: str = None, table_name: str = None) -> Dict:
        """Get progress statistics using FINAL."""
        conditions = []
        params = {}
        
        if scraper_id:
            conditions.append("scraper_id = %(scraper_id)s")
            params["scraper_id"] = scraper_id
        
        if table_name:
            conditions.append("table_name = %(table_name)s")
            params["table_name"] = table_name
        
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        query = f"""
        SELECT 
            countIf(status = 'completed') as completed,
            countIf(status = 'processing') as processing,
            countIf(status = 'pending') as pending,
            countIf(status = 'failed') as failed,
            max(end_slot) as max_slot,
            sum(rows_indexed) as total_rows
        FROM indexing_state FINAL
        {where_clause}
        """
        
        result = self.db.execute(query, params)
        
        if result:
            return result[0]
        else:
            return {
                'completed': 0,
                'processing': 0,
                'pending': 0,
                'failed': 0,
                'max_slot': 0,
                'total_rows': 0
            }
    
    def reset_stale_ranges(self, timeout_minutes: int = 30) -> int:
        """Reset ranges that have been processing for too long."""
        stale_time = datetime.now() - timedelta(minutes=timeout_minutes)
        
        # Get stale ranges first
        select_query = """
        SELECT scraper_id, table_name, start_slot, end_slot, attempt_count
        FROM indexing_state FINAL
        WHERE status = 'processing'
          AND updated_at < %(stale_time)s
        """
        
        stale_ranges = self.db.execute(select_query, {"stale_time": stale_time})
        
        count = 0
        for row in stale_ranges:
            # Use consistent batch_id
            batch_id = f"{row['scraper_id']}:{row['table_name']}:{row['start_slot']}:{row['end_slot']}"
            
            insert_query = """
            INSERT INTO indexing_state 
            (scraper_id, table_name, start_slot, end_slot, status, 
             error_message, updated_at, worker_id, batch_id,
             attempt_count, rows_indexed, started_at, completed_at, created_at)
            VALUES 
            (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 
             'pending', 'Reset due to timeout', now(), '', %(batch_id)s,
             %(attempt_count)s, 0, now(), NULL, now())
            """
            
            self.db.execute(insert_query, {
                "scraper_id": row['scraper_id'],
                "table_name": row['table_name'],
                "start_slot": row['start_slot'],
                "end_slot": row['end_slot'],
                "batch_id": batch_id,
                "attempt_count": row['attempt_count'] + 1
            })
            count += 1
        
        if count > 0:
            logger.info(f"Reset {count} stale ranges back to pending")
        
        return count
    
    def update_sync_position(self, scraper_id: str, table_name: str, slot: int) -> None:
        """Update sync position for realtime mode."""
        query = """
        INSERT INTO sync_position 
        (scraper_id, table_name, last_synced_slot, updated_at)
        VALUES 
        (%(scraper_id)s, %(table_name)s, %(slot)s, now())
        """
        
        self.db.execute(query, {
            "scraper_id": scraper_id,
            "table_name": table_name,
            "slot": slot
        })