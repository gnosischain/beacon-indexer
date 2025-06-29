import asyncio
from typing import List, Dict, Tuple
from datetime import datetime, timedelta
from src.services.state_manager import StateManager
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger
from src.config import config

class GapDetectionService:
    """Service to detect and fill gaps in indexed data."""
    
    def __init__(self, clickhouse: ClickHouseService, state_manager: StateManager):
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.check_interval = config.scraper.gap_check_interval_seconds
        self.running = False
    
    async def start(self):
        """Start the gap detection service."""
        self.running = True
        logger.info("Starting gap detection service")
        
        while self.running:
            try:
                await self._check_for_gaps()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in gap detection: {e}")
                await asyncio.sleep(60)  # Wait before retry
    
    async def stop(self):
        """Stop the gap detection service."""
        self.running = False
        logger.info("Stopping gap detection service")
    
    async def _check_for_gaps(self):
        """Check for gaps in all scraper/table combinations."""
        # Get all unique scraper/table combinations
        query = """
        SELECT DISTINCT scraper_id, table_name
        FROM indexing_state
        WHERE status = 'completed'
        """
        
        result = self.clickhouse.execute(query)
        
        for row in result:
            scraper_id = row['scraper_id']
            table_name = row['table_name']
            
            # Get the range to check
            range_query = """
            SELECT 
                MIN(start_slot) as min_slot,
                MAX(end_slot) as max_slot
            FROM indexing_state
            WHERE scraper_id = %(scraper_id)s
              AND table_name = %(table_name)s
              AND status = 'completed'
            """
            
            range_result = self.clickhouse.execute(range_query, {
                "scraper_id": scraper_id,
                "table_name": table_name
            })
            
            if not range_result:
                continue
            
            min_slot = range_result[0]['min_slot']
            max_slot = range_result[0]['max_slot']
            
            # Find gaps
            gaps = self.state_manager.find_gaps(scraper_id, table_name, min_slot, max_slot)
            
            if gaps:
                logger.info(f"Found {len(gaps)} gaps for {scraper_id}/{table_name}")
                
                # Create pending ranges for gaps
                for gap_start, gap_end in gaps:
                    # Check if gap is already being processed
                    if not self._is_gap_being_processed(scraper_id, table_name, gap_start, gap_end):
                        self._create_pending_range(scraper_id, table_name, gap_start, gap_end)
    
    def _is_gap_being_processed(self, scraper_id: str, table_name: str, 
                               start_slot: int, end_slot: int) -> bool:
        """Check if a gap is already being processed."""
        query = """
        SELECT COUNT(*) as count
        FROM indexing_state
        WHERE scraper_id = %(scraper_id)s
          AND table_name = %(table_name)s
          AND start_slot = %(start_slot)s
          AND end_slot = %(end_slot)s
          AND status IN ('pending', 'processing')
        """
        
        result = self.clickhouse.execute(query, {
            "scraper_id": scraper_id,
            "table_name": table_name,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        return result[0]['count'] > 0 if result else False
    
    def _create_pending_range(self, scraper_id: str, table_name: str, 
                            start_slot: int, end_slot: int):
        """Create a pending range for a gap."""
        query = """
        INSERT INTO indexing_state
        (scraper_id, table_name, start_slot, end_slot, status, batch_id)
        VALUES
        (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 
         'pending', 'gap_fill')
        """
        
        self.clickhouse.execute(query, {
            "scraper_id": scraper_id,
            "table_name": table_name,
            "start_slot": start_slot,
            "end_slot": end_slot
        })
        
        logger.info(f"Created pending range for gap: {scraper_id}/{table_name} {start_slot}-{end_slot}")