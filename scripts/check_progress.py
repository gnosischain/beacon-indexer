#!/usr/bin/env python3
"""
Simple progress checker for beacon indexer.
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager

async def main():
    clickhouse = ClickHouseService()
    state_manager = StateManager(clickhouse)
    
    # Reset stale claims first
    state_manager.reset_stale_ranges()
    
    # Get progress from view
    query = "SELECT * FROM indexing_progress ORDER BY scraper_id, table_name"
    results = clickhouse.execute(query)
    
    print("\nIndexing Progress:")
    print("-" * 80)
    print(f"{'Scraper':<20} {'Table':<20} {'Completed':<10} {'Processing':<10} {'Failed':<10} {'Max Slot':<10}")
    print("-" * 80)
    
    for row in results:
        print(f"{row['scraper_id']:<20} {row['table_name']:<20} "
              f"{row['completed']:<10} {row['processing']:<10} "
              f"{row['failed']:<10} {row['max_slot']:<10}")
    
    print("-" * 80)

if __name__ == "__main__":
    asyncio.run(main())