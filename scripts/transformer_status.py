#!/usr/bin/env python3
"""
Transformer status checker for range-based progress tracking.
Usage: python scripts/transformer_status.py
"""
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src' if os.path.exists('/app/src') else 'src')

from services.clickhouse import ClickHouse
from services.transformer import TransformerService
from utils.logger import setup_logger

def show_transformer_status():
    """Show transformer processing status."""
    setup_logger()
    transformer = TransformerService()
    
    print("\n=== Transformer Processing Status ===")
    
    status = transformer.get_processing_status()
    
    for table_name, table_status in status.items():
        print(f"\n{table_name.upper()}:")
        print(f"  Total ranges: {table_status['total_ranges']}")
        print(f"  Completed ranges: {table_status['completed_ranges']}")
        print(f"  Failed ranges: {table_status['failed_ranges']}")
        print(f"  Processing ranges: {table_status['processing_ranges']}")
        print(f"  Total processed items: {table_status['total_processed_items']}")
        print(f"  Total failed items: {table_status['total_failed_items']}")
        
        if table_status['max_completed_slot'] is not None:
            print(f"  Max completed slot: {table_status['max_completed_slot']}")
        else:
            print("  No completed ranges yet")
    
    # Show failed ranges
    failed_ranges = transformer.get_failed_ranges(5)
    if failed_ranges:
        print(f"\n=== Recent Failures ===")
        print(f"{'Table':<15} {'Start':<8} {'End':<8} {'Failed':<6} {'Error'}")
        print("-" * 80)
        
        for row in failed_ranges:
            table = row["raw_table_name"]
            start = row["start_slot"]
            end = row["end_slot"]
            failed = row["failed_count"]
            error = row["error_message"][:50] + "..." if len(row["error_message"]) > 50 else row["error_message"]
            print(f"{table:<15} {start:<8} {end:<8} {failed:<6} {error}")
    
    # Show recent activity
    clickhouse = ClickHouse()
    recent_query = """
    SELECT raw_table_name, start_slot, end_slot, status, processed_count, failed_count, processed_at
    FROM transformer_progress FINAL
    ORDER BY processed_at DESC
    LIMIT 10
    """
    
    recent_results = clickhouse.execute(recent_query)
    if recent_results:
        print(f"\n=== Recent Processing Activity ===")
        print(f"{'Table':<15} {'Start':<8} {'End':<8} {'Status':<10} {'OK':<4} {'Fail':<4} {'Processed At'}")
        print("-" * 80)
        
        for row in recent_results:
            table = row["raw_table_name"]
            start = row["start_slot"]
            end = row["end_slot"]
            status = row["status"]
            processed = row["processed_count"]
            failed = row["failed_count"]
            processed_at = row["processed_at"]
            print(f"{table:<15} {start:<8} {end:<8} {status:<10} {processed:<4} {failed:<4} {processed_at}")

if __name__ == "__main__":
    show_transformer_status()