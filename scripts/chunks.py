#!/usr/bin/env python3
"""
Simple chunk status checker.
Usage: python scripts/chunks.py [overview|status|test]
"""
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src' if os.path.exists('/app/src') else 'src')

from services.clickhouse import ClickHouse
from utils.logger import setup_logger

def show_overview():
    """Show simple overview of chunks."""
    setup_logger()
    clickhouse = ClickHouse()
    
    overview = clickhouse.get_chunk_overview()
    
    print("\n=== Chunk Overview ===")
    if not overview:
        print("No chunks found")
        return
    
    print(f"{'Loader':<12} {'Total':<6} {'Pending':<8} {'Active':<8} {'Done':<8} {'Failed':<8}")
    print("-" * 60)
    
    for row in overview:
        loader = row["loader_name"]
        total = row["total_chunks"]
        pending = row["pending"] 
        claimed = row["claimed"]
        completed = row["completed"]
        failed = row["failed"]
        
        print(f"{loader:<12} {total:<6} {pending:<8} {claimed:<8} {completed:<8} {failed:<8}")
    
    # Calculate totals
    total_all = sum(row["total_chunks"] for row in overview)
    pending_all = sum(row["pending"] for row in overview)
    claimed_all = sum(row["claimed"] for row in overview)
    completed_all = sum(row["completed"] for row in overview)
    failed_all = sum(row["failed"] for row in overview)
    
    print("-" * 60)
    print(f"{'TOTAL':<12} {total_all:<6} {pending_all:<8} {claimed_all:<8} {completed_all:<8} {failed_all:<8}")
    
    # Show progress percentage
    if total_all > 0:
        progress = (completed_all / total_all) * 100
        print(f"\nProgress: {completed_all}/{total_all} ({progress:.1f}%)")
        
        if claimed_all > 0:
            print(f"Currently processing: {claimed_all} chunks")
        elif completed_all == total_all:
            print("✅ All chunks completed!")
        if failed_all > 0:
            print(f"⚠️  Failed chunks: {failed_all} (check logs for errors)")

def show_status():
    """Show detailed chunk status."""
    setup_logger()
    clickhouse = ClickHouse()
    
    counts = clickhouse.get_chunk_counts()
    
    print("\n=== Detailed Chunk Status ===")
    if not counts:
        print("No chunks found")
        return
    
    for loader, statuses in counts.items():
        print(f"\n{loader}:")
        total = 0
        for status, count in statuses.items():
            print(f"  {status:<10}: {count:>4}")
            total += count
        print(f"  {'Total':<10}: {total:>4}")

def test_connection():
    """Test ClickHouse connection."""
    setup_logger()
    try:
        clickhouse = ClickHouse()
        result = clickhouse.execute("SELECT 1 as test")
        if result and result[0]["test"] == 1:
            print("✅ ClickHouse connection OK")
            
            # Test FINAL queries
            chunks_test = clickhouse.execute("SELECT COUNT(*) as count FROM load_state_chunks FINAL")
            print(f"✅ Found {chunks_test[0]['count']} chunks in database")
        else:
            print("❌ ClickHouse connection failed")
    except Exception as e:
        print(f"❌ ClickHouse connection error: {e}")

def debug_chunks():
    """Debug chunk counting issues."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== Debug Chunk Counting ===")
    
    # Show raw count vs FINAL count
    raw_count = clickhouse.execute("SELECT COUNT(*) as count FROM load_state_chunks")[0]["count"]
    final_count = clickhouse.execute("SELECT COUNT(*) as count FROM load_state_chunks FINAL")[0]["count"]
    
    print(f"Raw count: {raw_count}")
    print(f"FINAL count: {final_count}")
    
    if raw_count != final_count:
        print("⚠️  ReplacingMergeTree has duplicates - this is normal")
        print("   Run: OPTIMIZE TABLE load_state_chunks FINAL; to force merge")
    
    # Show status breakdown with and without FINAL
    print("\n--- Without FINAL ---")
    no_final = clickhouse.execute("""
        SELECT loader_name, status, count() as count
        FROM load_state_chunks
        GROUP BY loader_name, status
        ORDER BY loader_name, status
    """)
    for row in no_final:
        print(f"{row['loader_name']:<12} {row['status']:<10} {row['count']}")
    
    print("\n--- With FINAL (correct) ---")
    with_final = clickhouse.execute("""
        SELECT loader_name, status, count() as count
        FROM load_state_chunks FINAL
        GROUP BY loader_name, status
        ORDER BY loader_name, status
    """)
    for row in with_final:
        print(f"{row['loader_name']:<12} {row['status']:<10} {row['count']}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/chunks.py [overview|status|test|debug]")
        print("")
        print("Commands:")
        print("  overview  - Simple overview table (recommended)")
        print("  status    - Detailed status breakdown")
        print("  test      - Test ClickHouse connection")
        print("  debug     - Debug chunk counting issues")
        print("")
        print("✨ Chunks are automatically managed - no manual fixes needed!")
        sys.exit(1)
    
    action = sys.argv[1]
    
    if action == "overview":
        show_overview()
    elif action == "status":
        show_status()
    elif action == "test":
        test_connection()
    elif action == "debug":
        debug_chunks()
    else:
        print("Unknown action. Use: overview, status, test, or debug")
        sys.exit(1)