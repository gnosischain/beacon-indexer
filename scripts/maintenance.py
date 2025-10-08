#!/usr/bin/env python3
"""
Maintenance status checker and utility script.
Usage: python scripts/maintenance.py [status|failed|gaps|summary]
"""
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src' if os.path.exists('/app/src') else 'src')

from services.clickhouse import ClickHouse
from utils.logger import setup_logger, logger
from config import config
from datetime import datetime, timedelta

def show_failed_chunks():
    """Show all failed chunks with details."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== Failed Chunks ===")
    
    query = """
    SELECT 
        loader_name,
        chunk_id,
        start_slot,
        end_slot,
        status,
        worker_id,
        updated_at,
        toStartOfDay(updated_at) as failure_date
    FROM load_state_chunks FINAL
    WHERE status = 'failed'
    ORDER BY updated_at DESC
    LIMIT 50
    """
    
    try:
        failed_chunks = clickhouse.execute(query)
        
        if not failed_chunks:
            print("✅ No failed chunks found")
            return
        
        print(f"Found {len(failed_chunks)} failed chunks (showing latest 50):\n")
        print(f"{'Loader':<12} {'Start':<8} {'End':<8} {'Worker':<10} {'Failed Date':<12} {'Chunk ID'}")
        print("-" * 80)
        
        for chunk in failed_chunks:
            loader = chunk['loader_name']
            start = chunk['start_slot']
            end = chunk['end_slot']
            worker = chunk['worker_id'] or 'unknown'
            date = str(chunk['failure_date'])
            chunk_id = chunk['chunk_id'][:30] + "..." if len(chunk['chunk_id']) > 30 else chunk['chunk_id']
            
            print(f"{loader:<12} {start:<8} {end:<8} {worker:<10} {date:<12} {chunk_id}")
        
        # Summary by loader
        print(f"\n=== Failed Chunks by Loader ===")
        summary_query = """
        SELECT 
            loader_name,
            COUNT(*) as failed_count,
            MIN(start_slot) as earliest_slot,
            MAX(end_slot) as latest_slot
        FROM load_state_chunks FINAL
        WHERE status = 'failed'
        GROUP BY loader_name
        ORDER BY failed_count DESC
        """
        
        summary = clickhouse.execute(summary_query)
        
        for row in summary:
            loader = row['loader_name']
            count = row['failed_count']
            earliest = row['earliest_slot']
            latest = row['latest_slot']
            print(f"{loader}: {count} failed chunks (slots {earliest} - {latest})")
            
    except Exception as e:
        print(f"❌ Error checking failed chunks: {e}")

def show_data_gaps():
    """Show gaps in raw data tables."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== Data Gaps Analysis ===")
    
    loaders = [name for name in config.ENABLED_LOADERS if name not in ["genesis", "specs"]]
    
    for loader_name in loaders:
        raw_table = f"raw_{loader_name}"
        
        try:
            # Get slot range and count
            range_query = f"""
            SELECT 
                MIN(slot) as min_slot,
                MAX(slot) as max_slot,
                COUNT(DISTINCT slot) as actual_slots
            FROM {raw_table}
            """
            
            range_result = clickhouse.execute(range_query)
            if not range_result or not range_result[0]['min_slot']:
                print(f"{loader_name}: No data found")
                continue
            
            min_slot = range_result[0]['min_slot']
            max_slot = range_result[0]['max_slot']
            actual_slots = range_result[0]['actual_slots']
            expected_slots = max_slot - min_slot + 1
            missing_slots = expected_slots - actual_slots
            
            print(f"\n{loader_name.upper()}:")
            print(f"  Slot range: {min_slot} - {max_slot}")
            print(f"  Expected slots: {expected_slots:,}")
            print(f"  Actual slots: {actual_slots:,}")
            print(f"  Missing slots: {missing_slots:,} ({missing_slots/expected_slots*100:.2f}%)")
            
            if missing_slots > 0 and missing_slots < 1000:  # Show gaps if not too many
                gap_query = f"""
                WITH expected_slots AS (
                    SELECT number + {min_slot} as slot
                    FROM numbers({expected_slots})
                ),
                gaps AS (
                    SELECT e.slot
                    FROM expected_slots e
                    LEFT JOIN {raw_table} r ON e.slot = r.slot
                    WHERE r.slot IS NULL
                    ORDER BY e.slot
                    LIMIT 20
                )
                SELECT GROUP_CONCAT(toString(slot)) as missing_slots
                FROM gaps
                """
                
                gap_result = clickhouse.execute(gap_query)
                if gap_result and gap_result[0]['missing_slots']:
                    missing_list = gap_result[0]['missing_slots']
                    print(f"  Sample missing: {missing_list}")
            
        except Exception as e:
            print(f"{loader_name}: Error - {e}")

def show_transformation_status():
    """Show transformation status and gaps."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== Transformation Status ===")
    
    query = """
    SELECT 
        raw_table_name,
        status,
        COUNT(*) as count,
        SUM(processed_count) as total_processed,
        SUM(failed_count) as total_failed,
        MIN(start_slot) as earliest_slot,
        MAX(end_slot) as latest_slot
    FROM transformer_progress FINAL
    GROUP BY raw_table_name, status
    ORDER BY raw_table_name, status
    """
    
    try:
        results = clickhouse.execute(query)
        
        if not results:
            print("No transformation data found")
            return
        
        current_table = None
        for row in results:
            table = row['raw_table_name']
            status = row['status']
            count = row['count']
            processed = row['total_processed']
            failed = row['total_failed']
            earliest = row['earliest_slot']
            latest = row['latest_slot']
            
            if current_table != table:
                print(f"\n{table.upper()}:")
                current_table = table
            
            print(f"  {status}: {count} ranges, {processed:,} processed, {failed:,} failed")
            print(f"    Slot range: {earliest} - {latest}")
        
        # Check for untransformed completed chunks
        print(f"\n=== Untransformed Data ===")
        
        untransformed_query = """
        SELECT 
            lsc.loader_name,
            COUNT(*) as untransformed_chunks
        FROM load_state_chunks lsc FINAL
        LEFT JOIN (
            SELECT DISTINCT raw_table_name, start_slot, end_slot
            FROM transformer_progress FINAL
            WHERE status = 'completed'
        ) tp ON (
            tp.raw_table_name = CONCAT('raw_', lsc.loader_name)
            AND tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
        )
        WHERE lsc.status = 'completed'
        AND tp.start_slot IS NULL
        GROUP BY lsc.loader_name
        ORDER BY untransformed_chunks DESC
        SETTINGS join_use_nulls = 1
        """
        
        untransformed = clickhouse.execute(untransformed_query)
        
        if untransformed:
            for row in untransformed:
                loader = row['loader_name']
                count = row['untransformed_chunks']
                print(f"  {loader}: {count} completed chunks not yet transformed")
        else:
            print("  ✅ All completed chunks have been transformed")
            
    except Exception as e:
        print(f"❌ Error checking transformation status: {e}")

def show_summary():
    """Show overall system summary."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== System Summary ===")
    
    try:
        # Get chunk overview
        chunk_overview = clickhouse.get_chunk_overview()
        
        if chunk_overview:
            print("\nChunk Status:")
            total_chunks = 0
            total_completed = 0
            total_failed = 0
            
            for row in chunk_overview:
                loader = row["loader_name"]
                total = row["total_chunks"]
                completed = row["completed"]
                failed = row["failed"]
                pending = row["pending"]
                claimed = row["claimed"]
                
                total_chunks += total
                total_completed += completed
                total_failed += failed
                
                print(f"  {loader}: {total} total, {completed} completed, "
                      f"{failed} failed, {pending} pending, {claimed} claimed")
            
            completion_rate = (total_completed / total_chunks * 100) if total_chunks > 0 else 0
            failure_rate = (total_failed / total_chunks * 100) if total_chunks > 0 else 0
            
            print(f"\nOverall: {total_chunks:,} chunks, {completion_rate:.1f}% complete, "
                  f"{failure_rate:.1f}% failed")
        
        # Get data sizes
        print(f"\n=== Data Sizes ===")
        
        size_query = """
        SELECT 
            table,
            formatReadableSize(sum(bytes)) as size,
            sum(rows) as rows
        FROM system.parts 
        WHERE database = current_database()
        AND table NOT LIKE '%_log'
        AND active = 1
        GROUP BY table
        ORDER BY sum(bytes) DESC
        LIMIT 15
        """
        
        sizes = clickhouse.execute(size_query)
        
        for row in sizes:
            table = row['table']
            size = row['size']
            rows = row['rows']
            print(f"  {table}: {size}, {rows:,} rows")
        
        # Recent activity
        print(f"\n=== Recent Activity (Last 24 Hours) ===")
        
        recent_query = """
        SELECT 
            loader_name,
            status,
            COUNT(*) as count
        FROM load_state_chunks FINAL
        WHERE updated_at >= now() - INTERVAL 24 HOUR
        GROUP BY loader_name, status
        ORDER BY loader_name, status
        """
        
        recent = clickhouse.execute(recent_query)
        
        current_loader = None
        for row in recent:
            loader = row['loader_name']
            status = row['status']
            count = row['count']
            
            if current_loader != loader:
                print(f"\n  {loader}:")
                current_loader = loader
            
            print(f"    {status}: {count}")
            
    except Exception as e:
        print(f"❌ Error generating summary: {e}")

def show_maintenance_recommendations():
    """Show maintenance recommendations based on current state."""
    setup_logger()
    clickhouse = ClickHouse()
    
    print("\n=== Maintenance Recommendations ===")
    
    recommendations = []
    
    try:
        # Check for failed chunks
        failed_count_query = """
        SELECT COUNT(*) as failed_count
        FROM load_state_chunks FINAL
        WHERE status = 'failed'
        """
        
        failed_result = clickhouse.execute(failed_count_query)
        failed_count = failed_result[0]['failed_count'] if failed_result else 0
        
        if failed_count > 0:
            recommendations.append({
                "priority": "HIGH",
                "issue": f"{failed_count} failed chunks found",
                "action": "Run 'maintain fix --start-slot X --end-slot Y' to fix failed chunks",
                "command": "python -m src.main maintain check --start-slot 0 --end-slot 999999999"
            })
        
        # Check for old claimed chunks (stuck workers)
        stuck_query = """
        SELECT COUNT(*) as stuck_count
        FROM load_state_chunks FINAL
        WHERE status = 'claimed'
        AND updated_at < now() - INTERVAL 2 HOUR
        """
        
        stuck_result = clickhouse.execute(stuck_query)
        stuck_count = stuck_result[0]['stuck_count'] if stuck_result else 0
        
        if stuck_count > 0:
            recommendations.append({
                "priority": "MEDIUM",
                "issue": f"{stuck_count} chunks claimed for >2 hours (stuck workers)",
                "action": "Reset stuck chunks to pending",
                "command": "python -m src.main maintain reset --start-slot 0 --end-slot 999999999 --status claimed"
            })
        
        # Check for untransformed data
        untransformed_query = """
        SELECT COUNT(*) as untransformed_count
        FROM load_state_chunks lsc FINAL
        LEFT JOIN transformer_progress tp FINAL ON (
            tp.raw_table_name = CONCAT('raw_', lsc.loader_name)
            AND tp.start_slot = lsc.start_slot
            AND tp.end_slot = lsc.end_slot
            AND tp.status = 'completed'
        )
        WHERE lsc.status = 'completed'
        AND tp.start_slot IS NULL
        SETTINGS join_use_nulls = 1
        """
        
        untransformed_result = clickhouse.execute(untransformed_query)
        untransformed_count = untransformed_result[0]['untransformed_count'] if untransformed_result else 0
        
        if untransformed_count > 0:
            recommendations.append({
                "priority": "MEDIUM",
                "issue": f"{untransformed_count} loaded chunks not yet transformed",
                "action": "Run transformer to process completed chunks",
                "command": "python -m src.main transform batch"
            })
        
        # Show recommendations
        if recommendations:
            for i, rec in enumerate(recommendations, 1):
                print(f"\n{i}. [{rec['priority']}] {rec['issue']}")
                print(f"   Action: {rec['action']}")
                print(f"   Command: {rec['command']}")
        else:
            print("\n✅ No immediate maintenance actions needed!")
            print("\nOptional maintenance you can run:")
            print("  - Integrity check: python -m src.main maintain check --start-slot X --end-slot Y")
            print("  - System summary: python scripts/maintenance.py summary")
            
    except Exception as e:
        print(f"❌ Error generating recommendations: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/maintenance.py [status|failed|gaps|summary|recommendations]")
        print("")
        print("Commands:")
        print("  failed          - Show all failed chunks")
        print("  gaps           - Analyze data gaps in raw tables")
        print("  status         - Show transformation status") 
        print("  summary        - Show system summary")
        print("  recommendations - Show maintenance recommendations")
        sys.exit(1)
    
    action = sys.argv[1]
    
    if action == "failed":
        show_failed_chunks()
    elif action == "gaps":
        show_data_gaps()
    elif action == "status":
        show_transformation_status()
    elif action == "summary":
        show_summary()
    elif action == "recommendations":
        show_maintenance_recommendations()
    else:
        print("Unknown action. Use: failed, gaps, status, summary, or recommendations")
        sys.exit(1)