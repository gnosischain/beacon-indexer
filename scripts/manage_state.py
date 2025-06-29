#!/usr/bin/env python3
"""
CLI tool for managing indexing state.
"""
import asyncio
import sys
import os
import argparse
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.config import config
from rich.console import Console
from rich.table import Table

console = Console()

async def reset_failed_ranges(state_manager: StateManager, scraper_id: str = None, table_name: str = None):
    """Reset failed ranges back to pending."""
    query = """
    SELECT DISTINCT scraper_id, table_name, start_slot, end_slot
    FROM indexing_state
    WHERE status = 'failed'
    """
    
    params = {}
    if scraper_id:
        query += " AND scraper_id = %(scraper_id)s"
        params['scraper_id'] = scraper_id
    if table_name:
        query += " AND table_name = %(table_name)s"
        params['table_name'] = table_name
    
    results = state_manager.db.execute(query, params)
    
    count = 0
    for row in results:
        # Reset to pending with attempt count = 0
        update_query = """
        INSERT INTO indexing_state
        (scraper_id, table_name, start_slot, end_slot, status, attempt_count)
        VALUES
        (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 'pending', 0)
        """
        
        state_manager.db.execute(update_query, {
            "scraper_id": row['scraper_id'],
            "table_name": row['table_name'],
            "start_slot": row['start_slot'],
            "end_slot": row['end_slot']
        })
        count += 1
    
    console.print(f"[green]Reset {count} failed ranges to pending[/green]")

async def clear_state(state_manager: StateManager, scraper_id: str = None, table_name: str = None):
    """Clear state for specific scraper/table."""
    if not scraper_id and not table_name:
        console.print("[red]Must specify at least scraper_id or table_name[/red]")
        return
    
    # Confirm action
    console.print(f"[yellow]This will delete all state for:")
    if scraper_id:
        console.print(f"  Scraper: {scraper_id}")
    if table_name:
        console.print(f"  Table: {table_name}")
    
    confirm = console.input("[yellow]Continue? (y/N): [/yellow]")
    if confirm.lower() != 'y':
        console.print("[red]Cancelled[/red]")
        return
    
    # Delete from indexing state
    delete_query = "DELETE FROM indexing_state WHERE 1=1"
    params = {}
    
    if scraper_id:
        delete_query += " AND scraper_id = %(scraper_id)s"
        params['scraper_id'] = scraper_id
    if table_name:
        delete_query += " AND table_name = %(table_name)s"
        params['table_name'] = table_name
    
    state_manager.db.execute(delete_query, params)
    
    # Delete from sync position
    sync_delete = "DELETE FROM sync_position WHERE 1=1"
    if scraper_id:
        sync_delete += " AND scraper_id = %(scraper_id)s"
    if table_name:
        sync_delete += " AND table_name = %(table_name)s"
    
    state_manager.db.execute(sync_delete, params)
    
    console.print("[green]State cleared successfully[/green]")

async def show_gaps(state_manager: StateManager):
    """Show detected gaps in indexing."""
    # Get all unique scraper/table combinations
    query = """
    SELECT DISTINCT scraper_id, table_name
    FROM indexing_state
    WHERE status = 'completed'
    """
    
    results = state_manager.db.execute(query)
    
    if not results:
        console.print("[green]No completed ranges found![/green]")
        return
    
    all_gaps = []
    
    for row in results:
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
        
        range_result = state_manager.db.execute(range_query, {
            "scraper_id": scraper_id,
            "table_name": table_name
        })
        
        if not range_result:
            continue
        
        min_slot = range_result[0]['min_slot']
        max_slot = range_result[0]['max_slot']
        
        # Find gaps
        gaps = state_manager.find_gaps(scraper_id, table_name, min_slot, max_slot)
        
        for gap_start, gap_end in gaps:
            all_gaps.append({
                'scraper_id': scraper_id,
                'table_name': table_name,
                'start_slot': gap_start,
                'end_slot': gap_end,
                'gap_size': gap_end - gap_start
            })
    
    if not all_gaps:
        console.print("[green]No gaps detected![/green]")
        return
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Scraper", style="cyan")
    table.add_column("Table", style="cyan")
    table.add_column("Gap Start", justify="right")
    table.add_column("Gap End", justify="right")
    table.add_column("Size", justify="right")
    
    for gap in all_gaps[:100]:  # Limit to first 100 gaps
        table.add_row(
            gap['scraper_id'],
            gap['table_name'],
            str(gap['start_slot']),
            str(gap['end_slot']),
            str(gap['gap_size'])
        )
    
    console.print(table)
    
    if len(all_gaps) > 100:
        console.print(f"[yellow]Showing first 100 gaps out of {len(all_gaps)} total[/yellow]")

async def create_ranges(state_manager: StateManager, scraper_id: str, table_name: str, 
                       start_slot: int, end_slot: int):
    """Create pending ranges for a scraper/table."""
    range_size = state_manager.range_size
    
    # Align to range boundaries
    aligned_start = (start_slot // range_size) * range_size
    aligned_end = ((end_slot // range_size) + 1) * range_size
    
    count = 0
    current = aligned_start
    
    while current < aligned_end:
        range_end = min(current + range_size, aligned_end)
        
        # Check if range exists
        check_query = """
        SELECT COUNT(*) as count
        FROM indexing_state
        WHERE scraper_id = %(scraper_id)s
          AND table_name = %(table_name)s
          AND start_slot = %(start_slot)s
          AND end_slot = %(end_slot)s
        """
        
        result = state_manager.db.execute(check_query, {
            "scraper_id": scraper_id,
            "table_name": table_name,
            "start_slot": current,
            "end_slot": range_end
        })
        
        if result[0]['count'] == 0:
            # Create range
            insert_query = """
            INSERT INTO indexing_state
            (scraper_id, table_name, start_slot, end_slot, status, batch_id)
            VALUES
            (%(scraper_id)s, %(table_name)s, %(start_slot)s, %(end_slot)s, 'pending', 'manual')
            """
            
            state_manager.db.execute(insert_query, {
                "scraper_id": scraper_id,
                "table_name": table_name,
                "start_slot": current,
                "end_slot": range_end
            })
            count += 1
        
        current = range_end
    
    console.print(f"[green]Created {count} pending ranges[/green]")

async def main():
    parser = argparse.ArgumentParser(description='Manage beacon indexer state')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Progress command
    progress_parser = subparsers.add_parser('progress', help='Show indexing progress')
    progress_parser.add_argument('--scraper', help='Filter by scraper ID')
    
    # Reset failed command
    reset_parser = subparsers.add_parser('reset-failed', help='Reset failed ranges')
    reset_parser.add_argument('--scraper', help='Filter by scraper ID')
    reset_parser.add_argument('--table', help='Filter by table name')
    
    # Clear state command
    clear_parser = subparsers.add_parser('clear', help='Clear state for scraper/table')
    clear_parser.add_argument('--scraper', help='Scraper ID')
    clear_parser.add_argument('--table', help='Table name')
    
    # Show gaps command
    gaps_parser = subparsers.add_parser('gaps', help='Show indexing gaps')
    
    # Create ranges command
    create_parser = subparsers.add_parser('create-ranges', help='Create pending ranges')
    create_parser.add_argument('scraper', help='Scraper ID')
    create_parser.add_argument('table', help='Table name')
    create_parser.add_argument('start_slot', type=int, help='Start slot')
    create_parser.add_argument('end_slot', type=int, help='End slot')
    
    # Reset stale command
    stale_parser = subparsers.add_parser('reset-stale', help='Reset stale jobs')
    stale_parser.add_argument('--timeout', type=int, default=30, help='Timeout in minutes')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize services
    clickhouse = ClickHouseService()
    state_manager = StateManager(clickhouse)
    
    # Execute command
    if args.command == 'progress':
        # Import from the progress script
        from check_indexing_progress import main as show_progress
        await show_progress()
    
    elif args.command == 'reset-failed':
        await reset_failed_ranges(state_manager, args.scraper, args.table)
    
    elif args.command == 'clear':
        await clear_state(state_manager, args.scraper, args.table)
    
    elif args.command == 'gaps':
        await show_gaps(state_manager)
    
    elif args.command == 'create-ranges':
        await create_ranges(state_manager, args.scraper, args.table, 
                          args.start_slot, args.end_slot)
    
    elif args.command == 'reset-stale':
        count = state_manager.reset_stale_ranges(args.timeout)
        console.print(f"[green]Reset {count} stale jobs[/green]")

if __name__ == "__main__":
    asyncio.run(main())