#!/usr/bin/env python3
"""
State management CLI for the beacon indexer.
"""
import click
import os
import sys
from typing import Optional
from datetime import datetime, timedelta

# Add the parent directory to the path so we can import from src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from src.core.datasets import DatasetRegistry
from src.utils.logger import setup_logger

logger = setup_logger("manage_state")


def get_services():
    """Initialize database services."""
    clickhouse = ClickHouseService(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "443")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "beacon_chain"),
        secure=os.getenv("CH_SECURE", "True").lower() == "true",
        verify=os.getenv("CH_VERIFY", "False").lower() == "true"
    )
    
    state_manager = StateManager(clickhouse)
    return clickhouse, state_manager


@click.group()
def cli():
    """Beacon indexer state management tools."""
    pass


@cli.command()
@click.option('--mode', default='historical', help='Mode to check (historical/continuous)')
@click.option('--scraper', help='Filter by scraper ID')
def progress(mode, scraper):
    """Show indexing progress."""
    _, state_manager = get_services()
    
    progress_data = state_manager.get_progress_summary(mode)
    
    if not progress_data:
        click.echo("No progress data found.")
        return
    
    # Filter by scraper if specified
    if scraper:
        progress_data = {k: v for k, v in progress_data.items() if k.startswith(f"{scraper}_")}
    
    # Display progress
    total_completed = 0
    total_ranges = 0
    total_rows = 0
    
    for dataset, stats in sorted(progress_data.items()):
        click.echo(f"\n{dataset}:")
        click.echo(f"  Total ranges: {stats['total_ranges']:,}")
        click.echo(f"  Completed: {stats['completed_ranges']:,}")
        click.echo(f"  Processing: {stats['processing_ranges']:,}")
        click.echo(f"  Failed: {stats['failed_ranges']:,}")
        click.echo(f"  Pending: {stats['pending_ranges']:,}")
        click.echo(f"  Rows indexed: {stats['total_rows_indexed']:,}")
        
        completion_pct = (stats['completed_ranges'] / stats['total_ranges'] * 100) if stats['total_ranges'] > 0 else 0
        click.echo(f"  Completion: {completion_pct:.1f}%")
        
        total_completed += stats['completed_ranges']
        total_ranges += stats['total_ranges']
        total_rows += stats['total_rows_indexed']
    
    click.echo(f"\nOverall:")
    click.echo(f"  Total ranges: {total_ranges:,}")
    click.echo(f"  Completed: {total_completed:,}")
    click.echo(f"  Total rows: {total_rows:,}")
    
    overall_completion = (total_completed / total_ranges * 100) if total_ranges > 0 else 0
    click.echo(f"  Overall completion: {overall_completion:.1f}%")


@cli.command()
@click.option('--mode', default='historical', help='Mode to check')
@click.option('--scraper', help='Specific scraper to check')
@click.option('--min-slot', type=int, default=0, help='Minimum slot')
@click.option('--max-slot', type=int, help='Maximum slot')
@click.option('--create-ranges', is_flag=True, help='Create ranges for gaps')
def gaps(mode, scraper, min_slot, max_slot, create_ranges):
    """Find and optionally fill gaps in indexed data."""
    clickhouse, state_manager = get_services()
    
    # Get datasets
    where_clause = f"AND dataset LIKE '{scraper}_%'" if scraper else ""
    
    query = f"""
    SELECT DISTINCT dataset
    FROM indexing_state
    WHERE mode = %(mode)s
    {where_clause}
    """
    
    datasets = clickhouse.execute(query, {'mode': mode})
    
    # Convert result to list
    if hasattr(datasets, '__iter__') and not isinstance(datasets, list):
        datasets = list(datasets)
    
    total_gaps = 0
    for row in datasets:
        dataset = row['dataset']
        # Find gaps
        gaps = state_manager.find_gaps(dataset, min_slot, max_slot, mode)
        
        if gaps:
            click.echo(f"\n{dataset}:")
            for gap_start, gap_end in gaps:
                gap_size = gap_end - gap_start
                click.echo(f"  Gap: {gap_start:,} - {gap_end:,} ({gap_size:,} slots)")
                
            total_gaps += len(gaps)
            
            if create_ranges:
                # Create only the first range for each gap
                created = 0
                for gap_start, gap_end in gaps:
                    # Create first range of the gap
                    range_end = min(gap_start + state_manager.range_size, gap_end)
                    
                    # Check if range exists
                    check_query = """
                    SELECT 1 FROM indexing_state
                    WHERE mode = %(mode)s
                      AND dataset = %(dataset)s
                      AND start_slot = %(start_slot)s
                      AND end_slot = %(end_slot)s
                    LIMIT 1
                    """
                    
                    check_result = clickhouse.execute(check_query, {
                        'mode': mode,
                        'dataset': dataset,
                        'start_slot': gap_start,
                        'end_slot': range_end
                    })
                    
                    # Convert result to list
                    if hasattr(check_result, '__iter__') and not isinstance(check_result, list):
                        check_result = list(check_result)
                    
                    if not check_result:
                        insert_query = """
                        INSERT INTO indexing_state
                        (mode, dataset, start_slot, end_slot, status, version)
                        VALUES
                        (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'pending', now())
                        """
                        clickhouse.execute(insert_query, {
                            'mode': mode,
                            'dataset': dataset,
                            'start_slot': gap_start,
                            'end_slot': range_end
                        })
                        created += 1
                        
                click.echo(f"  Created {created} initial ranges for gaps")
    
    if total_gaps == 0:
        click.echo("\nNo gaps found!")
    else:
        click.echo(f"\nTotal gaps: {total_gaps}")


@cli.command()
@click.option('--mode', default='historical', help='Mode to reset')
@click.option('--scraper', help='Specific scraper to reset')
@click.option('--max-attempts', type=int, default=3, help='Reset ranges with fewer attempts than this')
@click.confirmation_option(prompt='Are you sure you want to reset failed ranges?')
def reset_failed(mode, scraper, max_attempts):
    """Reset failed ranges to pending status."""
    clickhouse, _ = get_services()
    
    where_clause = f"AND dataset LIKE '{scraper}_%'" if scraper else ""
    
    # Find failed ranges
    query = f"""
    SELECT dataset, start_slot, end_slot, attempt_count
    FROM indexing_state
    WHERE mode = %(mode)s
      AND status = 'failed'
      AND attempt_count < %(max_attempts)s
      {where_clause}
    ORDER BY dataset, start_slot
    """
    
    failed_ranges = clickhouse.execute(query, {
        'mode': mode,
        'max_attempts': max_attempts
    })
    
    # Convert result to list
    if hasattr(failed_ranges, '__iter__') and not isinstance(failed_ranges, list):
        failed_ranges = list(failed_ranges)
    
    if not failed_ranges:
        click.echo("No failed ranges found to reset.")
        return
    
    # Reset each range
    reset_count = 0
    for row in failed_ranges:
        try:
            insert_query = """
            INSERT INTO indexing_state
            (mode, dataset, start_slot, end_slot, status, version)
            VALUES
            (%(mode)s, %(dataset)s, %(start_slot)s, %(end_slot)s, 'pending', now())
            """
            clickhouse.execute(insert_query, {
                'mode': mode,
                'dataset': row['dataset'],
                'start_slot': row['start_slot'],
                'end_slot': row['end_slot']
            })
            reset_count += 1
            
            click.echo(f"Reset {row['dataset']} {row['start_slot']}-{row['end_slot']} (was {row['attempt_count']} attempts)")
            
        except Exception as e:
            click.echo(f"Error resetting range: {e}")
    
    click.echo(f"\nReset {reset_count} failed ranges to pending.")


@cli.command()
@click.option('--mode', default='historical', help='Mode to check')
@click.option('--timeout', type=int, default=30, help='Minutes before job is considered stale')
def reset_stale(mode, timeout):
    """Reset stale processing jobs."""
    _, state_manager = get_services()
    
    # Update stale timeout
    state_manager.stale_timeout = timeout
    
    reset_count = state_manager.reset_stale_jobs(mode)
    
    click.echo(f"Reset {reset_count} stale jobs to pending status.")


@cli.command()
@click.argument('scraper')
@click.argument('table')
@click.argument('start_slot', type=int)
@click.argument('end_slot', type=int)
@click.option('--mode', default='historical', help='Mode for ranges')
@click.option('--range-size', type=int, help='Override default range size')
def create_ranges(scraper, table, start_slot, end_slot, mode, range_size):
    """Manually create ranges for a scraper/table combination."""
    _, state_manager = get_services()
    
    if range_size:
        state_manager.range_size = range_size
    
    dataset = f"{scraper}_{table}"
    created = 0
    
    current = start_slot
    while current < end_slot:
        range_end = min(current + state_manager.range_size, end_slot)
        
        if state_manager.create_range(mode, dataset, current, range_end):
            created += 1
            
        current = range_end
    
    click.echo(f"Created {created} ranges for {dataset}")


@cli.command()
@click.option('--mode', default='historical', help='Mode to clean')
@click.option('--days', type=int, default=7, help='Delete entries older than this many days')
@click.confirmation_option(prompt='Are you sure you want to clean old entries?')
def cleanup(mode, days):
    """Clean up old state entries."""
    clickhouse, _ = get_services()
    
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Count entries to delete
    count_query = """
    SELECT COUNT(*) as count
    FROM indexing_state
    WHERE mode = %(mode)s
      AND created_at < %(cutoff_date)s
    """
    
    result = clickhouse.execute(count_query, {
        'mode': mode,
        'cutoff_date': cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
    })
    
    # Convert result to list
    if hasattr(result, '__iter__') and not isinstance(result, list):
        result = list(result)
        
    count = result[0]['count'] if result else 0
    
    if count == 0:
        click.echo("No old entries to clean up.")
        return
    
    click.echo(f"Found {count:,} entries older than {days} days.")
    
    # Delete old entries
    delete_query = """
    ALTER TABLE indexing_state
    DELETE WHERE mode = %(mode)s
      AND created_at < %(cutoff_date)s
    """
    
    try:
        clickhouse.execute(delete_query, {
            'mode': mode,
            'cutoff_date': cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        })
        click.echo(f"Deleted {count:,} old entries.")
    except Exception as e:
        click.echo(f"Error deleting entries: {e}")


@cli.command()
@click.option('--mode', default='historical', help='Mode to check')
def stats(mode):
    """Show detailed statistics."""
    clickhouse, state_manager = get_services()
    
    # Get overall stats
    query = """
    SELECT 
        COUNT(DISTINCT dataset) as num_datasets,
        COUNT(*) as total_entries,
        MIN(created_at) as first_entry,
        MAX(created_at) as last_entry,
        COUNT(DISTINCT worker_id) as unique_workers
    FROM indexing_state
    WHERE mode = %(mode)s
    """
    
    result = clickhouse.execute(query, {'mode': mode})
    
    # Convert result to list
    if hasattr(result, '__iter__') and not isinstance(result, list):
        result = list(result)
        
    if result:
        row = result[0]
        click.echo(f"\nOverall Statistics ({mode} mode):")
        click.echo(f"  Datasets: {row['num_datasets']}")
        click.echo(f"  Total entries: {row['total_entries']:,}")
        click.echo(f"  First entry: {row['first_entry']}")
        click.echo(f"  Last entry: {row['last_entry']}")
        click.echo(f"  Unique workers: {row['unique_workers']}")
    
    # Get status distribution
    query = """
    WITH latest_status AS (
        SELECT DISTINCT ON (dataset, start_slot, end_slot)
            status
        FROM indexing_state
        WHERE mode = %(mode)s
        ORDER BY dataset, start_slot, end_slot, version DESC
    )
    SELECT status, COUNT(*) as count
    FROM latest_status
    GROUP BY status
    ORDER BY count DESC
    """
    
    result = clickhouse.execute(query, {'mode': mode})
    
    # Convert result to list
    if hasattr(result, '__iter__') and not isinstance(result, list):
        result = list(result)
        
    if result:
        click.echo(f"\nStatus Distribution:")
        for row in result:
            click.echo(f"  {row['status']}: {row['count']:,}")


if __name__ == "__main__":
    cli()