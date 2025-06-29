#!/usr/bin/env python3
"""
Real-time performance monitoring dashboard for beacon indexer.
"""
import asyncio
import sys
import os
from datetime import datetime, timedelta
from collections import deque
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.services.clickhouse_service import ClickHouseService
from src.services.state_manager import StateManager
from rich.console import Console
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.live import Live
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn

console = Console()

class PerformanceMonitor:
    def __init__(self, clickhouse: ClickHouseService, state_manager: StateManager):
        self.clickhouse = clickhouse
        self.state_manager = state_manager
        self.rates = {}  # Track processing rates
        self.history = {}  # Track historical data
        
    async def get_current_stats(self):
        """Get current processing statistics."""
        # Get processing ranges
        processing_query = """
        SELECT 
            scraper_id,
            table_name,
            COUNT(*) as processing_count,
            MIN(start_slot) as min_slot,
            MAX(end_slot) as max_slot,
            SUM(end_slot - start_slot) as total_slots
        FROM indexing_state
        WHERE status = 'processing'
        GROUP BY scraper_id, table_name
        """
        
        processing = self.clickhouse.execute(processing_query)
        
        # Get recent completions (last 5 minutes)
        completed_query = """
        SELECT 
            scraper_id,
            table_name,
            COUNT(*) as completed_count,
            SUM(rows_indexed) as total_rows,
            SUM(end_slot - start_slot) as total_slots,
            AVG(end_slot - start_slot) as avg_range_size
        FROM indexing_state
        WHERE status = 'completed'
          AND completed_at >= now() - INTERVAL 5 MINUTE
        GROUP BY scraper_id, table_name
        """
        
        completed = self.clickhouse.execute(completed_query)
        
        # Get worker status
        worker_query = """
        SELECT 
            worker_id,
            COUNT(*) as active_ranges,
            MIN(started_at) as oldest_start
        FROM indexing_state
        WHERE status = 'processing'
          AND worker_id != ''
        GROUP BY worker_id
        """
        
        workers = self.clickhouse.execute(worker_query)
        
        return {
            'processing': processing,
            'completed': completed,
            'workers': workers
        }
    
    def calculate_rates(self, stats):
        """Calculate processing rates."""
        for row in stats['completed']:
            key = f"{row['scraper_id']}:{row['table_name']}"
            
            # Calculate slots per second
            slots_per_sec = row['total_slots'] / 300  # 5 minutes
            rows_per_sec = row['total_rows'] / 300 if row['total_rows'] else 0
            
            self.rates[key] = {
                'slots_per_sec': slots_per_sec,
                'rows_per_sec': rows_per_sec
            }
    
    def create_dashboard(self, stats):
        """Create the monitoring dashboard."""
        layout = Layout()
        
        # Main sections
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=10)
        )
        
        # Header
        header_text = f"[bold cyan]Beacon Indexer Performance Monitor[/bold cyan]\n"
        header_text += f"[dim]{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}[/dim]"
        layout["header"].update(Panel(header_text, style="cyan"))
        
        # Body sections
        layout["body"].split_row(
            Layout(name="processing"),
            Layout(name="rates")
        )
        
        # Processing table
        proc_table = Table(title="Currently Processing", show_header=True)
        proc_table.add_column("Scraper", style="cyan")
        proc_table.add_column("Table", style="cyan")
        proc_table.add_column("Ranges", justify="right")
        proc_table.add_column("Slots", justify="right")
        proc_table.add_column("Range", justify="center")
        
        for row in stats['processing']:
            proc_table.add_row(
                row['scraper_id'],
                row['table_name'],
                str(row['processing_count']),
                f"{row['total_slots']:,}",
                f"{row['min_slot']:,} - {row['max_slot']:,}"
            )
        
        layout["processing"].update(Panel(proc_table, title="Active Processing"))
        
        # Rates table
        rates_table = Table(title="Processing Rates (5 min avg)", show_header=True)
        rates_table.add_column("Scraper", style="cyan")
        rates_table.add_column("Table", style="cyan")
        rates_table.add_column("Slots/sec", justify="right", style="green")
        rates_table.add_column("Rows/sec", justify="right", style="green")
        
        for key, rate in self.rates.items():
            scraper_id, table_name = key.split(':', 1)
            rates_table.add_row(
                scraper_id,
                table_name,
                f"{rate['slots_per_sec']:.2f}",
                f"{rate['rows_per_sec']:.0f}"
            )
        
        layout["rates"].update(Panel(rates_table, title="Performance"))
        
        # Workers table
        workers_table = Table(title="Active Workers", show_header=True)
        workers_table.add_column("Worker ID", style="cyan")
        workers_table.add_column("Active Ranges", justify="right")
        workers_table.add_column("Running Time", justify="right")
        
        for row in stats['workers']:
            runtime = datetime.now() - row['oldest_start']
            runtime_str = str(runtime).split('.')[0]  # Remove microseconds
            
            workers_table.add_row(
                row['worker_id'],
                str(row['active_ranges']),
                runtime_str
            )
        
        layout["footer"].update(Panel(workers_table, title="Workers"))
        
        return layout
    
    async def run(self):
        """Run the monitoring dashboard."""
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                try:
                    stats = await self.get_current_stats()
                    self.calculate_rates(stats)
                    dashboard = self.create_dashboard(stats)
                    live.update(dashboard)
                    await asyncio.sleep(5)  # Update every 5 seconds
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    await asyncio.sleep(5)

async def main():
    clickhouse = ClickHouseService()
    state_manager = StateManager(clickhouse)
    monitor = PerformanceMonitor(clickhouse, state_manager)
    
    console.print("[green]Starting performance monitor... Press Ctrl+C to exit[/green]")
    await monitor.run()

if __name__ == "__main__":
    asyncio.run(main())