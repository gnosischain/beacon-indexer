"""Backfill operation to populate indexing_state from existing data."""
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.utils.logger import logger


class BackfillOperation(OperationMode):
    """Operation to backfill indexing_state from existing data."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.datasets = self.config.get('datasets', [])
        self.chunk_size = self.config.get('chunk_size', 100000)
        self.force = self.config.get('force', False)
        
    def validate_config(self) -> None:
        """Validate configuration."""
        if self.chunk_size < 1000:
            raise ValueError("chunk_size must be at least 1000")
            
    async def execute(self) -> Dict[str, Any]:
        """Execute backfill operation."""
        start_time = time.time()
        results = {}
        
        logger.info(
            f"Starting backfill operation: "
            f"datasets={self.datasets or 'all'}, "
            f"chunk_size={self.chunk_size}, "
            f"force={self.force}"
        )
        
        # Get datasets to backfill
        datasets_to_backfill = self._get_datasets_to_backfill()
        
        # Process each dataset
        for dataset_name in datasets_to_backfill:
            logger.info(f"Backfilling {dataset_name}...")
            dataset_result = await self._backfill_dataset(dataset_name)
            results[dataset_name] = dataset_result
            
        # Generate summary
        total_ranges = sum(r['ranges_created'] for r in results.values())
        total_rows = sum(r['total_rows'] for r in results.values())
        
        return {
            "operation": "backfill",
            "duration": time.time() - start_time,
            "datasets_processed": len(results),
            "total_ranges_created": total_ranges,
            "total_rows": total_rows,
            "details": results
        }
        
    def _get_datasets_to_backfill(self) -> List[str]:
        """Get list of datasets to backfill."""
        if self.datasets:
            # Validate requested datasets
            registry = self.dataset_registry
            valid_datasets = []
            
            for dataset_name in self.datasets:
                dataset = registry.get_dataset(dataset_name)
                if dataset:
                    valid_datasets.append(dataset_name)
                else:
                    logger.warning(f"Unknown dataset: {dataset_name}")
                    
            return valid_datasets
        else:
            # Backfill all continuous datasets
            datasets = self.dataset_registry.get_all_datasets()
            return [d.name for d in datasets if d.is_continuous]
            
    async def _backfill_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """Backfill indexing_state for a single dataset."""
        dataset = self.dataset_registry.get_dataset(dataset_name)
        if not dataset:
            return {
                "ranges_created": 0,
                "total_rows": 0,
                "error": "Dataset not found"
            }
            
        # Get the primary table for this dataset
        primary_table = dataset.tables[0]
        
        # Get existing ranges from state
        existing_ranges = await self._get_existing_ranges(dataset_name)
        
        # Find data ranges in the table
        data_ranges = await self._find_data_ranges(primary_table, dataset.is_sparse)
        
        # Create missing state entries
        ranges_created = 0
        total_rows = 0
        
        for start_slot, end_slot, row_count in data_ranges:
            # Check if range already exists in state
            range_key = (start_slot, end_slot)
            
            if not self.force and range_key in existing_ranges:
                logger.debug(f"Range {start_slot}-{end_slot} already exists in state")
                continue
                
            # Create state entry
            if self._create_backfill_entry(dataset_name, start_slot, end_slot, row_count):
                ranges_created += 1
                total_rows += row_count
                
                if ranges_created % 100 == 0:
                    logger.info(f"Created {ranges_created} ranges for {dataset_name}")
                    
        logger.info(
            f"Backfilled {dataset_name}: "
            f"{ranges_created} ranges created, {total_rows} total rows"
        )
        
        return {
            "ranges_created": ranges_created,
            "total_rows": total_rows,
            "min_slot": min(r[0] for r in data_ranges) if data_ranges else 0,
            "max_slot": max(r[1] for r in data_ranges) if data_ranges else 0
        }
        
    async def _get_existing_ranges(self, dataset_name: str) -> set:
        """Get existing ranges from indexing_state."""
        try:
            query = f"""
            SELECT DISTINCT start_slot, end_slot
            FROM {self.clickhouse.database}.indexing_state
            WHERE dataset = '{dataset_name}'
              AND status = 'completed'
            """
            
            result = self.clickhouse.execute(query)
            
            existing = set()
            for row in result:
                existing.add((row['start_slot'], row['end_slot']))
                
            logger.info(f"Found {len(existing)} existing ranges for {dataset_name}")
            return existing
            
        except Exception as e:
            logger.error(f"Error getting existing ranges: {e}")
            return set()
            
    async def _find_data_ranges(self, table_name: str, is_sparse: bool) -> List[Tuple[int, int, int]]:
        """Find data ranges in a table."""
        ranges = []
        range_size = self.state_manager.range_size
        
        try:
            # Get min and max slots
            bounds_query = f"""
            SELECT MIN(slot) as min_slot, MAX(slot) as max_slot
            FROM {self.clickhouse.database}.{table_name}
            """
            
            bounds_result = self.clickhouse.execute(bounds_query)
            
            if not bounds_result or bounds_result[0]['min_slot'] is None:
                logger.info(f"No data found in {table_name}")
                return []
                
            min_slot = bounds_result[0]['min_slot']
            max_slot = bounds_result[0]['max_slot']
            
            logger.info(f"Table {table_name} has data from slot {min_slot} to {max_slot}")
            
            # Process in chunks
            current = min_slot
            
            while current <= max_slot:
                chunk_end = min(current + self.chunk_size, max_slot + 1)
                
                # Get ranges with data in this chunk
                if is_sparse:
                    # For sparse data, find actual ranges with data
                    ranges_query = f"""
                    SELECT 
                        intDiv(slot, {range_size}) * {range_size} as range_start,
                        (intDiv(slot, {range_size}) + 1) * {range_size} as range_end,
                        COUNT(*) as row_count
                    FROM {self.clickhouse.database}.{table_name}
                    WHERE slot >= {current} AND slot < {chunk_end}
                    GROUP BY range_start
                    ORDER BY range_start
                    """
                else:
                    # For continuous data, create ranges for all slots
                    ranges_query = f"""
                    WITH ranges AS (
                        SELECT 
                            number * {range_size} as range_start,
                            (number + 1) * {range_size} as range_end
                        FROM numbers({current // range_size}, {(chunk_end - current) // range_size + 1})
                    )
                    SELECT 
                        r.range_start,
                        r.range_end,
                        COUNT(t.slot) as row_count
                    FROM ranges r
                    LEFT JOIN {self.clickhouse.database}.{table_name} t
                        ON t.slot >= r.range_start AND t.slot < r.range_end
                    WHERE r.range_start >= {current} AND r.range_start < {chunk_end}
                    GROUP BY r.range_start, r.range_end
                    HAVING row_count > 0
                    ORDER BY r.range_start
                    """
                    
                chunk_result = self.clickhouse.execute(ranges_query)
                
                for row in chunk_result:
                    ranges.append((
                        row['range_start'],
                        row['range_end'],
                        row['row_count']
                    ))
                    
                current = chunk_end
                
                if len(ranges) % 1000 == 0:
                    logger.info(f"Found {len(ranges)} ranges so far...")
                    
            logger.info(f"Found {len(ranges)} total ranges with data in {table_name}")
            return ranges
            
        except Exception as e:
            logger.error(f"Error finding data ranges: {e}")
            return []
            
    def _create_backfill_entry(self, dataset_name: str, start_slot: int, 
                              end_slot: int, row_count: int) -> bool:
        """Create a backfill entry in indexing_state."""
        try:
            insert_query = f"""
            INSERT INTO {self.clickhouse.database}.indexing_state
            (mode, dataset, start_slot, end_slot, status, rows_indexed, 
             batch_id, created_at, completed_at, version)
            VALUES
            ('historical', '{dataset_name}', {start_slot}, {end_slot}, 
             'completed', {row_count}, 'backfill', now(), now(), now())
            """
            
            self.clickhouse.execute(insert_query)
            return True
            
        except Exception as e:
            logger.error(f"Error creating backfill entry: {e}")
            return False