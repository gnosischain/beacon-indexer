"""Consolidate operation to merge adjacent completed ranges."""
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry
from src.utils.logger import logger


class ConsolidateOperation(OperationMode):
    """Operation to consolidate fragmented ranges in indexing_state."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.datasets = self.config.get('datasets', [])
        self.dry_run = self.config.get('dry_run', False)
        
    def validate_config(self) -> None:
        """Validate configuration."""
        pass  # No specific validation needed
        
    async def execute(self) -> Dict[str, Any]:
        """Execute consolidation operation."""
        start_time = time.time()
        results = {}
        
        logger.info(
            f"Starting consolidation operation: "
            f"datasets={self.datasets or 'all'}, "
            f"dry_run={self.dry_run}"
        )
        
        # Get datasets to consolidate
        datasets_to_consolidate = self._get_datasets_to_consolidate()
        
        # Process each dataset
        for dataset_name in datasets_to_consolidate:
            logger.info(f"Consolidating {dataset_name}...")
            dataset_result = await self._consolidate_dataset(dataset_name)
            results[dataset_name] = dataset_result
            
        # Generate summary
        total_original = sum(r['original_count'] for r in results.values())
        total_consolidated = sum(r['consolidated_count'] for r in results.values())
        total_merged = sum(r['ranges_merged'] for r in results.values())
        
        return {
            "operation": "consolidate",
            "duration": time.time() - start_time,
            "dry_run": self.dry_run,
            "datasets_processed": len(results),
            "total_original_ranges": total_original,
            "total_consolidated_ranges": total_consolidated,
            "total_ranges_merged": total_merged,
            "details": results
        }
        
    def _get_datasets_to_consolidate(self) -> List[str]:
        """Get list of datasets to consolidate."""
        if self.datasets:
            return self.datasets
        else:
            # Consolidate all continuous datasets
            datasets = self.dataset_registry.get_all_datasets()
            return [d.name for d in datasets if d.is_continuous]
            
    async def _consolidate_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """Consolidate ranges for a single dataset."""
        # Get all completed ranges
        ranges = await self._get_completed_ranges(dataset_name)
        
        if len(ranges) < 2:
            return {
                "original_count": len(ranges),
                "consolidated_count": len(ranges),
                "ranges_merged": 0
            }
            
        original_count = len(ranges)
        
        # Sort by start slot
        ranges.sort(key=lambda r: r['start_slot'])
        
        # Find groups of adjacent ranges
        groups = self._find_adjacent_groups(ranges)
        
        # Consolidate each group
        ranges_merged = 0
        
        for group in groups:
            if len(group) > 1:
                if not self.dry_run:
                    merged = await self._consolidate_group(dataset_name, group)
                    if merged:
                        ranges_merged += len(group) - 1
                else:
                    # Dry run - just count
                    ranges_merged += len(group) - 1
                    logger.info(
                        f"Would consolidate {len(group)} ranges: "
                        f"{group[0]['start_slot']}-{group[-1]['end_slot']}"
                    )
                    
        consolidated_count = original_count - ranges_merged
        
        logger.info(
            f"{dataset_name}: {original_count} -> {consolidated_count} ranges "
            f"({ranges_merged} merged)"
        )
        
        return {
            "original_count": original_count,
            "consolidated_count": consolidated_count,
            "ranges_merged": ranges_merged
        }
        
    async def _get_completed_ranges(self, dataset_name: str) -> List[Dict]:
        """Get all completed ranges for a dataset."""
        try:
            query = f"""
            SELECT 
                start_slot,
                end_slot,
                rows_indexed,
                created_at,
                completed_at
            FROM {self.clickhouse.database}.indexing_state
            WHERE dataset = '{dataset_name}'
              AND status = 'completed'
            ORDER BY start_slot
            """
            
            result = self.clickhouse.execute(query)
            
            ranges = []
            for row in result:
                ranges.append({
                    'start_slot': row['start_slot'],
                    'end_slot': row['end_slot'],
                    'rows_indexed': row['rows_indexed'] or 0,
                    'created_at': row['created_at'],
                    'completed_at': row['completed_at']
                })
                
            return ranges
            
        except Exception as e:
            logger.error(f"Error getting completed ranges: {e}")
            return []
            
    def _find_adjacent_groups(self, ranges: List[Dict]) -> List[List[Dict]]:
        """Find groups of adjacent ranges that can be merged."""
        groups = []
        current_group = []
        range_size = self.state_manager.range_size
        
        for i, range_info in enumerate(ranges):
            if not current_group:
                current_group = [range_info]
            else:
                # Check if this range is adjacent to the last in group
                last_range = current_group[-1]
                
                # Ranges are adjacent if end of last == start of current
                # and they maintain the fixed range size alignment
                if (last_range['end_slot'] == range_info['start_slot'] and
                    range_info['start_slot'] % range_size == 0 and
                    range_info['end_slot'] % range_size == 0):
                    current_group.append(range_info)
                else:
                    # Start new group
                    if len(current_group) > 1:
                        groups.append(current_group)
                    current_group = [range_info]
                    
        # Add final group
        if len(current_group) > 1:
            groups.append(current_group)
            
        return groups
        
    async def _consolidate_group(self, dataset_name: str, group: List[Dict]) -> bool:
        """Consolidate a group of adjacent ranges."""
        if len(group) < 2:
            return False
            
        try:
            # Calculate consolidated range
            start_slot = group[0]['start_slot']
            end_slot = group[-1]['end_slot']
            total_rows = sum(r['rows_indexed'] for r in group)
            
            # Use earliest created_at and latest completed_at
            created_at = min(r['created_at'] for r in group)
            completed_at = max(r['completed_at'] for r in group)
            
            logger.info(
                f"Consolidating {len(group)} ranges into {start_slot}-{end_slot} "
                f"({total_rows} total rows)"
            )
            
            # Delete all existing entries for this group
            for range_info in group:
                delete_query = f"""
                ALTER TABLE {self.clickhouse.database}.indexing_state
                DELETE WHERE dataset = '{dataset_name}'
                  AND start_slot = {range_info['start_slot']}
                  AND end_slot = {range_info['end_slot']}
                  AND status = 'completed'
                """
                
                self.clickhouse.execute(delete_query)
                
            # Insert consolidated entry
            insert_query = f"""
            INSERT INTO {self.clickhouse.database}.indexing_state
            (mode, dataset, start_slot, end_slot, status, rows_indexed,
             batch_id, created_at, completed_at, version)
            VALUES
            ('historical', '{dataset_name}', {start_slot}, {end_slot},
             'completed', {total_rows}, 'consolidated',
             '{created_at.isoformat()}', '{completed_at.isoformat()}', now())
            """
            
            self.clickhouse.execute(insert_query)
            
            return True
            
        except Exception as e:
            logger.error(f"Error consolidating group: {e}")
            return False