"""Data validation operation."""
import asyncio
import time
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from collections import defaultdict

from src.core.operations import OperationMode
from src.core.datasets import DatasetRegistry, Dataset
from src.core.state import StateStatus
from src.utils.logger import logger


class ValidateOperation(OperationMode):
    """Operation to validate data integrity and consistency."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataset_registry = DatasetRegistry()
        self.start_slot = self.config.get('start_slot', 0)
        self.end_slot = self.config.get('end_slot')
        self.datasets = self.config.get('datasets', [])
        self.validation_types = self.config.get('validation_types', ['completeness', 'consistency'])
        self.fix_issues = self.config.get('fix_issues', False)
        self.sample_rate = self.config.get('sample_rate', 0.1)  # Sample 10% by default
        
    def validate_config(self) -> None:
        """Validate configuration."""
        if self.start_slot < 0:
            raise ValueError("start_slot must be non-negative")
            
        if self.end_slot is not None and self.end_slot <= self.start_slot:
            raise ValueError("end_slot must be greater than start_slot")
            
        valid_types = {'completeness', 'consistency', 'relationships', 'timestamps', 'beacon_specific'}
        for vtype in self.validation_types:
            if vtype not in valid_types:
                raise ValueError(f"Invalid validation type: {vtype}")
                
        if not 0.0 < self.sample_rate <= 1.0:
            raise ValueError("sample_rate must be between 0 and 1")
                
    async def execute(self) -> Dict[str, Any]:
        """Execute validation operation."""
        start_time = time.time()
        issues_found = defaultdict(list)
        issues_fixed = defaultdict(int)
        
        # Determine end slot if not specified
        if self.end_slot is None:
            await self._determine_end_slot()
            
        logger.info(
            f"Starting validation: slots {self.start_slot}-{self.end_slot}, "
            f"types={self.validation_types}, fix_issues={self.fix_issues}"
        )
        
        # Get datasets to validate
        datasets_to_validate = self._get_datasets_to_validate()
        
        # Run each validation type
        for validation_type in self.validation_types:
            logger.info(f"Running {validation_type} validation...")
            
            if validation_type == 'completeness':
                type_issues = await self._validate_completeness(datasets_to_validate)
            elif validation_type == 'consistency':
                type_issues = await self._validate_consistency(datasets_to_validate)
            elif validation_type == 'relationships':
                type_issues = await self._validate_relationships(datasets_to_validate)
            elif validation_type == 'timestamps':
                type_issues = await self._validate_timestamps(datasets_to_validate)
            elif validation_type == 'beacon_specific':
                type_issues = await self._validate_beacon_specific(datasets_to_validate)
                
            # Collect issues
            for dataset, dataset_issues in type_issues.items():
                issues_found[dataset].extend(dataset_issues)
                
        # Fix issues if requested
        if self.fix_issues and issues_found:
            fixed = await self._fix_issues(issues_found)
            issues_fixed.update(fixed)
            
        # Generate report
        report = self._generate_report(issues_found, issues_fixed)
        
        return {
            "operation": "validate",
            "duration": time.time() - start_time,
            "start_slot": self.start_slot,
            "end_slot": self.end_slot,
            "datasets_validated": len(datasets_to_validate),
            "total_issues": sum(len(issues) for issues in issues_found.values()),
            "issues_fixed": sum(issues_fixed.values()),
            "report": report
        }
        
    async def _determine_end_slot(self) -> None:
        """Determine end slot from existing data."""
        progress = self.state_manager.get_progress_summary("historical")
        
        if progress:
            max_slot = max(p.get('highest_slot', 0) for p in progress.values())
            if max_slot > 0:
                self.end_slot = max_slot
                return
                
        # Fall back to chain head if beacon API available
        if self.beacon_api:
            try:
                latest_header = await self.beacon_api.get_block_header("head")
                self.end_slot = int(latest_header["header"]["message"]["slot"])
            except Exception as e:
                logger.error(f"Could not get chain head: {e}")
                self.end_slot = 10000000  # Default to a large number
        else:
            self.end_slot = 10000000
            
    def _get_datasets_to_validate(self) -> List[str]:
        """Get list of datasets to validate."""
        if self.datasets:
            return self.datasets
        else:
            # Validate all continuous datasets
            datasets = self.dataset_registry.get_all_datasets()
            return [d.name for d in datasets if d.is_continuous]
            
    async def _validate_completeness(self, datasets: List[str]) -> Dict[str, List[Dict]]:
        """Validate data completeness."""
        issues = defaultdict(list)
        
        for dataset_name in datasets:
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            logger.info(f"Checking completeness for {dataset_name}...")
            
            # Check for gaps in state
            gaps = self.state_manager.find_gaps(
                "historical", dataset_name,
                self.start_slot, self.end_slot
            )
            
            for start, end in gaps:
                issues[dataset_name].append({
                    "type": "gap",
                    "severity": "high",
                    "description": f"Missing data for slots {start}-{end}",
                    "start_slot": start,
                    "end_slot": end
                })
                
            # Check for incomplete ranges
            incomplete = await self._find_incomplete_ranges(dataset_name)
            
            for range_info in incomplete:
                issues[dataset_name].append({
                    "type": "incomplete_range",
                    "severity": "medium",
                    "description": f"Incomplete range {range_info['start']}-{range_info['end']}",
                    "start_slot": range_info['start'],
                    "end_slot": range_info['end'],
                    "status": range_info['status']
                })
                
            # Check actual data vs state (sample-based for large ranges)
            if not dataset.is_sparse:
                missing = await self._check_data_vs_state(dataset_name, dataset.tables[0])
                
                for slot_range in missing:
                    issues[dataset_name].append({
                        "type": "data_state_mismatch",
                        "severity": "high",
                        "description": f"State shows completed but data missing for {slot_range}",
                        "slot_range": slot_range
                    })
                    
        return dict(issues)
        
    async def _validate_consistency(self, datasets: List[str]) -> Dict[str, List[Dict]]:
        """Validate data consistency."""
        issues = defaultdict(list)
        
        # Check blocks table first
        if "blocks" in datasets:
            # Check for duplicate blocks
            duplicates = await self._find_duplicate_blocks()
            
            for slot, count in duplicates:
                issues["blocks"].append({
                    "type": "duplicate_block",
                    "severity": "high",
                    "description": f"Slot {slot} has {count} blocks",
                    "slot": slot,
                    "count": count
                })
                
            # Check block sequence
            sequence_issues = await self._check_block_sequence()
            
            for issue in sequence_issues:
                issues["blocks"].append(issue)
                
        # Check dataset-specific consistency
        for dataset_name in datasets:
            dataset = self.dataset_registry.get_dataset(dataset_name)
            if not dataset:
                continue
                
            # Check row counts vs state (sample-based)
            count_issues = await self._check_row_counts(dataset_name, dataset.tables)
            
            for issue in count_issues:
                issues[dataset_name].append(issue)
                
        return dict(issues)
        
    async def _validate_relationships(self, datasets: List[str]) -> Dict[str, List[Dict]]:
        """Validate relationships between datasets."""
        issues = defaultdict(list)
        
        # Check attestations reference valid blocks
        if "attestations" in datasets and "blocks" in datasets:
            orphaned = await self._find_orphaned_attestations()
            
            for slot, count in orphaned:
                issues["attestations"].append({
                    "type": "orphaned_attestations",
                    "severity": "high",
                    "description": f"{count} attestations reference missing block at slot {slot}",
                    "slot": slot,
                    "count": count
                })
                
        # Check transactions reference valid blocks
        if "transactions" in datasets and "blocks" in datasets:
            orphaned = await self._find_orphaned_transactions()
            
            for slot, count in orphaned:
                issues["transactions"].append({
                    "type": "orphaned_transactions",
                    "severity": "high",
                    "description": f"{count} transactions reference missing block at slot {slot}",
                    "slot": slot,
                    "count": count
                })
                
        # Check withdrawals reference valid validators
        if "withdrawals" in datasets and "validators" in datasets:
            invalid = await self._find_invalid_withdrawals()
            
            for issue in invalid:
                issues["withdrawals"].append(issue)
                
        # Check deposits reference valid blocks
        if "deposits" in datasets and "blocks" in datasets:
            orphaned = await self._find_orphaned_deposits()
            
            for slot, count in orphaned:
                issues["deposits"].append({
                    "type": "orphaned_deposits",
                    "severity": "medium",
                    "description": f"{count} deposits reference missing block at slot {slot}",
                    "slot": slot,
                    "count": count
                })
                
        return dict(issues)
        
    async def _validate_timestamps(self, datasets: List[str]) -> Dict[str, List[Dict]]:
        """Validate timestamp consistency."""
        issues = defaultdict(list)
        
        # Check block timestamps are sequential
        if "blocks" in datasets:
            timestamp_issues = await self._check_block_timestamps()
            
            for issue in timestamp_issues:
                issues["blocks"].append(issue)
                
        # Check execution payload timestamps match blocks
        if "execution_payloads" in datasets and "blocks" in datasets:
            payload_issues = await self._check_execution_payload_timestamps()
            
            for issue in payload_issues:
                issues["execution_payloads"].append(issue)
                
        # Check slot_timestamp calculations
        calculation_issues = await self._check_slot_timestamp_calculations()
        
        for dataset_name, dataset_issues in calculation_issues.items():
            if dataset_name in datasets:
                issues[dataset_name].extend(dataset_issues)
            
        return dict(issues)
        
    async def _validate_beacon_specific(self, datasets: List[str]) -> Dict[str, List[Dict]]:
        """Validate beacon chain specific rules."""
        issues = defaultdict(list)
        
        # Check validator daily snapshots
        if "validators" in datasets:
            validator_issues = await self._check_validator_snapshots()
            issues["validators"].extend(validator_issues)
            
        # Check attestation aggregation bits
        if "attestations" in datasets:
            attestation_issues = await self._check_attestation_validity()
            issues["attestations"].extend(attestation_issues)
            
        # Check proposer slashing validity
        if "proposer_slashings" in datasets:
            slashing_issues = await self._check_proposer_slashings()
            issues["proposer_slashings"].extend(slashing_issues)
            
        # Check sync aggregate participation
        if "sync_aggregates" in datasets:
            sync_issues = await self._check_sync_aggregates()
            issues["sync_aggregates"].extend(sync_issues)
            
        # Check blob sidecar consistency
        if "blob_sidecars" in datasets:
            blob_issues = await self._check_blob_sidecars()
            issues["blob_sidecars"].extend(blob_issues)
            
        return dict(issues)
        
    async def _find_incomplete_ranges(self, dataset_name: str) -> List[Dict]:
        """Find ranges that are not completed."""
        try:
            query = f"""
            SELECT start_slot, end_slot, status, attempt_count
            FROM {self.clickhouse.database}.indexing_state FINAL
            WHERE mode = 'historical'
              AND dataset = '{dataset_name}'
              AND start_slot >= {self.start_slot}
              AND end_slot <= {self.end_slot}
              AND status != 'completed'
            ORDER BY start_slot
            """
            
            result = self.clickhouse.execute(query)
            
            incomplete = []
            for row in result:
                incomplete.append({
                    'start': row['start_slot'],
                    'end': row['end_slot'],
                    'status': row['status'],
                    'attempts': row['attempt_count']
                })
                
            return incomplete
            
        except Exception as e:
            logger.error(f"Error finding incomplete ranges: {e}")
            return []
            
    async def _check_data_vs_state(self, dataset_name: str, table_name: str) -> List[str]:
        """Check if data exists for completed ranges (sample-based)."""
        missing_ranges = []
        
        try:
            # Get completed ranges from state
            state_query = f"""
            SELECT start_slot, end_slot
            FROM {self.clickhouse.database}.indexing_state FINAL
            WHERE mode = 'historical'
              AND dataset = '{dataset_name}'
              AND status = 'completed'
              AND start_slot >= {self.start_slot}
              AND end_slot <= {self.end_slot}
            ORDER BY start_slot
            """
            
            state_result = self.clickhouse.execute(state_query)
            
            # Sample ranges to check
            import random
            sample_size = max(1, int(len(state_result) * self.sample_rate))
            sampled_ranges = random.sample(state_result, min(sample_size, len(state_result)))
            
            # Check each sampled range for actual data
            for row in sampled_ranges:
                start_slot = row['start_slot']
                end_slot = row['end_slot']
                
                # Check if data exists
                data_query = f"""
                SELECT COUNT(*) as count
                FROM {self.clickhouse.database}.{table_name}
                WHERE slot >= {start_slot} AND slot < {end_slot}
                """
                
                data_result = self.clickhouse.execute(data_query)
                
                if data_result[0]['count'] == 0:
                    missing_ranges.append(f"{start_slot}-{end_slot}")
                    
            return missing_ranges
            
        except Exception as e:
            logger.error(f"Error checking data vs state: {e}")
            return []
            
    async def _find_duplicate_blocks(self) -> List[Tuple[int, int]]:
        """Find slots with multiple blocks."""
        try:
            query = f"""
            SELECT slot, COUNT(*) as count
            FROM {self.clickhouse.database}.blocks
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
            GROUP BY slot
            HAVING count > 1
            ORDER BY slot
            LIMIT 100
            """
            
            result = self.clickhouse.execute(query)
            
            duplicates = [(row['slot'], row['count']) for row in result]
            return duplicates
            
        except Exception as e:
            logger.error(f"Error finding duplicate blocks: {e}")
            return []
            
    async def _check_block_sequence(self) -> List[Dict]:
        """Check for gaps in block sequence (sample-based)."""
        issues = []
        
        try:
            # Sample some ranges to check
            range_size = 10000
            current = self.start_slot
            
            while current < self.end_slot:
                range_end = min(current + range_size, self.end_slot)
                
                # Random decision to check this range
                if random.random() < self.sample_rate:
                    query = f"""
                    SELECT DISTINCT slot
                    FROM {self.clickhouse.database}.blocks
                    WHERE slot >= {current} AND slot < {range_end}
                    ORDER BY slot
                    """
                    
                    result = self.clickhouse.execute(query)
                    slots = [row['slot'] for row in result]
                    
                    # Check for gaps
                    prev_slot = None
                    for slot in slots:
                        if prev_slot is not None and slot > prev_slot + 1:
                            gap_size = slot - prev_slot - 1
                            if gap_size > 1:  # Ignore single slot gaps (common)
                                issues.append({
                                    "type": "sequence_gap",
                                    "severity": "medium" if gap_size < 10 else "high",
                                    "description": f"Gap of {gap_size} slots between {prev_slot} and {slot}",
                                    "start_slot": prev_slot + 1,
                                    "end_slot": slot - 1
                                })
                        prev_slot = slot
                
                current = range_end
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking block sequence: {e}")
            return []
            
    async def _check_row_counts(self, dataset_name: str, tables: List[str]) -> List[Dict]:
        """Check if row counts match state records (sample-based)."""
        issues = []
        
        try:
            # Get row counts from state
            state_query = f"""
            SELECT start_slot, end_slot, rows_indexed
            FROM {self.clickhouse.database}.indexing_state FINAL
            WHERE mode = 'historical'
              AND dataset = '{dataset_name}'
              AND status = 'completed'
              AND start_slot >= {self.start_slot}
              AND end_slot <= {self.end_slot}
              AND rows_indexed IS NOT NULL
              AND rows_indexed > 0
            ORDER BY rand()
            LIMIT {int(100 * self.sample_rate)}
            """
            
            state_result = self.clickhouse.execute(state_query)
            
            for row in state_result:
                start_slot = row['start_slot']
                end_slot = row['end_slot']
                expected_rows = row['rows_indexed']
                
                # Count actual rows across all tables
                actual_rows = 0
                for table in tables:
                    count_query = f"""
                    SELECT COUNT(*) as count
                    FROM {self.clickhouse.database}.{table}
                    WHERE slot >= {start_slot} AND slot < {end_slot}
                    """
                    
                    try:
                        count_result = self.clickhouse.execute(count_query)
                        actual_rows += count_result[0]['count']
                    except:
                        pass
                        
                # Check for significant mismatch (allow 10% tolerance)
                if abs(actual_rows - expected_rows) > expected_rows * 0.1:
                    issues.append({
                        "type": "row_count_mismatch",
                        "severity": "medium",
                        "description": f"Range {start_slot}-{end_slot}: expected {expected_rows} rows, found {actual_rows}",
                        "start_slot": start_slot,
                        "end_slot": end_slot,
                        "expected": expected_rows,
                        "actual": actual_rows
                    })
                    
            return issues
            
        except Exception as e:
            logger.error(f"Error checking row counts: {e}")
            return []
            
    async def _find_orphaned_attestations(self) -> List[Tuple[int, int]]:
        """Find attestations that reference non-existent blocks."""
        try:
            query = f"""
            WITH sampled_slots AS (
                SELECT DISTINCT slot
                FROM {self.clickhouse.database}.attestations
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                ORDER BY rand()
                LIMIT {int(1000 * self.sample_rate)}
            )
            SELECT a.slot, COUNT(*) as count
            FROM {self.clickhouse.database}.attestations a
            INNER JOIN sampled_slots s ON a.slot = s.slot
            LEFT JOIN {self.clickhouse.database}.blocks b ON a.slot = b.slot
            WHERE b.slot IS NULL
            GROUP BY a.slot
            ORDER BY a.slot
            """
            
            result = self.clickhouse.execute(query)
            return [(row['slot'], row['count']) for row in result]
            
        except Exception as e:
            logger.error(f"Error finding orphaned attestations: {e}")
            return []
            
    async def _find_orphaned_transactions(self) -> List[Tuple[int, int]]:
        """Find transactions that reference non-existent blocks."""
        try:
            query = f"""
            WITH sampled_slots AS (
                SELECT DISTINCT slot
                FROM {self.clickhouse.database}.transactions
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                ORDER BY rand()
                LIMIT {int(1000 * self.sample_rate)}
            )
            SELECT t.slot, COUNT(*) as count
            FROM {self.clickhouse.database}.transactions t
            INNER JOIN sampled_slots s ON t.slot = s.slot
            LEFT JOIN {self.clickhouse.database}.blocks b ON t.slot = b.slot
            WHERE b.slot IS NULL
            GROUP BY t.slot
            ORDER BY t.slot
            """
            
            result = self.clickhouse.execute(query)
            return [(row['slot'], row['count']) for row in result]
            
        except Exception as e:
            logger.error(f"Error finding orphaned transactions: {e}")
            return []
            
    async def _find_invalid_withdrawals(self) -> List[Dict]:
        """Find withdrawals with invalid validator indices."""
        issues = []
        
        try:
            # Get max validator index
            max_query = f"""
            SELECT MAX(validator_index) as max_index
            FROM {self.clickhouse.database}.validators
            """
            
            max_result = self.clickhouse.execute(max_query)
            max_validator = max_result[0]['max_index'] if max_result else 1000000
            
            # Find invalid withdrawals (sample)
            query = f"""
            SELECT slot, validator_index, COUNT(*) as count
            FROM {self.clickhouse.database}.withdrawals
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
              AND validator_index > {max_validator}
            GROUP BY slot, validator_index
            ORDER BY slot
            LIMIT 100
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                issues.append({
                    "type": "invalid_withdrawal_validator",
                    "severity": "high",
                    "description": f"Withdrawal at slot {row['slot']} references invalid validator {row['validator_index']}",
                    "slot": row['slot'],
                    "validator_index": row['validator_index']
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error finding invalid withdrawals: {e}")
            return []
            
    async def _find_orphaned_deposits(self) -> List[Tuple[int, int]]:
        """Find deposits that reference non-existent blocks."""
        try:
            query = f"""
            WITH sampled_slots AS (
                SELECT DISTINCT slot
                FROM {self.clickhouse.database}.deposits
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                ORDER BY rand()
                LIMIT {int(100 * self.sample_rate)}
            )
            SELECT d.slot, COUNT(*) as count
            FROM {self.clickhouse.database}.deposits d
            INNER JOIN sampled_slots s ON d.slot = s.slot
            LEFT JOIN {self.clickhouse.database}.blocks b ON d.slot = b.slot
            WHERE b.slot IS NULL
            GROUP BY d.slot
            ORDER BY d.slot
            """
            
            result = self.clickhouse.execute(query)
            return [(row['slot'], row['count']) for row in result]
            
        except Exception as e:
            logger.error(f"Error finding orphaned deposits: {e}")
            return []
            
    async def _check_block_timestamps(self) -> List[Dict]:
        """Check block timestamp consistency."""
        issues = []
        
        try:
            # Sample some blocks
            query = f"""
            WITH sampled AS (
                SELECT 
                    slot,
                    timestamp,
                    LAG(timestamp) OVER (ORDER BY slot) as prev_timestamp,
                    LAG(slot) OVER (ORDER BY slot) as prev_slot
                FROM {self.clickhouse.database}.blocks
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                  AND timestamp IS NOT NULL
                ORDER BY rand()
                LIMIT {int(1000 * self.sample_rate)}
            )
            SELECT * FROM sampled
            ORDER BY slot
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                if row['prev_timestamp'] and row['prev_slot']:
                    slot_diff = row['slot'] - row['prev_slot']
                    time_diff = (row['timestamp'] - row['prev_timestamp']).total_seconds()
                    
                    # Expected time difference (12s per slot for mainnet)
                    expected_diff = slot_diff * 12
                    
                    # Allow 2 second tolerance
                    if abs(time_diff - expected_diff) > 2:
                        issues.append({
                            "type": "timestamp_inconsistency",
                            "severity": "medium",
                            "description": f"Timestamp gap between slots {row['prev_slot']} and {row['slot']}: expected {expected_diff}s, got {time_diff}s",
                            "slot": row['slot'],
                            "expected_diff": expected_diff,
                            "actual_diff": time_diff
                        })
                        
            return issues
            
        except Exception as e:
            logger.error(f"Error checking block timestamps: {e}")
            return []
            
    async def _check_execution_payload_timestamps(self) -> List[Dict]:
        """Check execution payload timestamps match blocks."""
        issues = []
        
        try:
            query = f"""
            WITH sampled AS (
                SELECT 
                    b.slot,
                    b.timestamp as block_timestamp,
                    e.timestamp as exec_timestamp
                FROM {self.clickhouse.database}.blocks b
                INNER JOIN {self.clickhouse.database}.execution_payloads e
                    ON b.slot = e.slot AND b.block_root = e.block_root
                WHERE b.slot >= {self.start_slot} AND b.slot <= {self.end_slot}
                  AND b.timestamp IS NOT NULL
                  AND e.timestamp IS NOT NULL
                ORDER BY rand()
                LIMIT {int(500 * self.sample_rate)}
            )
            SELECT * FROM sampled
            WHERE abs(dateDiff('second', block_timestamp, exec_timestamp)) > 0
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                diff = abs((row['block_timestamp'] - row['exec_timestamp']).total_seconds())
                issues.append({
                    "type": "timestamp_mismatch",
                    "severity": "low" if diff < 60 else "medium",
                    "description": f"Slot {row['slot']}: block and execution payload timestamps differ by {diff}s",
                    "slot": row['slot'],
                    "difference": diff
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking execution payload timestamps: {e}")
            return []
            
    async def _check_slot_timestamp_calculations(self) -> Dict[str, List[Dict]]:
        """Check if slot_timestamp is calculated correctly."""
        issues = defaultdict(list)
        
        # Get genesis time and slots per second
        genesis_time = self.clickhouse.get_genesis_time()
        time_params = self.clickhouse.get_time_parameters()
        
        if not genesis_time or not time_params:
            logger.warning("Cannot check slot timestamps without genesis time")
            return {}
            
        seconds_per_slot = time_params.get('seconds_per_slot', 12)
        
        # Tables to check
        tables_to_check = ['blocks', 'attestations', 'validators', 'deposits', 'withdrawals']
        
        for table in tables_to_check:
            try:
                # Check if table exists and has slot_timestamp
                check_query = f"""
                SELECT COUNT(*) as count
                FROM system.columns
                WHERE database = '{self.clickhouse.database}'
                  AND table = '{table}'
                  AND name = 'slot_timestamp'
                """
                
                check_result = self.clickhouse.execute(check_query)
                if not check_result or check_result[0]['count'] == 0:
                    continue
                
                # Sample some slots
                query = f"""
                SELECT slot, slot_timestamp
                FROM {self.clickhouse.database}.{table}
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                  AND slot_timestamp IS NOT NULL
                ORDER BY rand()
                LIMIT {int(100 * self.sample_rate)}
                """
                
                result = self.clickhouse.execute(query)
                
                for row in result:
                    slot = row['slot']
                    actual_timestamp = row['slot_timestamp']
                    
                    # Calculate expected timestamp
                    expected_timestamp = datetime.fromtimestamp(
                        genesis_time + (slot * seconds_per_slot)
                    )
                    
                    # Check if they match (within 1 second)
                    diff = abs((actual_timestamp - expected_timestamp).total_seconds())
                    if diff > 1:
                        issues[table].append({
                            "type": "slot_timestamp_calculation",
                            "severity": "low",
                            "description": f"Slot {slot} timestamp off by {diff:.1f}s",
                            "slot": slot,
                            "expected": expected_timestamp.isoformat(),
                            "actual": actual_timestamp.isoformat()
                        })
                        
            except Exception as e:
                logger.error(f"Error checking slot timestamps for {table}: {e}")
                
        return dict(issues)
        
    async def _check_validator_snapshots(self) -> List[Dict]:
        """Check validator daily snapshots are correct."""
        issues = []
        
        try:
            # Check for duplicate snapshots on same day
            query = f"""
            SELECT 
                slot,
                toDate(slot_timestamp) as snapshot_date,
                COUNT(DISTINCT validator_index) as validator_count,
                COUNT(*) as total_rows
            FROM {self.clickhouse.database}.validators
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
            GROUP BY slot, snapshot_date
            HAVING validator_count != total_rows
            ORDER BY slot
            LIMIT 50
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                issues.append({
                    "type": "duplicate_validator_snapshot",
                    "severity": "high",
                    "description": f"Slot {row['slot']}: duplicate validators in snapshot ({row['total_rows']} rows for {row['validator_count']} validators)",
                    "slot": row['slot'],
                    "date": row['snapshot_date'].isoformat()
                })
                
            # Check for missing daily snapshots
            missing_query = f"""
            WITH daily_slots AS (
                SELECT 
                    toDate(slot_timestamp) as snapshot_date,
                    MAX(slot) as last_slot
                FROM {self.clickhouse.database}.blocks
                WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
                GROUP BY snapshot_date
            )
            SELECT 
                d.snapshot_date,
                d.last_slot,
                v.slot as validator_slot
            FROM daily_slots d
            LEFT JOIN (
                SELECT DISTINCT slot, toDate(slot_timestamp) as snapshot_date
                FROM {self.clickhouse.database}.validators
            ) v ON d.snapshot_date = v.snapshot_date
            WHERE v.slot IS NULL
            ORDER BY d.snapshot_date
            LIMIT 10
            """
            
            missing_result = self.clickhouse.execute(missing_query)
            
            for row in missing_result:
                issues.append({
                    "type": "missing_validator_snapshot",
                    "severity": "high",
                    "description": f"Missing validator snapshot for {row['snapshot_date'].isoformat()}",
                    "date": row['snapshot_date'].isoformat(),
                    "expected_slot": row['last_slot']
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking validator snapshots: {e}")
            return []
            
    async def _check_attestation_validity(self) -> List[Dict]:
        """Check attestation validity rules."""
        issues = []
        
        try:
            # Check attestations not too far from their slot
            query = f"""
            SELECT 
                slot as block_slot,
                attestation_slot,
                COUNT(*) as count
            FROM {self.clickhouse.database}.attestations
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
              AND slot - attestation_slot > 32  -- More than 1 epoch old
            GROUP BY block_slot, attestation_slot
            ORDER BY block_slot
            LIMIT 50
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                age = row['block_slot'] - row['attestation_slot']
                issues.append({
                    "type": "old_attestation",
                    "severity": "medium",
                    "description": f"Block {row['block_slot']} includes {row['count']} attestations from slot {row['attestation_slot']} ({age} slots old)",
                    "block_slot": row['block_slot'],
                    "attestation_slot": row['attestation_slot'],
                    "age": age
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking attestation validity: {e}")
            return []
            
    async def _check_proposer_slashings(self) -> List[Dict]:
        """Check proposer slashing validity."""
        issues = []
        
        try:
            # Check for duplicate proposer indices in slashings
            query = f"""
            SELECT 
                proposer_index,
                COUNT(*) as count,
                MIN(slot) as first_slot,
                MAX(slot) as last_slot
            FROM {self.clickhouse.database}.proposer_slashings
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
            GROUP BY proposer_index
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 20
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                issues.append({
                    "type": "duplicate_proposer_slashing",
                    "severity": "high",
                    "description": f"Proposer {row['proposer_index']} slashed {row['count']} times (slots {row['first_slot']}-{row['last_slot']})",
                    "proposer_index": row['proposer_index'],
                    "count": row['count']
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking proposer slashings: {e}")
            return []
            
    async def _check_sync_aggregates(self) -> List[Dict]:
        """Check sync aggregate participation."""
        issues = []
        
        try:
            # Check for abnormal participation rates
            query = f"""
            SELECT 
                slot,
                sync_aggregate_bits,
                length(sync_aggregate_bits) * 8 as total_bits,
                countSubstringsCaseInsensitive(sync_aggregate_bits, '1') as participation
            FROM {self.clickhouse.database}.sync_aggregates
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
              AND (participation = 0 OR participation = total_bits)
            ORDER BY rand()
            LIMIT 50
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                if row['participation'] == 0:
                    issues.append({
                        "type": "zero_sync_participation",
                        "severity": "medium",
                        "description": f"Slot {row['slot']}: sync aggregate has zero participation",
                        "slot": row['slot']
                    })
                elif row['participation'] == row['total_bits']:
                    issues.append({
                        "type": "perfect_sync_participation",
                        "severity": "low",
                        "description": f"Slot {row['slot']}: sync aggregate has perfect participation (unusual)",
                        "slot": row['slot']
                    })
                    
            return issues
            
        except Exception as e:
            logger.error(f"Error checking sync aggregates: {e}")
            return []
            
    async def _check_blob_sidecars(self) -> List[Dict]:
        """Check blob sidecar consistency."""
        issues = []
        
        try:
            # Check for blocks with too many blobs
            query = f"""
            SELECT 
                slot,
                block_root,
                COUNT(DISTINCT blob_index) as blob_count
            FROM {self.clickhouse.database}.blob_sidecars
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
            GROUP BY slot, block_root
            HAVING blob_count > 6  -- Max blobs per block
            ORDER BY slot
            LIMIT 20
            """
            
            result = self.clickhouse.execute(query)
            
            for row in result:
                issues.append({
                    "type": "excessive_blobs",
                    "severity": "high",
                    "description": f"Slot {row['slot']}: block has {row['blob_count']} blobs (max 6)",
                    "slot": row['slot'],
                    "blob_count": row['blob_count']
                })
                
            # Check for duplicate blob indices
            dup_query = f"""
            SELECT 
                slot,
                block_root,
                blob_index,
                COUNT(*) as count
            FROM {self.clickhouse.database}.blob_sidecars
            WHERE slot >= {self.start_slot} AND slot <= {self.end_slot}
            GROUP BY slot, block_root, blob_index
            HAVING count > 1
            ORDER BY slot
            LIMIT 20
            """
            
            dup_result = self.clickhouse.execute(dup_query)
            
            for row in dup_result:
                issues.append({
                    "type": "duplicate_blob_index",
                    "severity": "high",
                    "description": f"Slot {row['slot']}: blob index {row['blob_index']} appears {row['count']} times",
                    "slot": row['slot'],
                    "blob_index": row['blob_index']
                })
                
            return issues
            
        except Exception as e:
            logger.error(f"Error checking blob sidecars: {e}")
            return []
            
    async def _fix_issues(self, issues: Dict[str, List[Dict]]) -> Dict[str, int]:
        """Attempt to fix found issues."""
        fixed_counts = defaultdict(int)
        
        for dataset_name, dataset_issues in issues.items():
            for issue in dataset_issues:
                if issue['type'] == 'gap':
                    # Create range for gap
                    if self.state_manager.create_range(
                        "historical", dataset_name, 
                        issue['start_slot'], issue['end_slot'],
                        batch_id="validation_fix"
                    ):
                        fixed_counts[dataset_name] += 1
                        logger.info(f"Created range to fill gap {issue['start_slot']}-{issue['end_slot']}")
                        
                elif issue['type'] == 'duplicate_block' and self.fix_issues:
                    # Remove duplicate blocks (keep the first one)
                    await self._remove_duplicate_blocks(issue['slot'])
                    fixed_counts[dataset_name] += 1
                    
                elif issue['type'] == 'row_count_mismatch':
                    # Update state with correct count
                    await self._update_row_count(
                        dataset_name, 
                        issue['start_slot'], 
                        issue['end_slot'],
                        issue['actual']
                    )
                    fixed_counts[dataset_name] += 1
                    
                elif issue['type'] == 'incomplete_range' and issue['status'] == 'failed':
                    # Reset failed range to pending
                    self.state_manager.reset_failed_range(
                        "historical", dataset_name,
                        issue['start_slot'], issue['end_slot']
                    )
                    fixed_counts[dataset_name] += 1
                    
        return dict(fixed_counts)
        
    async def _remove_duplicate_blocks(self, slot: int) -> None:
        """Remove duplicate blocks, keeping the canonical one."""
        try:
            # Get all blocks for this slot
            query = f"""
            SELECT slot, block_root, proposer_index, is_canonical, slot_timestamp
            FROM {self.clickhouse.database}.blocks
            WHERE slot = {slot}
            ORDER BY is_canonical DESC, slot_timestamp ASC
            """
            
            result = self.clickhouse.execute(query)
            
            if len(result) > 1:
                # Keep the first one (canonical or earliest)
                keep_root = result[0]['block_root']
                
                # Delete the rest
                for i in range(1, len(result)):
                    delete_query = f"""
                    ALTER TABLE {self.clickhouse.database}.blocks
                    DELETE WHERE slot = {slot} AND block_root = '{result[i]['block_root']}'
                    """
                    
                    self.clickhouse.execute(delete_query)
                    
                logger.info(f"Removed {len(result)-1} duplicate blocks at slot {slot}, kept {keep_root}")
                
        except Exception as e:
            logger.error(f"Error removing duplicate blocks: {e}")
            
    async def _update_row_count(self, dataset: str, start_slot: int, end_slot: int, count: int) -> None:
        """Update row count in state."""
        try:
            update_query = f"""
            INSERT INTO {self.clickhouse.database}.indexing_state
            (mode, dataset, start_slot, end_slot, status, rows_indexed, version)
            VALUES
            ('historical', '{dataset}', {start_slot}, {end_slot}, 'completed', {count}, now())
            """
            
            self.clickhouse.execute(update_query)
            logger.info(f"Updated row count for {dataset} {start_slot}-{end_slot} to {count}")
            
        except Exception as e:
            logger.error(f"Error updating row count: {e}")
            
    def _generate_report(self, issues: Dict[str, List[Dict]], fixed: Dict[str, int]) -> str:
        """Generate validation report."""
        report_lines = ["VALIDATION REPORT", "=" * 80]
        
        # Summary
        total_issues = sum(len(i) for i in issues.values())
        total_fixed = sum(fixed.values())
        
        report_lines.extend([
            f"Validation completed at: {datetime.now().isoformat()}",
            f"Slot range: {self.start_slot} - {self.end_slot}",
            f"Sample rate: {self.sample_rate * 100:.1f}%",
            f"Datasets validated: {len(issues)}",
            f"Total issues found: {total_issues}",
            f"Issues fixed: {total_fixed}",
            ""
        ])
        
        if not issues:
            report_lines.append("No issues found!")
        else:
            # Group issues by severity
            severity_counts = {"high": 0, "medium": 0, "low": 0}
            
            for dataset_issues in issues.values():
                for issue in dataset_issues:
                    severity = issue.get('severity', 'medium')
                    severity_counts[severity] += 1
                    
            report_lines.extend([
                "Issue severity breakdown:",
                f"  High:   {severity_counts['high']}",
                f"  Medium: {severity_counts['medium']}",
                f"  Low:    {severity_counts['low']}",
                "",
                "Issues by dataset:",
                "-" * 40
            ])
            
            # Sort datasets by issue count
            sorted_datasets = sorted(issues.items(), key=lambda x: len(x[1]), reverse=True)
            
            for dataset_name, dataset_issues in sorted_datasets:
                if not dataset_issues:
                    continue
                    
                high = sum(1 for i in dataset_issues if i.get('severity') == 'high')
                medium = sum(1 for i in dataset_issues if i.get('severity') == 'medium')
                low = sum(1 for i in dataset_issues if i.get('severity') == 'low')
                
                report_lines.append(
                    f"\n{dataset_name}: {len(dataset_issues)} issues "
                    f"(H:{high} M:{medium} L:{low})"
                )
                
                # Group issues by type
                issue_types = defaultdict(list)
                for issue in dataset_issues:
                    issue_types[issue['type']].append(issue)
                    
                # Show summary by type
                for issue_type, type_issues in sorted(issue_types.items(), key=lambda x: len(x[1]), reverse=True):
                    report_lines.append(f"  {issue_type}: {len(type_issues)} occurrences")
                    
                    # Show first few examples
                    for issue in type_issues[:3]:
                        desc = issue['description']
                        if len(desc) > 70:
                            desc = desc[:67] + "..."
                        report_lines.append(f"    - {desc}")
                        
                    if len(type_issues) > 3:
                        report_lines.append(f"    ... and {len(type_issues) - 3} more")
                        
                if dataset_name in fixed and fixed[dataset_name] > 0:
                    report_lines.append(f"  ✓ Fixed: {fixed[dataset_name]} issues")
                    
        # Add recommendations
        report_lines.extend([
            "",
            "Recommendations:",
            "-" * 40
        ])
        
        if severity_counts.get('high', 0) > 0:
            report_lines.append("- Address high severity issues immediately")
            report_lines.append("- Consider running with fix_issues=true for auto-fixes")
            
        if total_issues > 100:
            report_lines.append("- Large number of issues detected")
            report_lines.append("- Consider running gap filling operation")
            report_lines.append("- Review indexing configuration and retry logic")
            
        if any('gap' in issue['type'] for issues in issues.values() for issue in issues):
            report_lines.append("- Gaps detected in data")
            report_lines.append("- Run FILL_GAPS operation to address missing data")
            
        if any('duplicate' in issue['type'] for issues in issues.values() for issue in issues):
            report_lines.append("- Duplicate data detected")
            report_lines.append("- Review deduplication settings")
            report_lines.append("- Consider running with fix_issues=true")
            
        report_lines.append("=" * 80)
        
        return "\n".join(report_lines)