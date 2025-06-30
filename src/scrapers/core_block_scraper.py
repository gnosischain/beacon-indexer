from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
import traceback

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info, parse_timestamp
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class CoreBlockScraper(BaseScraper):
    """
    Processes core block data including basic block info, raw blocks, and execution payloads.
    This is the minimal scraper that should always run.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("core_block_scraper", beacon_api, clickhouse)
        self._bulk_inserter = None
        
        # Register only core tables
        self.register_table("blocks")
        self.register_table("execution_payloads")
    
    def get_bulk_inserter(self) -> Optional[BulkInsertionService]:
        """Get the bulk inserter from the parent worker if available."""
        if not self._bulk_inserter:
            import inspect
            frame = inspect.currentframe()
            try:
                while frame:
                    if 'self' in frame.f_locals and hasattr(frame.f_locals['self'], 'bulk_inserter'):
                        self._bulk_inserter = frame.f_locals['self'].bulk_inserter
                        break
                    frame = frame.f_back
            finally:
                del frame
        return self._bulk_inserter
    
    async def _insert_with_bulk(self, table_name: str, data: Dict[str, Any]) -> bool:
        """Insert data either using bulk inserter or direct method."""
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion(table_name, data)
            self.increment_row_count(table_name)
            return True
        else:
            # Direct insertion logic would go here
            self.increment_row_count(table_name)
            return True
    
    async def process(self, block_data: Dict) -> None:
        """Process a block and extract core data."""
        try:
            info = extract_block_info(block_data)
            
            # Determine if block is canonical (default to True)
            is_canonical = info.get("finalized", True)
            
            # Always process core block data
            await self._process_beacon_block(info, is_canonical)
            
            # Process execution payload if present
            version = info["version"]
            body = info["body"]
            
            if version in ["bellatrix", "capella", "deneb", "electra"]:
                execution_payload = body.get("execution_payload", {})
                if execution_payload:
                    await self._process_execution_payload(info, execution_payload)
            
            logger.info(f"Processed core block data for slot {info['slot']}")
            
        except Exception as e:
            logger.error(f"Error processing core block data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_beacon_block(self, info: Dict, is_canonical: bool) -> None:
        """Process and store beacon block data."""
        try:
            # Prepare block data with defaults
            block_data = self._prepare_block_data(info)
            
            # Insert the block
            await self._insert_with_bulk("blocks", block_data)
            
        except Exception as e:
            logger.error(f"Error processing block: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    async def _process_execution_payload(self, info: Dict, execution_payload: Dict) -> None:
        """Process and store execution payload data."""
        if not execution_payload:
            return
            
        try:
            # Prepare payload data with defaults
            payload_data = self._prepare_execution_payload_data(info, execution_payload)
            
            # Insert the execution payload
            await self._insert_with_bulk("execution_payloads", payload_data)
            
        except Exception as e:
            logger.error(f"Error processing execution payload: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _prepare_block_data(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare block data with proper defaults for NULL values."""
        # Calculate epoch from slot
        slot = info['slot']
        slots_per_epoch = self.get_slots_per_epoch()
        epoch = slot // slots_per_epoch
        
        # Get timestamp and ensure it's timezone-naive for ClickHouse
        timestamp = info.get('timestamp', datetime.now(timezone.utc))
        if hasattr(timestamp, 'replace'):
            # Remove timezone info for ClickHouse
            timestamp = timestamp.replace(tzinfo=None)
        
        block_data = {
            'slot': slot,
            'epoch': epoch,  # Calculated from slot
            'block_root': info.get('block_root', ''),
            'parent_root': info.get('parent_root', ''),
            'state_root': info.get('state_root', ''),
            'proposer_index': info.get('proposer_index', 0),
            'graffiti': info.get('graffiti', ''),
            'eth1_block_hash': info.get('eth1_block_hash', ''),
            'eth1_deposit_root': info.get('eth1_deposit_root', ''),
            'eth1_deposit_count': info.get('eth1_deposit_count', 0),
            'attestations_count': len(info.get('body', {}).get('attestations', [])),
            'deposits_count': len(info.get('body', {}).get('deposits', [])),
            'voluntary_exits_count': len(info.get('body', {}).get('voluntary_exits', [])),
            'attester_slashings_count': len(info.get('body', {}).get('attester_slashings', [])),
            'proposer_slashings_count': len(info.get('body', {}).get('proposer_slashings', [])),
            'sync_committee_bits': info.get('body', {}).get('sync_aggregate', {}).get('sync_committee_bits', ''),
            'sync_committee_signature': info.get('body', {}).get('sync_aggregate', {}).get('sync_committee_signature', ''),
            'sync_committee_participants': 0,  # Will be calculated if sync_committee_bits exists
            'bls_to_execution_changes_count': len(info.get('body', {}).get('bls_to_execution_changes', [])),
            'blob_kzg_commitments_count': len(info.get('body', {}).get('blob_kzg_commitments', [])),
            'randao_reveal': info.get('body', {}).get('randao_reveal', ''),
            'version': info.get('version', ''),
            'signature': info.get('signature', ''),
            'timestamp': timestamp,  # Now timezone-naive
            'execution_optimistic': info.get('execution_optimistic', False)
        }
        
        # Calculate sync committee participants if we have the bits
        if block_data['sync_committee_bits']:
            try:
                block_data['sync_committee_participants'] = bin(int(block_data['sync_committee_bits'], 16)).count('1')
            except:
                block_data['sync_committee_participants'] = 0
        
        return block_data
    
    def _prepare_execution_payload_data(self, info: Dict, payload: Dict) -> Dict[str, Any]:
        """Prepare execution payload data with proper defaults."""
        # Convert timestamp and remove timezone
        ts = int(payload.get('timestamp', 0))
        if ts > 0:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt = dt.replace(tzinfo=None)  # Remove timezone for ClickHouse
        else:
            dt = datetime.now().replace(tzinfo=None)
        
        return {
            'slot': info['slot'],
            'block_root': info.get('block_root', ''),
            'parent_hash': payload.get('parent_hash', ''),
            'fee_recipient': payload.get('fee_recipient', ''),
            'state_root': payload.get('state_root', ''),
            'receipts_root': payload.get('receipts_root', ''),
            'logs_bloom': payload.get('logs_bloom', ''),
            'prev_randao': payload.get('prev_randao', ''),
            'block_number': int(payload.get('block_number', 0)),
            'gas_limit': int(payload.get('gas_limit', 0)),
            'gas_used': int(payload.get('gas_used', 0)),
            'timestamp': dt,  # Now timezone-naive
            'extra_data': payload.get('extra_data', ''),
            'base_fee_per_gas': int(payload.get('base_fee_per_gas', 0)),
            'block_hash': payload.get('block_hash', ''),
            'transactions_count': len(payload.get('transactions', [])),
            'withdrawals_count': len(payload.get('withdrawals', [])),
            'excess_blob_gas': int(payload.get('excess_blob_gas', 0)) if payload.get('excess_blob_gas') is not None else None,
            'blob_gas_used': int(payload.get('blob_gas_used', 0)) if payload.get('blob_gas_used') is not None else None
        }