from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

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
            
            # Always process core block data
            await self._process_beacon_block(info)
            
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
    
    async def _process_beacon_block(self, info: Dict, is_canonical: bool = True) -> None:
        """Process and store beacon block data."""
        slot = info["slot"]
        block_root = info["block_root"]
        proposer_index = info["proposer_index"]
        parent_root = info["parent_root"]
        state_root = info["state_root"]
        signature = info["signature"]
        version = info["version"]
        body = info["body"]
        
        # Extract eth1 data
        eth1_data = body.get("eth1_data", {})
        eth1_block_hash = eth1_data.get("block_hash", "")
        eth1_deposit_root = eth1_data.get("deposit_root", "")
        eth1_deposit_count = int(eth1_data.get("deposit_count", 0))
        
        # Other block data
        graffiti = body.get("graffiti", "")
        randao_reveal = body.get("randao_reveal", "")
        
        # Get current timestamp from execution payload if available
        timestamp = None
        if version in ["bellatrix", "capella", "deneb", "electra"]:
            execution_payload = body.get("execution_payload", {})
            if execution_payload and "timestamp" in execution_payload:
                timestamp_val = int(execution_payload.get("timestamp", 0))
                if timestamp_val > 0:
                    timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc)
        
        block_data = {
            "slot": slot,
            "block_root": block_root,
            "parent_root": parent_root,
            "state_root": state_root,
            "proposer_index": proposer_index,
            "eth1_block_hash": eth1_block_hash,
            "eth1_deposit_root": eth1_deposit_root,
            "eth1_deposit_count": eth1_deposit_count,
            "graffiti": graffiti,
            "signature": signature,
            "randao_reveal": randao_reveal,
            "timestamp": timestamp,
            "is_canonical": 1 if is_canonical else 0,
            "fork_version": version
        }
        
        await self._insert_with_bulk("blocks", block_data)
        
        logger.info(f"Processed beacon block at slot {slot} with root {block_root}")
    
    async def _process_execution_payload(self, info: Dict, execution_payload: Dict) -> None:
        """Process and store execution payload."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        # Extract execution payload fields
        parent_hash = execution_payload.get("parent_hash", "")
        fee_recipient = execution_payload.get("fee_recipient", "")
        state_root = execution_payload.get("state_root", "")
        receipts_root = execution_payload.get("receipts_root", "")
        logs_bloom = execution_payload.get("logs_bloom", "")
        prev_randao = execution_payload.get("prev_randao", "")
        block_number = int(execution_payload.get("block_number", 0))
        gas_limit = int(execution_payload.get("gas_limit", 0))
        gas_used = int(execution_payload.get("gas_used", 0))
        timestamp = int(execution_payload.get("timestamp", 0))
        extra_data = execution_payload.get("extra_data", "")
        base_fee_per_gas = int(execution_payload.get("base_fee_per_gas", 0))
        block_hash = execution_payload.get("block_hash", "")
        
        # Post-Deneb fields
        blob_gas_used = int(execution_payload.get("blob_gas_used", 0))
        excess_blob_gas = int(execution_payload.get("excess_blob_gas", 0))
        
        payload_data = {
            "slot": slot,
            "block_root": block_root,
            "parent_hash": parent_hash,
            "fee_recipient": fee_recipient,
            "state_root": state_root,
            "receipts_root": receipts_root,
            "logs_bloom": logs_bloom,
            "prev_randao": prev_randao,
            "block_number": block_number,
            "gas_limit": gas_limit,
            "gas_used": gas_used,
            "timestamp": parse_timestamp(timestamp),
            "extra_data": extra_data,
            "base_fee_per_gas": base_fee_per_gas,
            "block_hash": block_hash,
            "blob_gas_used": blob_gas_used,
            "excess_blob_gas": excess_blob_gas
        }
        
        await self._insert_with_bulk("execution_payloads", payload_data)
        
        logger.info(f"Processed execution payload for block at slot {slot}")