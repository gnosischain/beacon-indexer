from typing import Dict, List, Any, Optional

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info, ensure_list
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class TransactionScraper(BaseScraper):
    """
    Processes transaction data and KZG commitments.
    Can be memory intensive due to transaction volume.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("transaction_scraper", beacon_api, clickhouse)
        self._bulk_inserter = None
        
        # Register transaction tables
        self.register_table("transactions")
        self.register_table("kzg_commitments")
    
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
    
    async def process(self, block_data: Dict) -> None:
        """Process a block and extract transaction data."""
        try:
            info = extract_block_info(block_data)
            body = info["body"]
            version = info["version"]
            
            # Process transactions (post-merge)
            if version in ["bellatrix", "capella", "deneb", "electra"]:
                execution_payload = body.get("execution_payload", {})
                if execution_payload:
                    transactions = execution_payload.get("transactions", [])
                    if transactions:
                        await self._process_transactions(info, execution_payload)
            
            # Process KZG commitments (Deneb+)
            if version in ["deneb", "electra"]:
                kzg_commitments = body.get("blob_kzg_commitments", [])
                if kzg_commitments:
                    await self._process_kzg_commitments(info, kzg_commitments)
            
            logger.info(f"Processed transaction data for slot {info['slot']}")
            
        except Exception as e:
            logger.error(f"Error processing transaction data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_transactions(self, info: Dict, execution_payload: Dict) -> None:
        """Process and store transactions."""
        slot = info["slot"]
        block_root = info["block_root"]
        block_hash = execution_payload.get("block_hash", "")
        
        transactions = ensure_list(execution_payload.get("transactions", []))
        
        params = []
        
        for i, tx in enumerate(transactions):
            params.append({
                "slot": slot,
                "block_root": block_root,
                "block_hash": block_hash,
                "tx_index": i,
                "raw_tx": tx
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("transactions", param)
                    self.increment_row_count("transactions")
                logger.info(f"Queued {len(params)} transactions for block at slot {slot}")
            else:
                query = """
                INSERT INTO transactions (
                    slot, block_root, block_hash, tx_index, raw_tx
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                self.increment_row_count("transactions", len(params))
                logger.info(f"Processed {len(params)} transactions for block at slot {slot}")
    
    async def _process_kzg_commitments(self, info: Dict, kzg_commitments: List[str]) -> None:
        """Process and store KZG commitments."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for i, commitment in enumerate(kzg_commitments):
            params.append({
                "slot": slot,
                "block_root": block_root,
                "commitment_index": i,
                "kzg_commitment": commitment
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("kzg_commitments", param)
                    self.increment_row_count("kzg_commitments")
                logger.info(f"Queued {len(params)} KZG commitments for block at slot {slot}")
            else:
                query = """
                INSERT INTO kzg_commitments (
                    slot, block_root, commitment_index, kzg_commitment
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                self.increment_row_count("kzg_commitments", len(params))
                logger.info(f"Processed {len(params)} KZG commitments for block at slot {slot}")