from typing import Dict, List, Any, Optional

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class OperationalEventsScraper(BaseScraper):
    """
    Processes operational events: deposits, withdrawals, voluntary exits, 
    BLS to execution changes, and consolidations.
    These are the key events for tracking validator lifecycle.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("operational_events_scraper", beacon_api, clickhouse)
        self._bulk_inserter = None
        
        # Register operational event tables
        self.register_table("deposits")
        self.register_table("withdrawals")
        self.register_table("voluntary_exits")
        self.register_table("bls_to_execution_changes")
        self.register_table("consolidations")
    
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
        """Process a block and extract operational events."""
        try:
            info = extract_block_info(block_data)
            body = info["body"]
            version = info["version"]
            
            # Process deposits
            deposits = body.get("deposits", [])
            if deposits:
                await self._process_deposits(info, deposits)
            
            # Process withdrawals (Shanghai+)
            if version in ["capella", "deneb", "electra"]:
                execution_payload = body.get("execution_payload", {})
                withdrawals = execution_payload.get("withdrawals", [])
                if withdrawals:
                    await self._process_withdrawals(info, withdrawals)
            
            # Process voluntary exits
            voluntary_exits = body.get("voluntary_exits", [])
            if voluntary_exits:
                await self._process_voluntary_exits(info, voluntary_exits)
            
            # Process BLS to execution changes (Capella+)
            if version in ["capella", "deneb", "electra"]:
                bls_changes = body.get("bls_to_execution_changes", [])
                if bls_changes:
                    await self._process_bls_changes(info, bls_changes)
            
            # Process consolidations (Electra+)
            if version == "electra":
                execution_requests = body.get("execution_requests", {})
                if execution_requests:
                    consolidations = execution_requests.get("consolidations", [])
                    if consolidations:
                        await self._process_consolidations(info, consolidations)
            
            logger.info(f"Processed operational events for slot {info['slot']}")
            
        except Exception as e:
            logger.error(f"Error processing operational events: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_deposits(self, info: Dict, deposits: List[Dict]) -> None:
        """Process and store deposits."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for i, deposit in enumerate(deposits):
            data = deposit.get("data", {})
            
            pubkey = data.get("pubkey", "")
            withdrawal_credentials = data.get("withdrawal_credentials", "")
            amount = int(data.get("amount", 0))
            signature = data.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "deposit_index": i,
                "pubkey": pubkey,
                "withdrawal_credentials": withdrawal_credentials,
                "amount": amount,
                "signature": signature
            })
        
        if params:
            await self._bulk_insert("deposits", params)
            logger.info(f"Processed {len(params)} deposits for block at slot {slot}")
    
    async def _process_withdrawals(self, info: Dict, withdrawals: List[Dict]) -> None:
        """Process and store withdrawals."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for withdrawal in withdrawals:
            index = int(withdrawal.get("index", 0))
            validator_index = int(withdrawal.get("validator_index", 0))
            address = withdrawal.get("address", "")
            amount = int(withdrawal.get("amount", 0))
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "withdrawal_index": index,
                "validator_index": validator_index,
                "address": address,
                "amount": amount
            })
        
        if params:
            await self._bulk_insert("withdrawals", params)
            logger.info(f"Processed {len(params)} withdrawals for block at slot {slot}")
    
    async def _process_voluntary_exits(self, info: Dict, exits: List[Dict]) -> None:
        """Process and store voluntary exits."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for exit in exits:
            message = exit.get("message", {})
            epoch = int(message.get("epoch", 0))
            validator_index = int(message.get("validator_index", 0))
            signature = exit.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "epoch": epoch,
                "validator_index": validator_index,
                "signature": signature
            })
        
        if params:
            await self._bulk_insert("voluntary_exits", params)
            logger.info(f"Processed {len(params)} voluntary exits for block at slot {slot}")
    
    async def _process_bls_changes(self, info: Dict, changes: List[Dict]) -> None:
        """Process and store BLS to execution address changes."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for change in changes:
            message = change.get("message", {})
            validator_index = int(message.get("validator_index", 0))
            from_bls_pubkey = message.get("from_bls_pubkey", "")
            to_execution_address = message.get("to_execution_address", "")
            signature = change.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "validator_index": validator_index,
                "from_bls_pubkey": from_bls_pubkey,
                "to_execution_address": to_execution_address,
                "signature": signature
            })
        
        if params:
            await self._bulk_insert("bls_to_execution_changes", params)
            logger.info(f"Processed {len(params)} BLS to execution changes for block at slot {slot}")
    
    async def _process_consolidations(self, info: Dict, consolidations: List[Dict]) -> None:
        """Process and store consolidations (Electra+)."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for consolidation in consolidations:
            source_address = consolidation.get("source_address", "")
            source_pubkey = consolidation.get("source_pubkey", "")
            target_pubkey = consolidation.get("target_pubkey", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "source_address": source_address,
                "source_pubkey": source_pubkey,
                "target_pubkey": target_pubkey
            })
        
        if params:
            await self._bulk_insert("consolidations", params)
            logger.info(f"Processed {len(params)} consolidations for block at slot {slot}")
    
    async def _bulk_insert(self, table_name: str, params: List[Dict]) -> None:
        """Helper method for bulk insertion."""
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            for param in params:
                bulk_inserter.queue_for_insertion(table_name, param)
                self.increment_row_count(table_name)
            logger.debug(f"Queued {len(params)} rows for {table_name}")
        else:
            # Construct query based on table
            query = f"INSERT INTO {table_name} ({', '.join(params[0].keys())}) VALUES"
            self.clickhouse.execute_many(query, params)
            self.increment_row_count(table_name, len(params))