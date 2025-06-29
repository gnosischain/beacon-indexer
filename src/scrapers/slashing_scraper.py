from typing import Dict, List, Any, Optional

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class SlashingScraper(BaseScraper):
    """
    Processes slashing events: proposer slashings and attester slashings.
    These are rare but important events for validator monitoring.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("slashing_scraper", beacon_api, clickhouse)
        self._bulk_inserter = None
        
        # Register slashing tables
        self.register_table("proposer_slashings")
        self.register_table("attester_slashings")
    
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
        """Process a block and extract slashing events."""
        try:
            info = extract_block_info(block_data)
            body = info["body"]
            
            # Process proposer slashings
            proposer_slashings = body.get("proposer_slashings", [])
            if proposer_slashings:
                await self._process_proposer_slashings(info, proposer_slashings)
            
            # Process attester slashings
            attester_slashings = body.get("attester_slashings", [])
            if attester_slashings:
                await self._process_attester_slashings(info, attester_slashings)
            
            logger.info(f"Processed slashing events for slot {info['slot']}")
            
        except Exception as e:
            logger.error(f"Error processing slashing events: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_proposer_slashings(self, info: Dict, slashings: List[Dict]) -> None:
        """Process and store proposer slashings."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for slashing in slashings:
            header_1 = slashing.get("signed_header_1", {})
            header_2 = slashing.get("signed_header_2", {})
            
            message_1 = header_1.get("message", {})
            proposer_index = int(message_1.get("proposer_index", 0))
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "proposer_index": proposer_index,
                "header_1_slot": int(message_1.get("slot", 0)),
                "header_1_parent_root": message_1.get("parent_root", ""),
                "header_1_state_root": message_1.get("state_root", ""),
                "header_1_body_root": message_1.get("body_root", ""),
                "header_1_signature": header_1.get("signature", ""),
                "header_2_slot": int(header_2.get("message", {}).get("slot", 0)),
                "header_2_parent_root": header_2.get("message", {}).get("parent_root", ""),
                "header_2_state_root": header_2.get("message", {}).get("state_root", ""),
                "header_2_body_root": header_2.get("message", {}).get("body_root", ""),
                "header_2_signature": header_2.get("signature", "")
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("proposer_slashings", param)
                    self.increment_row_count("proposer_slashings")
            else:
                query = """
                INSERT INTO proposer_slashings (
                    slot, block_root, proposer_index, header_1_slot, header_1_parent_root,
                    header_1_state_root, header_1_body_root, header_1_signature,
                    header_2_slot, header_2_parent_root, header_2_state_root,
                    header_2_body_root, header_2_signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                self.increment_row_count("proposer_slashings", len(params))
            
            logger.info(f"Processed {len(params)} proposer slashings for block at slot {slot}")
    
    async def _process_attester_slashings(self, info: Dict, slashings: List[Dict]) -> None:
        """Process and store attester slashings."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for slashing in slashings:
            attestation_1 = slashing.get("attestation_1", {})
            attestation_2 = slashing.get("attestation_2", {})
            
            # Extract attesting indices
            indices_1 = attestation_1.get("attesting_indices", [])
            indices_2 = attestation_2.get("attesting_indices", [])
            
            # Extract attestation data
            data_1 = attestation_1.get("data", {})
            data_2 = attestation_2.get("data", {})
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "attestation_1_indices": [int(i) for i in indices_1],
                "attestation_1_slot": int(data_1.get("slot", 0)),
                "attestation_1_index": int(data_1.get("index", 0)),
                "attestation_1_beacon_block_root": data_1.get("beacon_block_root", ""),
                "attestation_1_source_epoch": int(data_1.get("source", {}).get("epoch", 0)),
                "attestation_1_source_root": data_1.get("source", {}).get("root", ""),
                "attestation_1_target_epoch": int(data_1.get("target", {}).get("epoch", 0)),
                "attestation_1_target_root": data_1.get("target", {}).get("root", ""),
                "attestation_1_signature": attestation_1.get("signature", ""),
                "attestation_2_indices": [int(i) for i in indices_2],
                "attestation_2_slot": int(data_2.get("slot", 0)),
                "attestation_2_index": int(data_2.get("index", 0)),
                "attestation_2_beacon_block_root": data_2.get("beacon_block_root", ""),
                "attestation_2_source_epoch": int(data_2.get("source", {}).get("epoch", 0)),
                "attestation_2_source_root": data_2.get("source", {}).get("root", ""),
                "attestation_2_target_epoch": int(data_2.get("target", {}).get("epoch", 0)),
                "attestation_2_target_root": data_2.get("target", {}).get("root", ""),
                "attestation_2_signature": attestation_2.get("signature", "")
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("attester_slashings", param)
                    self.increment_row_count("attester_slashings")
            else:
                query = """
                INSERT INTO attester_slashings (
                    slot, block_root, attestation_1_indices, attestation_1_slot,
                    attestation_1_index, attestation_1_beacon_block_root,
                    attestation_1_source_epoch, attestation_1_source_root,
                    attestation_1_target_epoch, attestation_1_target_root,
                    attestation_1_signature, attestation_2_indices, attestation_2_slot,
                    attestation_2_index, attestation_2_beacon_block_root,
                    attestation_2_source_epoch, attestation_2_source_root,
                    attestation_2_target_epoch, attestation_2_target_root,
                    attestation_2_signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                self.increment_row_count("attester_slashings", len(params))
            
            logger.info(f"Processed {len(params)} attester slashings for block at slot {slot}")