from typing import Dict, List, Any, Optional

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class AttestationScraper(BaseScraper):
    """
    Processes attestation and sync aggregate data.
    These are the heaviest tables in terms of data volume.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("attestation_scraper", beacon_api, clickhouse)
        self._bulk_inserter = None
        
        # Register attestation-related tables
        self.register_table("attestations")
        self.register_table("sync_aggregates")
    
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
        """Process a block and extract attestation data."""
        try:
            info = extract_block_info(block_data)
            body = info["body"]
            version = info["version"]
            
            # Process attestations
            attestations = body.get("attestations", [])
            if attestations:
                await self._process_attestations(info, attestations)
            
            # Process sync aggregate (Altair+)
            if version in ["altair", "bellatrix", "capella", "deneb", "electra"]:
                sync_aggregate = body.get("sync_aggregate", {})
                if sync_aggregate:
                    await self._process_sync_aggregate(info, sync_aggregate)
            
            logger.info(f"Processed attestation data for slot {info['slot']}")
            
        except Exception as e:
            logger.error(f"Error processing attestation data: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_attestations(self, info: Dict, attestations: List[Dict]) -> None:
        """Process and store attestations."""
        slot = info["slot"]
        block_root = info["block_root"]
        version = info["version"]
        
        params = []
        
        for attestation in attestations:
            aggregation_bits = attestation.get("aggregation_bits", "")
            committee_bits = attestation.get("committee_bits", "") if version == "electra" else ""
            signature = attestation.get("signature", "")
            
            data = attestation.get("data", {})
            attestation_slot = int(data.get("slot", 0))
            attestation_index = int(data.get("index", 0))
            beacon_block_root = data.get("beacon_block_root", "")
            
            source = data.get("source", {})
            source_epoch = int(source.get("epoch", 0))
            source_root = source.get("root", "")
            
            target = data.get("target", {})
            target_epoch = int(target.get("epoch", 0))
            target_root = target.get("root", "")
            
            validators = []  # Would need committee data to resolve
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "attestation_slot": attestation_slot,
                "attestation_index": attestation_index,
                "aggregation_bits": aggregation_bits,
                "committee_bits": committee_bits,
                "beacon_block_root": beacon_block_root,
                "source_epoch": source_epoch,
                "source_root": source_root,
                "target_epoch": target_epoch,
                "target_root": target_root,
                "signature": signature,
                "validators": validators
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("attestations", param)
                    self.increment_row_count("attestations")
                logger.info(f"Queued {len(params)} attestations for block at slot {slot}")
            else:
                query = """
                INSERT INTO attestations (
                    slot, block_root, attestation_slot, attestation_index, aggregation_bits,
                    committee_bits, beacon_block_root, source_epoch, source_root, target_epoch,
                    target_root, signature, validators
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                self.increment_row_count("attestations", len(params))
                logger.info(f"Processed {len(params)} attestations for block at slot {slot}")
    
    async def _process_sync_aggregate(self, info: Dict, sync_aggregate: Dict) -> None:
        """Process and store sync aggregate."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        sync_committee_bits = sync_aggregate.get("sync_committee_bits", "")
        sync_committee_signature = sync_aggregate.get("sync_committee_signature", "")
        
        # Count participating validators
        bits_hex = sync_committee_bits.replace("0x", "")
        bits_binary = bin(int(bits_hex, 16))[2:].zfill(512)
        participating_validators = bits_binary.count('1')
        
        aggregate_data = {
            "slot": slot,
            "block_root": block_root,
            "sync_committee_bits": sync_committee_bits,
            "sync_committee_signature": sync_committee_signature,
            "participating_validators": participating_validators
        }
        
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion("sync_aggregates", aggregate_data)
            self.increment_row_count("sync_aggregates")
        else:
            query = """
            INSERT INTO sync_aggregates (
                slot, block_root, sync_committee_bits, sync_committee_signature, participating_validators
            ) VALUES
            """
            
            self.clickhouse.execute(query, aggregate_data)
            self.increment_row_count("sync_aggregates")
        
        logger.info(f"Processed sync aggregate for slot {slot} with {participating_validators} participants")