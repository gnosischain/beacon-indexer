from typing import Dict, List, Any, Optional

from src.scrapers.base_scraper import BaseScraper
from src.services.beacon_api_service import BeaconAPIService
from src.services.clickhouse_service import ClickHouseService
from src.utils.logger import logger

class BlobSidecarScraper(BaseScraper):
    """Scraper for blob sidecars, using a separate API endpoint."""
    
    def __init__(self, beacon_api: BeaconAPIService, clickhouse: ClickHouseService):
        super().__init__("blob_sidecar_scraper", beacon_api, clickhouse)
    
    async def process(self, block_data: Dict) -> None:
        """Process a block and store its blob sidecars via API call."""
        # Extract version and data
        version = block_data.get("version", "phase0")
        
        # Skip if this is pre-Deneb
        if version not in ["deneb", "electra"]:
            logger.debug(f"Skipping blob sidecars for pre-Deneb block (version: {version})")
            return
        
        # Extract block information
        data = block_data.get("data", {})
        message = data.get("message", {})
        slot = int(message.get("slot", 0))
        
        # Get block root
        try:
            header_response = await self.beacon_api.get_block_header(str(slot))
            block_root = header_response.get("root", "")
        except Exception as e:
            logger.warning(f"Failed to get block header for slot {slot}: {e}")
            return
        
        # Check if there are blob KZG commitments in the block
        body = message.get("body", {})
        blob_kzg_commitments = body.get("blob_kzg_commitments", [])
        
        if not blob_kzg_commitments:
            logger.debug(f"No blob KZG commitments in block at slot {slot}, skipping blob sidecars")
            return
        
        # Get blob sidecars via API call
        try:
            blob_data = await self.beacon_api.get_blob_sidecars(str(slot))
            
            if not blob_data:
                logger.debug(f"No blob sidecars found for block at slot {slot}")
                return
            
            # Prepare batch insert for blob sidecars
            params = []
            
            for blob in blob_data:
                blob_index = int(blob.get("index", 0))
                kzg_commitment = blob.get("kzg_commitment", "")
                kzg_proof = blob.get("kzg_proof", "")
                blob_content = blob.get("blob", "")
                
                # For large blobs, we might want to store them in a separate table or compress them
                # Here we'll truncate to a reasonable size for demonstration
                max_blob_size = 10000  # Limit blob size to avoid overwhelming the database
                if blob_content and len(blob_content) > max_blob_size:
                    blob_content = blob_content[:max_blob_size] + "... [truncated]"
                
                params.append({
                    "slot": slot,
                    "block_root": block_root,
                    "blob_index": blob_index,
                    "kzg_commitment": kzg_commitment,
                    "kzg_proof": kzg_proof,
                    "blob_data": blob_content
                })
            
            # Insert blob sidecars
            if params:
                query = """
                INSERT INTO blob_sidecars (
                    slot, block_root, blob_index, kzg_commitment, 
                    kzg_proof, blob_data
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                
                logger.info(f"Processed {len(params)} blob sidecars for block at slot {slot}")
        
        except Exception as e:
            logger.error(f"Error processing blob sidecars for slot {slot}: {e}")