from typing import Dict, List, Any, Optional
from datetime import datetime, timezone

from src.scrapers.base_scraper import BaseScraper
from src.utils.block_utils import extract_block_info, parse_timestamp, ensure_list
from src.utils.logger import logger
from src.services.bulk_insertion_service import BulkInsertionService

class BlockScraper(BaseScraper):
    """
    Processes all data directly available within a block response.
    Does not make additional API calls beyond the initial block fetch.
    Optimized for high throughput with bulk insertions.
    """
    
    def __init__(self, beacon_api, clickhouse):
        super().__init__("block_scraper", beacon_api, clickhouse)
        # Flag to detect if we're in a parallel worker context
        self._bulk_inserter = None
    
    def get_bulk_inserter(self) -> Optional[BulkInsertionService]:
        """Get the bulk inserter from the parent worker if available."""
        if not self._bulk_inserter:
            # Try to find it in the global context
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
            return True
        else:
            # Fall back to direct insertion
            query = f"INSERT INTO {table_name} VALUES"
            self.clickhouse.execute(query, data)
            return True
    
    async def process(self, block_data: Dict) -> None:
        """Process all components available directly in the block data."""
        try:
            # Extract common information using the utility function
            info = extract_block_info(block_data)
            
            # Get essential information
            version = info["version"]
            slot = info["slot"]
            block_root = info["block_root"]
            body = info["body"]
            
            # If block_root is empty, try to get it from the API
            if not block_root:
                try:
                    header_response = await self.beacon_api.get_block_header(str(slot))
                    block_root = header_response.get("root", "")
                    info["block_root"] = block_root
                except Exception as e:
                    logger.warning(f"Failed to get block header for slot {slot}: {e}")
                    block_root = f"unknown_root_for_slot_{slot}"
                    info["block_root"] = block_root
            
            # Determine finalization status
            is_canonical = info.get("finalized", True)
            
            # Process different aspects of the block based on what's available in the block data
            # 1. Process beacon block
            await self._process_beacon_block(info, is_canonical)
            
            # 2. Process attestations if present
            attestations = body.get("attestations", [])
            if attestations:
                await self._process_attestations(info, attestations)
            
            # 3. Process execution payload (post-merge)
            if version in ["bellatrix", "capella", "deneb", "electra"]:
                execution_payload = body.get("execution_payload", {})
                if execution_payload:
                    await self._process_execution_payload(info, execution_payload)
            
            # 4. Process deposits if present
            deposits = body.get("deposits", [])
            if deposits:
                await self._process_deposits(info, deposits)
            
            # 5. Process voluntary exits if present
            voluntary_exits = body.get("voluntary_exits", [])
            if voluntary_exits:
                await self._process_voluntary_exits(info, voluntary_exits)
            
            # 6. Process BLS to execution changes (Capella+)
            if version in ["capella", "deneb", "electra"]:
                bls_changes = body.get("bls_to_execution_changes", [])
                if bls_changes:
                    await self._process_bls_changes(info, bls_changes)
            
            # 7. Process blob KZG commitments (Deneb+)
            if version in ["deneb", "electra"]:
                kzg_commitments = body.get("blob_kzg_commitments", [])
                if kzg_commitments:
                    await self._process_kzg_commitments(info, kzg_commitments)
            
            # 8. Process sync aggregate (Altair+)
            if version in ["altair", "bellatrix", "capella", "deneb", "electra"]:
                sync_aggregate = body.get("sync_aggregate", {})
                if sync_aggregate:
                    await self._process_sync_aggregate(info, sync_aggregate)
            
            # 9. Process slashings
            proposer_slashings = body.get("proposer_slashings", [])
            if proposer_slashings:
                await self._process_proposer_slashings(info, proposer_slashings)
                
            attester_slashings = body.get("attester_slashings", [])
            if attester_slashings:
                await self._process_attester_slashings(info, attester_slashings)
            
            logger.info(f"Processed all block components for slot {slot}")
            
        except Exception as e:
            logger.error(f"Error processing block: {e}")
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
        
        # Insert block using bulk inserter if available
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion("blocks", block_data)
        else:
            # Fall back to direct insertion
            query = """
            INSERT INTO blocks (
                slot, block_root, parent_root, state_root, proposer_index,
                eth1_block_hash, eth1_deposit_root, eth1_deposit_count,
                graffiti, signature, randao_reveal, timestamp, is_canonical, fork_version
            ) VALUES (
                %(slot)s, %(block_root)s, %(parent_root)s, %(state_root)s, %(proposer_index)s,
                %(eth1_block_hash)s, %(eth1_deposit_root)s, %(eth1_deposit_count)s,
                %(graffiti)s, %(signature)s, %(randao_reveal)s, %(timestamp)s, %(is_canonical)s, %(fork_version)s
            )
            """
            
            self.clickhouse.execute(query, block_data)
        
        # Also store raw block data for reprocessing if needed
        import json
        block_json = json.dumps(info["data"])
        
        raw_block_data = {
            "slot": slot,
            "block_root": block_root,
            "version": version,
            "block_data": block_json,
            "is_canonical": 1 if is_canonical else 0
        }
        
        if bulk_inserter:
            bulk_inserter.queue_for_insertion("raw_blocks", raw_block_data)
        else:
            raw_query = """
            INSERT INTO raw_blocks (
                slot, block_root, version, block_data, is_canonical
            ) VALUES (
                %(slot)s, %(block_root)s, %(version)s, %(block_data)s, %(is_canonical)s
            )
            """
            
            self.clickhouse.execute(raw_query, raw_block_data)
        
        logger.info(f"Processed beacon block at slot {slot} with root {block_root}")
    
    async def _process_attestations(self, info: Dict, attestations: List[Dict]) -> None:
        """Process and store attestations."""
        slot = info["slot"]
        block_root = info["block_root"]
        version = info["version"]
        
        params = []
        
        for attestation in attestations:
            # Extract attestation data
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
            
            # In a real implementation, you would convert aggregation bits to validator indices
            validators = []
            
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
        
        # Insert attestations in batches
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("attestations", param)
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
                logger.info(f"Processed {len(params)} attestations for block at slot {slot}")
    
    async def _process_execution_payload(self, info: Dict, execution_payload: Dict) -> None:
        """Process and store execution payload, transactions, and withdrawals."""
        slot = info["slot"]
        block_root = info["block_root"]
        version = info["version"]
        
        # Extract payload data
        parent_hash = execution_payload.get("parent_hash", "")
        fee_recipient = execution_payload.get("fee_recipient", "")
        state_root = execution_payload.get("state_root", "")
        receipts_root = execution_payload.get("receipts_root", "")
        logs_bloom = execution_payload.get("logs_bloom", "")
        prev_randao = execution_payload.get("prev_randao", "")
        block_number = int(execution_payload.get("block_number", 0))
        gas_limit = int(execution_payload.get("gas_limit", 0))
        gas_used = int(execution_payload.get("gas_used", 0))
        timestamp_val = int(execution_payload.get("timestamp", 0))
        timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc) if timestamp_val > 0 else None
        extra_data = execution_payload.get("extra_data", "")
        base_fee_per_gas = int(execution_payload.get("base_fee_per_gas", 0))
        block_hash = execution_payload.get("block_hash", "")
        
        # Deneb+ fields
        excess_blob_gas = None
        blob_gas_used = None
        if version in ["deneb", "electra"]:
            excess_blob_gas = int(execution_payload.get("excess_blob_gas", 0))
            blob_gas_used = int(execution_payload.get("blob_gas_used", 0))
        
        # Get transactions and withdrawals
        transactions = execution_payload.get("transactions", [])
        transactions_count = len(transactions)
        
        withdrawals = []
        withdrawals_count = 0
        if version in ["capella", "deneb", "electra"]:
            withdrawals = execution_payload.get("withdrawals", [])
            withdrawals_count = len(withdrawals)
        
        # Create execution payload data
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
            "timestamp": timestamp,
            "extra_data": extra_data,
            "base_fee_per_gas": base_fee_per_gas,
            "block_hash": block_hash,
            "excess_blob_gas": excess_blob_gas,
            "blob_gas_used": blob_gas_used,
            "transactions_count": transactions_count,
            "withdrawals_count": withdrawals_count
        }
        
        # Insert using bulk inserter if available
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion("execution_payloads", payload_data)
        else:
            query = """
            INSERT INTO execution_payloads (
                slot, block_root, parent_hash, fee_recipient, state_root,
                receipts_root, logs_bloom, prev_randao, block_number, gas_limit,
                gas_used, timestamp, extra_data, base_fee_per_gas, block_hash,
                excess_blob_gas, blob_gas_used, transactions_count, withdrawals_count
            ) VALUES (
                %(slot)s, %(block_root)s, %(parent_hash)s, %(fee_recipient)s, %(state_root)s,
                %(receipts_root)s, %(logs_bloom)s, %(prev_randao)s, %(block_number)s, %(gas_limit)s,
                %(gas_used)s, %(timestamp)s, %(extra_data)s, %(base_fee_per_gas)s, %(block_hash)s,
                %(excess_blob_gas)s, %(blob_gas_used)s, %(transactions_count)s, %(withdrawals_count)s
            )
            """
            
            self.clickhouse.execute(query, payload_data)
        
        logger.info(f"Processed execution payload for block at slot {slot}")
        
        # Process transactions
        if transactions:
            tx_params = []
            for i, tx in enumerate(transactions):
                tx_params.append({
                    "slot": slot,
                    "block_root": block_root,
                    "tx_index": i,
                    "transaction_data": tx
                })
            
            if tx_params:
                if bulk_inserter:
                    for param in tx_params:
                        bulk_inserter.queue_for_insertion("transactions", param)
                    logger.info(f"Queued {len(tx_params)} transactions for block at slot {slot}")
                else:
                    tx_query = """
                    INSERT INTO transactions (
                        slot, block_root, tx_index, transaction_data
                    ) VALUES
                    """
                    
                    # Process in batches for larger transaction sets
                    batch_size = 1000
                    for i in range(0, len(tx_params), batch_size):
                        batch = tx_params[i:i+batch_size]
                        self.clickhouse.execute_many(tx_query, batch)
                    
                    logger.info(f"Processed {len(tx_params)} transactions for block at slot {slot}")
        
        # Process withdrawals
        if withdrawals:
            wd_params = []
            for wd in withdrawals:
                wd_params.append({
                    "slot": slot,
                    "block_root": block_root,
                    "withdrawal_index": int(wd.get("index", 0)),
                    "validator_index": int(wd.get("validator_index", 0)),
                    "address": wd.get("address", ""),
                    "amount": int(wd.get("amount", 0))
                })
            
            if wd_params:
                if bulk_inserter:
                    for param in wd_params:
                        bulk_inserter.queue_for_insertion("withdrawals", param)
                    logger.info(f"Queued {len(wd_params)} withdrawals for block at slot {slot}")
                else:
                    wd_query = """
                    INSERT INTO withdrawals (
                        slot, block_root, withdrawal_index, validator_index, address, amount
                    ) VALUES
                    """
                    
                    self.clickhouse.execute_many(wd_query, wd_params)
                    logger.info(f"Processed {len(wd_params)} withdrawals for block at slot {slot}")
    
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
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("deposits", param)
                logger.info(f"Queued {len(params)} deposits for block at slot {slot}")
            else:
                query = """
                INSERT INTO deposits (
                    slot, block_root, deposit_index, pubkey,
                    withdrawal_credentials, amount, signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} deposits for block at slot {slot}")
    
    async def _process_voluntary_exits(self, info: Dict, exits: List[Dict]) -> None:
        """Process and store voluntary exits."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for exit_data in exits:
            message = exit_data.get("message", {})
            validator_index = int(message.get("validator_index", 0))
            epoch = int(message.get("epoch", 0))
            signature = exit_data.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "validator_index": validator_index,
                "epoch": epoch,
                "signature": signature
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("voluntary_exits", param)
                logger.info(f"Queued {len(params)} voluntary exits for block at slot {slot}")
            else:
                query = """
                INSERT INTO voluntary_exits (
                    slot, block_root, validator_index, epoch, signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} voluntary exits for block at slot {slot}")
    
    async def _process_bls_changes(self, info: Dict, changes: List[Dict]) -> None:
        """Process and store BLS to execution changes."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for i, change in enumerate(changes):
            message = change.get("message", {})
            validator_index = int(message.get("validator_index", 0))
            from_bls_pubkey = message.get("from_bls_pubkey", "")
            to_execution_address = message.get("to_execution_address", "")
            signature = change.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "change_index": i,
                "validator_index": validator_index,
                "from_bls_pubkey": from_bls_pubkey,
                "to_execution_address": to_execution_address,
                "signature": signature
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("bls_to_execution_changes", param)
                logger.info(f"Queued {len(params)} BLS to execution changes for block at slot {slot}")
            else:
                query = """
                INSERT INTO bls_to_execution_changes (
                    slot, block_root, change_index, validator_index, 
                    from_bls_pubkey, to_execution_address, signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} BLS to execution changes for block at slot {slot}")
    
    async def _process_kzg_commitments(self, info: Dict, commitments: List) -> None:
        """Process and store KZG commitments."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for i, commitment in enumerate(commitments):
            params.append({
                "slot": slot,
                "block_root": block_root,
                "commitment_index": i,
                "commitment": commitment
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("kzg_commitments", param)
                logger.info(f"Queued {len(params)} KZG commitments for block at slot {slot}")
            else:
                query = """
                INSERT INTO kzg_commitments (
                    slot, block_root, commitment_index, commitment
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} KZG commitments for block at slot {slot}")

    async def _process_sync_aggregate(self, info: Dict, sync_aggregate: Dict) -> None:
        """Process and store sync aggregate data."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        sync_committee_bits = sync_aggregate.get("sync_committee_bits", "")
        sync_committee_signature = sync_aggregate.get("sync_committee_signature", "")
        
        sync_data = {
            "slot": slot,
            "block_root": block_root,
            "sync_committee_bits": sync_committee_bits,
            "sync_committee_signature": sync_committee_signature
        }
        
        bulk_inserter = self.get_bulk_inserter()
        if bulk_inserter:
            bulk_inserter.queue_for_insertion("sync_aggregates", sync_data)
            logger.info(f"Queued sync aggregate for block at slot {slot}")
        else:
            query = """
            INSERT INTO sync_aggregates (
                slot, block_root, sync_committee_bits, sync_committee_signature
            ) VALUES (
                %(slot)s, %(block_root)s, %(sync_committee_bits)s, %(sync_committee_signature)s
            )
            """
            
            self.clickhouse.execute(query, sync_data)
            logger.info(f"Processed sync aggregate for block at slot {slot}")
    
    async def _process_proposer_slashings(self, info: Dict, slashings: List[Dict]) -> None:
        """Process and store proposer slashings."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for slashing in slashings:
            header_1 = slashing.get("signed_header_1", {})
            header_1_message = header_1.get("message", {})
            header_1_slot = int(header_1_message.get("slot", 0))
            header_1_proposer = int(header_1_message.get("proposer_index", 0))
            header_1_root = header_1_message.get("body_root", "")
            header_1_signature = header_1.get("signature", "")
            
            header_2 = slashing.get("signed_header_2", {})
            header_2_message = header_2.get("message", {})
            header_2_slot = int(header_2_message.get("slot", 0))
            header_2_proposer = int(header_2_message.get("proposer_index", 0))
            header_2_root = header_2_message.get("body_root", "")
            header_2_signature = header_2.get("signature", "")
            
            # Both headers should have the same proposer index
            proposer_index = header_1_proposer
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "proposer_index": proposer_index,
                "header_1_slot": header_1_slot,
                "header_1_proposer": header_1_proposer,
                "header_1_root": header_1_root,
                "header_1_signature": header_1_signature,
                "header_2_slot": header_2_slot,
                "header_2_proposer": header_2_proposer,
                "header_2_root": header_2_root,
                "header_2_signature": header_2_signature
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("proposer_slashings", param)
                logger.info(f"Queued {len(params)} proposer slashings for block at slot {slot}")
            else:
                query = """
                INSERT INTO proposer_slashings (
                    slot, block_root, proposer_index, header_1_slot, header_1_proposer,
                    header_1_root, header_1_signature, header_2_slot, header_2_proposer,
                    header_2_root, header_2_signature
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} proposer slashings for block at slot {slot}")
    
    async def _process_attester_slashings(self, info: Dict, slashings: List[Dict]) -> None:
        """Process and store attester slashings."""
        slot = info["slot"]
        block_root = info["block_root"]
        
        params = []
        
        for i, slashing in enumerate(slashings):
            attestation_1 = slashing.get("attestation_1", {})
            attestation_1_data = attestation_1.get("data", {})
            attestation_1_indices = [int(idx) for idx in ensure_list(attestation_1.get("attesting_indices", []))]
            attestation_1_slot = int(attestation_1_data.get("slot", 0))
            attestation_1_index = int(attestation_1_data.get("index", 0))
            attestation_1_root = attestation_1_data.get("beacon_block_root", "")
            attestation_1_sig = attestation_1.get("signature", "")
            
            attestation_2 = slashing.get("attestation_2", {})
            attestation_2_data = attestation_2.get("data", {})
            attestation_2_indices = [int(idx) for idx in ensure_list(attestation_2.get("attesting_indices", []))]
            attestation_2_slot = int(attestation_2_data.get("slot", 0))
            attestation_2_index = int(attestation_2_data.get("index", 0))
            attestation_2_root = attestation_2_data.get("beacon_block_root", "")
            attestation_2_sig = attestation_2.get("signature", "")
            
            params.append({
                "slot": slot,
                "block_root": block_root,
                "slashing_index": i,
                "attestation_1_indices": attestation_1_indices,
                "attestation_1_slot": attestation_1_slot,
                "attestation_1_index": attestation_1_index,
                "attestation_1_root": attestation_1_root,
                "attestation_1_sig": attestation_1_sig,
                "attestation_2_indices": attestation_2_indices,
                "attestation_2_slot": attestation_2_slot,
                "attestation_2_index": attestation_2_index,
                "attestation_2_root": attestation_2_root,
                "attestation_2_sig": attestation_2_sig
            })
        
        if params:
            bulk_inserter = self.get_bulk_inserter()
            if bulk_inserter:
                for param in params:
                    bulk_inserter.queue_for_insertion("attester_slashings", param)
                logger.info(f"Queued {len(params)} attester slashings for block at slot {slot}")
            else:
                query = """
                INSERT INTO attester_slashings (
                    slot, block_root, slashing_index, attestation_1_indices,
                    attestation_1_slot, attestation_1_index, attestation_1_root, attestation_1_sig,
                    attestation_2_indices, attestation_2_slot, attestation_2_index,
                    attestation_2_root, attestation_2_sig
                ) VALUES
                """
                
                self.clickhouse.execute_many(query, params)
                logger.info(f"Processed {len(params)} attester slashings for block at slot {slot}")