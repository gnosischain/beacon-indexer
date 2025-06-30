"""Dataset definitions and registry for beacon chain data."""
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from src.utils.logger import logger


class DatasetType(Enum):
    """Types of datasets in the beacon chain."""
    BLOCK = "block"
    ATTESTATION = "attestation"
    VALIDATOR = "validator"
    EXECUTION = "execution"
    REWARD = "reward"
    SLASHING = "slashing"
    OPERATIONAL = "operational"
    BLOB = "blob"
    SPEC = "spec"


@dataclass
class Dataset:
    """Definition of a dataset."""
    name: str
    dataset_type: DatasetType
    tables: List[str]
    scraper_id: str
    dependencies: List[str]
    priority: int
    is_continuous: bool = True
    is_sparse: bool = False  # For validator daily snapshots
    
    def __hash__(self):
        return hash(self.name)


class DatasetRegistry:
    """Registry of all datasets and their relationships."""
    
    def __init__(self):
        self._datasets: Dict[str, Dataset] = {}
        self._table_to_dataset: Dict[str, str] = {}
        self._scraper_to_datasets: Dict[str, List[str]] = {}
        self._initialize_datasets()
        
    def _initialize_datasets(self):
        """Initialize all dataset definitions."""
        # Core block data
        self.register(Dataset(
            name="blocks",
            dataset_type=DatasetType.BLOCK,
            tables=["blocks"],
            scraper_id="core_block_scraper",
            dependencies=[],
            priority=0,
            is_continuous=True
        ))
        
        # Execution payload data
        self.register(Dataset(
            name="execution_payloads",
            dataset_type=DatasetType.EXECUTION,
            tables=["execution_payloads"],
            scraper_id="core_block_scraper",
            dependencies=["blocks"],
            priority=1,
            is_continuous=True
        ))
        
        # Attestation data
        self.register(Dataset(
            name="attestations",
            dataset_type=DatasetType.ATTESTATION,
            tables=["attestations"],
            scraper_id="attestation_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        # Sync aggregates
        self.register(Dataset(
            name="sync_aggregates",
            dataset_type=DatasetType.ATTESTATION,
            tables=["sync_aggregates"],
            scraper_id="attestation_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        # Validator data (daily snapshots)
        self.register(Dataset(
            name="validators",
            dataset_type=DatasetType.VALIDATOR,
            tables=["validators"],
            scraper_id="validator_scraper",
            dependencies=[],
            priority=3,
            is_continuous=True,
            is_sparse=True
        ))
        
        # Operational events
        self.register(Dataset(
            name="deposits",
            dataset_type=DatasetType.OPERATIONAL,
            tables=["deposits"],
            scraper_id="operational_events_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        self.register(Dataset(
            name="withdrawals",
            dataset_type=DatasetType.OPERATIONAL,
            tables=["withdrawals"],
            scraper_id="operational_events_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        self.register(Dataset(
            name="voluntary_exits",
            dataset_type=DatasetType.OPERATIONAL,
            tables=["voluntary_exits"],
            scraper_id="operational_events_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        self.register(Dataset(
            name="bls_to_execution_changes",
            dataset_type=DatasetType.OPERATIONAL,
            tables=["bls_to_execution_changes"],
            scraper_id="operational_events_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        # Transaction data
        self.register(Dataset(
            name="transactions",
            dataset_type=DatasetType.EXECUTION,
            tables=["transactions"],
            scraper_id="transaction_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        self.register(Dataset(
            name="kzg_commitments",
            dataset_type=DatasetType.BLOB,
            tables=["kzg_commitments"],
            scraper_id="transaction_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        # Slashing data
        self.register(Dataset(
            name="proposer_slashings",
            dataset_type=DatasetType.SLASHING,
            tables=["proposer_slashings"],
            scraper_id="slashing_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        self.register(Dataset(
            name="attester_slashings",
            dataset_type=DatasetType.SLASHING,
            tables=["attester_slashings"],
            scraper_id="slashing_scraper",
            dependencies=["blocks"],
            priority=2,
            is_continuous=True
        ))
        
        # Reward data
        self.register(Dataset(
            name="block_rewards",
            dataset_type=DatasetType.REWARD,
            tables=["block_rewards"],
            scraper_id="reward_scraper",
            dependencies=["blocks"],
            priority=3,
            is_continuous=True
        ))
        
        # Blob sidecars
        self.register(Dataset(
            name="blob_sidecars",
            dataset_type=DatasetType.BLOB,
            tables=["blob_sidecars"],
            scraper_id="blob_sidecar_scraper",
            dependencies=["blocks"],
            priority=3,
            is_continuous=True
        ))
        
        # Specs (one-time)
        self.register(Dataset(
            name="specs",
            dataset_type=DatasetType.SPEC,
            tables=["specs"],
            scraper_id="specs_scraper",
            dependencies=[],
            priority=0,
            is_continuous=False
        ))
        
    def register(self, dataset: Dataset) -> None:
        """Register a dataset."""
        self._datasets[dataset.name] = dataset
        
        # Update table mappings
        for table in dataset.tables:
            self._table_to_dataset[table] = dataset.name
            
        # Update scraper mappings
        if dataset.scraper_id not in self._scraper_to_datasets:
            self._scraper_to_datasets[dataset.scraper_id] = []
        self._scraper_to_datasets[dataset.scraper_id].append(dataset.name)
        
    def get_dataset(self, name: str) -> Optional[Dataset]:
        """Get a dataset by name."""
        return self._datasets.get(name)
        
    def get_dataset_for_table(self, table: str) -> Optional[Dataset]:
        """Get the dataset that owns a table."""
        dataset_name = self._table_to_dataset.get(table)
        return self._datasets.get(dataset_name) if dataset_name else None
        
    def get_datasets_for_scraper(self, scraper_id: str) -> List[Dataset]:
        """Get all datasets for a scraper."""
        dataset_names = self._scraper_to_datasets.get(scraper_id, [])
        return [self._datasets[name] for name in dataset_names]
        
    def get_all_datasets(self) -> List[Dataset]:
        """Get all registered datasets."""
        return list(self._datasets.values())
        
    def get_datasets_by_priority(self) -> List[Dataset]:
        """Get datasets sorted by priority."""
        return sorted(self._datasets.values(), key=lambda d: d.priority)
        
    def get_dependencies(self, dataset_name: str) -> List[Dataset]:
        """Get all dependencies for a dataset."""
        dataset = self._datasets.get(dataset_name)
        if not dataset:
            return []
            
        deps = []
        for dep_name in dataset.dependencies:
            dep = self._datasets.get(dep_name)
            if dep:
                deps.append(dep)
        return deps
        
    def get_dependent_datasets(self, dataset_name: str) -> List[Dataset]:
        """Get all datasets that depend on this dataset."""
        dependents = []
        for dataset in self._datasets.values():
            if dataset_name in dataset.dependencies:
                dependents.append(dataset)
        return dependents