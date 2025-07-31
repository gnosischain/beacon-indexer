import os
import yaml
from typing import Dict, Optional, NamedTuple, List
from src.utils.logger import logger

class ForkInfo(NamedTuple):
    name: str
    version: str
    epoch: int

class NetworkConfig(NamedTuple):
    name: str
    genesis_time: int
    seconds_per_slot: int
    slots_per_epoch: int

class ForkDetectionService:
    """Simplified fork detection service that gets most info from database."""
    
    def __init__(self, clickhouse_client=None):
        self.clickhouse = clickhouse_client
        self.network_config: Optional[NetworkConfig] = None
        self.fork_versions: Dict[str, str] = {}
        self.fork_epochs: Dict[str, int] = {}
        self.fork_order: List[str] = []
        
        # Load minimal config
        self._load_fork_config()
        
        # Auto-detect from database if available
        if self.clickhouse:
            self._auto_detect_from_database()
    
    def _load_fork_config(self):
        """Load minimal fork configuration."""
        config_path = self._get_config_path()
        
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            self.network_mapping = config.get('network_mapping', {})
            self.all_fork_versions = config.get('fork_versions', {})
            self.fork_order = config.get('fork_order', ['phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra'])
            
            logger.info("Minimal fork configuration loaded")
            
        except Exception as e:
            logger.error("Failed to load fork configuration", error=str(e))
            self._create_fallback_config()
    
    def _create_fallback_config(self):
        """Create minimal fallback configuration."""
        self.network_mapping = {"mainnet": "mainnet", "gnosis": "gnosis"}
        self.all_fork_versions = {
            "mainnet": {
                "phase0": "0x00000000", "altair": "0x01000000", "bellatrix": "0x02000000",
                "capella": "0x03000000", "deneb": "0x04000000", "electra": "0x05000000"
            }
        }
        self.fork_order = ['phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra']
        logger.warning("Using fallback fork configuration")
    
    def _get_config_path(self) -> str:
        """Get path to fork configuration file."""
        possible_paths = [
            '/app/config/forks.yaml',
            'config/forks.yaml',
            os.path.join(os.path.dirname(__file__), '../../config/forks.yaml')
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        # Create minimal config
        return self._create_minimal_config_file()
    
    def _create_minimal_config_file(self) -> str:
        """Create minimal configuration file."""
        minimal_config = {
            'network_mapping': {'mainnet': 'mainnet', 'gnosis': 'gnosis'},
            'fork_versions': {
                'mainnet': {
                    'phase0': '0x00000000', 'altair': '0x01000000', 'bellatrix': '0x02000000',
                    'capella': '0x03000000', 'deneb': '0x04000000', 'electra': '0x05000000'
                }
            },
            'fork_order': ['phase0', 'altair', 'bellatrix', 'capella', 'deneb', 'electra']
        }
        
        config_path = 'forks_minimal.yaml'
        with open(config_path, 'w') as f:
            yaml.dump(minimal_config, f)
        
        return config_path
    
    def _auto_detect_from_database(self):
        """Auto-detect network and fork info from database."""
        try:
            # 1. Get network from CONFIG_NAME
            network_name = self._detect_network_from_config_name()
            
            # 2. Get timing parameters from specs
            timing_params = self._get_timing_from_specs()
            
            # 3. Get genesis time
            genesis_time = self._get_genesis_time()
            
            # 4. Get fork epochs from raw blocks (analyze version field changes)
            fork_epochs = self._detect_fork_epochs_from_blocks()
            
            if network_name and timing_params and genesis_time:
                self.network_config = NetworkConfig(
                    name=network_name,
                    genesis_time=genesis_time,
                    seconds_per_slot=timing_params['seconds_per_slot'],
                    slots_per_epoch=timing_params['slots_per_epoch']
                )
                
                # Set fork versions for this network
                self.fork_versions = self.all_fork_versions.get(network_name, self.all_fork_versions.get('mainnet', {}))
                
                # Set fork epochs (from database analysis or defaults)
                self.fork_epochs = fork_epochs
                
                logger.info("Auto-detected network configuration", 
                           network=network_name,
                           genesis_time=genesis_time,
                           seconds_per_slot=timing_params['seconds_per_slot'],
                           slots_per_epoch=timing_params['slots_per_epoch'],
                           detected_forks=len(fork_epochs))
            
        except Exception as e:
            logger.warning("Auto-detection failed", error=str(e))
    
    def _detect_network_from_config_name(self) -> Optional[str]:
        """Detect network from CONFIG_NAME in specs."""
        try:
            result = self.clickhouse.execute("""
                SELECT parameter_value 
                FROM specs FINAL 
                WHERE parameter_name = 'CONFIG_NAME' 
                LIMIT 1
            """)
            
            if result:
                config_name = result[0]["parameter_value"].lower()
                network_name = self.network_mapping.get(config_name, "mainnet")
                logger.info("Detected network from CONFIG_NAME", 
                           config_name=config_name, 
                           network=network_name)
                return network_name
                
        except Exception as e:
            logger.error("Failed to detect network from CONFIG_NAME", error=str(e))
            
        return None
    
    def _get_timing_from_specs(self) -> Optional[Dict]:
        """Get timing parameters from specs table."""
        try:
            result = self.clickhouse.execute("""
                SELECT parameter_name, parameter_value 
                FROM specs FINAL 
                WHERE parameter_name IN ('SECONDS_PER_SLOT', 'SLOTS_PER_EPOCH')
            """)
            
            if result:
                params = {row["parameter_name"]: int(row["parameter_value"]) for row in result}
                if 'SECONDS_PER_SLOT' in params and 'SLOTS_PER_EPOCH' in params:
                    return {
                        'seconds_per_slot': params['SECONDS_PER_SLOT'],
                        'slots_per_epoch': params['SLOTS_PER_EPOCH']
                    }
                    
        except Exception as e:
            logger.error("Failed to get timing from specs", error=str(e))
            
        return None
    
    def _get_genesis_time(self) -> Optional[int]:
        """Get genesis time from genesis table."""
        try:
            result = self.clickhouse.execute("""
                SELECT toUnixTimestamp(genesis_time) as genesis_time_unix 
                FROM genesis 
                LIMIT 1
            """)
            
            if result:
                return int(result[0]["genesis_time_unix"])
                
        except Exception as e:
            logger.error("Failed to get genesis time", error=str(e))
            
        return None
    
    def _detect_fork_epochs_from_blocks(self) -> Dict[str, int]:
        """Detect fork activation epochs by analyzing version changes in raw blocks."""
        try:
            # Check if raw_blocks table exists and has data
            count_result = self.clickhouse.execute("SELECT COUNT(*) as count FROM raw_blocks LIMIT 1")
            if not count_result or count_result[0]["count"] == 0:
                logger.info("No raw blocks found yet, using default fork epochs")
                return self._get_default_fork_epochs()
            
            # Parse version from raw block data and find first occurrences
            # We need to extract the version from the JSON payload
            result = self.clickhouse.execute("""
                SELECT 
                    JSONExtractString(payload, 'version') as version,
                    min(slot) as first_slot,
                    min(slot) / {slots_per_epoch:UInt32} as first_epoch
                FROM raw_blocks 
                GROUP BY version
                ORDER BY first_slot
            """, {"slots_per_epoch": self.slots_per_epoch or 32})
            
            fork_epochs = {'phase0': 0}  # Always start with phase0 at epoch 0
            
            # Map versions to fork names
            if result:
                logger.info("Found version data in raw blocks", versions=[row["version"] for row in result])
                for row in result:
                    version = row["version"]
                    epoch = int(row["first_epoch"])
                    
                    # Find which fork this version belongs to
                    for fork_name in self.fork_order[1:]:  # Skip phase0, already added
                        if version == self.fork_versions.get(fork_name):
                            fork_epochs[fork_name] = epoch
                            logger.info("Mapped version to fork", version=version, fork=fork_name, epoch=epoch)
                            break
            else:
                logger.warning("No version data found in raw blocks, this should not happen")
            
            # If we didn't detect many forks, fill in defaults for missing ones
            if len(fork_epochs) < 3:  # Less than 3 forks detected
                logger.info("Limited fork detection, supplementing with defaults")
                default_epochs = self._get_default_fork_epochs()
                for fork_name, default_epoch in default_epochs.items():
                    if fork_name not in fork_epochs:
                        fork_epochs[fork_name] = default_epoch
            
            logger.info("Final fork epochs", fork_epochs=fork_epochs)
            return fork_epochs
            
        except Exception as e:
            logger.warning("Failed to detect fork epochs from raw blocks", error=str(e))
            return self._get_default_fork_epochs()
    
    def _get_default_fork_epochs(self) -> Dict[str, int]:
        """Get default fork epochs based on network."""
        network_name = getattr(self.network_config, 'name', 'mainnet') if self.network_config else 'mainnet'
        
        if network_name == 'gnosis':
            return {
                'phase0': 0, 'altair': 512, 'bellatrix': 385536, 
                'capella': 648704, 'deneb': 889856, 'electra': 1337856
            }
        elif network_name == 'holesky':
            return {
                'phase0': 0, 'altair': 0, 'bellatrix': 0, 
                'capella': 256, 'deneb': 29696, 'electra': 115968
            }
        elif network_name == 'sepolia':
            return {
                'phase0': 0, 'altair': 50, 'bellatrix': 100, 
                'capella': 112260, 'deneb': 132608, 'electra': 267264
            }
        else:  # mainnet
            return {
                'phase0': 0, 'altair': 74240, 'bellatrix': 144896, 
                'capella': 194048, 'deneb': 269568, 'electra': 364032
            }
    
    @property
    def slots_per_epoch(self) -> int:
        """Get slots per epoch."""
        return self.network_config.slots_per_epoch if self.network_config else 32
    
    @property
    def seconds_per_slot(self) -> int:
        """Get seconds per slot."""
        return self.network_config.seconds_per_slot if self.network_config else 12
    
    @property
    def genesis_time(self) -> int:
        """Get genesis time."""
        return self.network_config.genesis_time if self.network_config else 1606824023
    
    def get_fork_at_slot(self, slot: int) -> ForkInfo:
        """Get active fork for a given slot."""
        epoch = slot // self.slots_per_epoch
        return self.get_fork_at_epoch(epoch)
    
    def get_fork_at_epoch(self, epoch: int) -> ForkInfo:
        """Get active fork for a given epoch."""
        active_fork_name = 'phase0'  # Default
        
        # Find the latest activated fork
        for fork_name in self.fork_order:
            fork_epoch = self.fork_epochs.get(fork_name, 0)
            if epoch >= fork_epoch:
                active_fork_name = fork_name
            else:
                break
        
        # Create ForkInfo
        return ForkInfo(
            name=active_fork_name,
            version=self.fork_versions.get(active_fork_name, "0x00000000"),
            epoch=self.fork_epochs.get(active_fork_name, 0)
        )
    
    def get_network_name(self) -> str:
        """Get current network name."""
        return self.network_config.name if self.network_config else "unknown"
    
    def get_all_forks(self) -> Dict[str, ForkInfo]:
        """Get all configured forks."""
        result = {}
        for fork_name in self.fork_order:
            if fork_name in self.fork_epochs:
                result[fork_name] = ForkInfo(
                    name=fork_name,
                    version=self.fork_versions.get(fork_name, "0x00000000"),
                    epoch=self.fork_epochs.get(fork_name, 0)
                )
        return result
    
    def is_auto_detected(self) -> bool:
        """Check if network was auto-detected."""
        return self.network_config is not None