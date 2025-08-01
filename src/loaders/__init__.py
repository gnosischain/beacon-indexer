from .blocks import BlocksLoader
from .validators import ValidatorsLoader
from .specs import SpecsLoader
from .genesis import GenesisLoader
from .rewards import RewardsLoader

LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader,
    "rewards": RewardsLoader
}

def get_enabled_loaders(enabled_names, beacon_api, clickhouse):
    """Get enabled loader instances."""
    loaders = []
    for name in enabled_names:
        if name in LOADER_REGISTRY:
            loader_class = LOADER_REGISTRY[name]
            loaders.append(loader_class(beacon_api, clickhouse))
    return loaders