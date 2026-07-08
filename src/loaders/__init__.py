from .blocks import BlocksLoader
from .validators import ValidatorsLoader
from .specs import SpecsLoader
from .genesis import GenesisLoader
from .rewards import RewardsLoader
from .data_column_sidecars import DataColumnSidecarsLoader
from .pending_consolidations import PendingConsolidationsLoader
from .pending_deposits import PendingDepositsLoader
from .pending_partial_withdrawals import PendingPartialWithdrawalsLoader

LOADER_REGISTRY = {
    "blocks": BlocksLoader,
    "validators": ValidatorsLoader,
    "specs": SpecsLoader,
    "genesis": GenesisLoader,
    "rewards": RewardsLoader,
    "data_column_sidecars": DataColumnSidecarsLoader,
    # Electra+ beacon-state queue loaders. Fork-gated via ELECTRA_START_SLOT in
    # config; opt-in via ENABLED_LOADERS env var. Safe to enable on networks that
    # haven't yet activated Electra — they'll skip silently until activation.
    "pending_consolidations": PendingConsolidationsLoader,
    "pending_deposits": PendingDepositsLoader,
    "pending_partial_withdrawals": PendingPartialWithdrawalsLoader,
}

def get_enabled_loaders(enabled_names, beacon_api, clickhouse):
    """Get enabled loader instances."""
    loaders = []
    for name in enabled_names:
        if name in LOADER_REGISTRY:
            loader_class = LOADER_REGISTRY[name]
            loaders.append(loader_class(beacon_api, clickhouse))
    return loaders
