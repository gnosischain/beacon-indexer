# Legacy parsers (keep for backwards compatibility)
from .blocks import BlocksParser
from .validators import ValidatorsParser

# Fork-aware parsers
from .fork_base import ForkBaseParser
from .phase0 import Phase0Parser
from .altair import AltairParser
from .bellatrix import BellatrixParser
from .capella import CapellaParser
from .deneb import DenebParser
from .electra import ElectraParser
from .factory import ParserFactory

# Legacy registry (for backwards compatibility)
PARSER_REGISTRY = {
    "blocks": BlocksParser,
    "validators": ValidatorsParser
}

def get_enabled_parsers(enabled_names):
    """Get enabled parser instances (legacy function)."""
    parsers = []
    for name in enabled_names:
        if name in PARSER_REGISTRY:
            parser_class = PARSER_REGISTRY[name]
            parsers.append(parser_class())
    return parsers

# Fork-aware registry
FORK_PARSER_REGISTRY = {
    "phase0": Phase0Parser,
    "altair": AltairParser,
    "bellatrix": BellatrixParser,
    "capella": CapellaParser,
    "deneb": DenebParser,
    "electra": ElectraParser
}