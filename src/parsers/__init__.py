from .blocks import BlocksParser
from .validators import ValidatorsParser

PARSER_REGISTRY = {
    "blocks": BlocksParser,
    "validators": ValidatorsParser
}

def get_enabled_parsers(enabled_names):
    """Get enabled parser instances."""
    parsers = []
    for name in enabled_names:
        if name in PARSER_REGISTRY:
            parser_class = PARSER_REGISTRY[name]
            parsers.append(parser_class())
    return parsers