from .electra import ElectraParser


class FuluParser(ElectraParser):
    """Fulu parser. Block shape remains compatible with Electra for core tables."""

    def __init__(self):
        super().__init__()
        self.fork_name = "fulu"

    def _get_fallback_fork_version(self) -> str:
        """Get fallback fork version for Fulu (mainnet)."""
        return "0x06000000"
