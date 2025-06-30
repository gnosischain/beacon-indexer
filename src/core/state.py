"""Enhanced state management types."""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class StateStatus(Enum):
    """Status of an indexing range."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    VALIDATING = "validating"


@dataclass
class IndexingRange:
    """Represents a range of slots being indexed."""
    mode: str
    dataset: str
    start_slot: int
    end_slot: int
    status: StateStatus
    worker_id: Optional[str] = None
    batch_id: Optional[str] = None
    attempt_count: int = 0
    rows_indexed: Optional[int] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    version: Optional[datetime] = None
    
    def __repr__(self):
        return f"IndexingRange({self.dataset} {self.start_slot}-{self.end_slot} {self.status.value})"