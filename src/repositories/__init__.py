from src.repositories.attestation_repository import AttestationRepository
from src.repositories.beacon_block_repository import BeaconBlockRepository
from src.repositories.blob_sidecar_repository import BlobSidecarRepository
from src.repositories.committee_repository import CommitteeRepository, SyncCommitteeRepository
from src.repositories.deposit_repository import DepositRepository
from src.repositories.execution_payload_repository import (
    ExecutionPayloadRepository,
    TransactionRepository,
    WithdrawalRepository
)
from src.repositories.reward_repository import (
    BlockRewardRepository,
    AttestationRewardRepository,
    SyncCommitteeRewardRepository
)
from src.repositories.validator_repository import ValidatorRepository
from src.repositories.voluntary_exit_repository import (
    VoluntaryExitRepository,
    ProposerSlashingRepository,
    AttesterSlashingRepository
)