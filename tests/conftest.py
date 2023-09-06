from typing import Dict, Optional

import pytest

from saga.models import JobRecord
from saga.saga import WorkerJournal, SagaCompensate


class MemoryWorkerJournal(WorkerJournal):
    def __init__(self) -> None:
        self._records: Dict[str, JobRecord] = {}

    def get_record(self, idempotent_operation_id: str) -> Optional[JobRecord]:
        return self._records.get(idempotent_operation_id)

    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        self._records[idempotent_operation_id] = JobRecord(
            idempotent_operation_id=idempotent_operation_id
        )
        return self._records[idempotent_operation_id]

    def update_record(self, record: JobRecord) -> None:
        self._records[record.idempotent_operation_id] = record


@pytest.fixture()
def wk_journal() -> WorkerJournal:
    return MemoryWorkerJournal()


@pytest.fixture()
def compensator() -> SagaCompensate:
    return SagaCompensate()
