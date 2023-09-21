from abc import ABC, abstractmethod
from typing import Dict, Optional

from saga.models import JobRecord, SagaRecord


class SagaJournal(ABC):
    @abstractmethod
    def get_saga(self, idempotent_key: str) -> Optional[SagaRecord]:
        """
        Returns a saga record associated with idempotent_key.
        """

    @abstractmethod
    def create_saga(self, idempotent_key: str) -> SagaRecord:
        """
        Creates new saga record with unique key idempotent_key.
        """

    @abstractmethod
    def update_saga(self, saga: SagaRecord) -> None:
        """
        Updates saga record.
        """

    @abstractmethod
    def delete_sagas(self, *idempotent_keys: str) -> None:
        """
        Deletes saga records.
        """


class WorkerJournal(ABC):
    """
    Abstract journal to keep track of all executed operations inside any saga.
    Each saga must have a unique idempotent key associated with it. All operations
    inside saga will be stored/updated via appropriate methods and also have a unique key.
    """

    @abstractmethod
    def get_record(self, idempotent_operation_id: str) -> Optional[JobRecord]:
        """
        Returns a job record associated with idempotent_operation_id.
        """

    @abstractmethod
    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        """
        Creates new job record with unique key idempotent_operation_id.
        """

    @abstractmethod
    def update_record(self, record: JobRecord) -> None:
        """
        Updates job record.
        """

    @abstractmethod
    def delete_records(self, *records: JobRecord) -> None:
        """
        Deletes job records.
        """


class MemorySagaJournal(SagaJournal):

    def __init__(self, sagas: Optional[Dict[str, SagaRecord]] = None) -> None:
        self._sagas: Dict[str, SagaRecord] = sagas if sagas is not None else {}

    def get_saga(self, idempotent_key: str) -> Optional[SagaRecord]:
        return self._sagas.get(idempotent_key)

    def create_saga(self, idempotent_key: str) -> SagaRecord:
        self._sagas[idempotent_key] = SagaRecord(
            idempotent_key=idempotent_key
        )
        return self._sagas[idempotent_key]

    def update_saga(self, saga: SagaRecord) -> None:
        self._sagas[saga.idempotent_key] = saga

    def delete_sagas(self, *idempotent_keys: str) -> None:
        for key in idempotent_keys:
            del self._sagas[key]


class MemoryJournal(WorkerJournal):
    def __init__(self, records: Optional[Dict[str, JobRecord]] = None,
                 sagas: Optional[Dict[str, SagaRecord]] = None) -> None:
        self._records: Dict[str, JobRecord] = records if records is not None else {}

    def get_record(self, idempotent_operation_id: str) -> Optional[JobRecord]:
        return self._records.get(idempotent_operation_id)

    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        self._records[idempotent_operation_id] = JobRecord(
            idempotent_operation_id=idempotent_operation_id
        )
        return self._records[idempotent_operation_id]

    def update_record(self, record: JobRecord) -> None:
        self._records[record.idempotent_operation_id] = record

    def delete_records(self, *records: JobRecord) -> None:
        for r in records:
            del self._records[r.idempotent_operation_id]
