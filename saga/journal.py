from abc import ABC, abstractmethod
from typing import Optional, Dict

from saga.models import JobRecord


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
        :param idempotent_operation_id: Unique key of a job record.
        """
        pass

    @abstractmethod
    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        """
        Creates new job record with unique key idempotent_operation_id.
        """
        pass

    @abstractmethod
    def update_record(self, record: JobRecord) -> None:
        """
        Updates job record.
        """
        pass


class MemoryJournal(WorkerJournal):
    def __init__(self, records: Optional[Dict[str, JobRecord]] = None) -> None:
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
