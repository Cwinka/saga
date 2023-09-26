import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from saga.models import JobRecord, JobStatus, SagaRecord


class SagaJournal(ABC):
    """
    Абстрактный журнал саги для отслеживания выполнения любой саги.
    С каждой сагой связан уникальный идемпотентный ключ, который может быть использован в
    качестве первичного ключа в базе данных.
    """

    @abstractmethod
    def get_saga(self, idempotent_key: str) -> Optional[SagaRecord]:
        """
        Получить запись `SagaRecord`, связанную с idempotent_key.
        """

    @abstractmethod
    def create_saga(self, idempotent_key: str) -> SagaRecord:
        """
        Создать новую запись `SagaRecord` с уникальным ключом idempotent_key.
        """

    @abstractmethod
    def update_saga(self, idempotent_key: str, fields: List[str], values: List[Any]) -> None:
        """
        Обновить запись `SagaRecord` с уникальным ключом idempotent_key.

        :param idempotent_key: Уникальный ключ записи `SagaRecord`.
        :param fields: Поля, которые необходимо обновить.
        :param values: Значения обновляемых полей. Значения гарантированно того же типа, что и
                       поле.
        """

    @abstractmethod
    def delete_sagas(self, *idempotent_keys: str) -> None:
        """
        Удалить записи `SagaRecord` с уникальными ключами idempotent_keys.
        """

    @abstractmethod
    def get_incomplete_saga(self) -> List[SagaRecord]:
        """
        Получить все незавершенные `SagaRecord`. Незавершенными записи считаются те, которые
        имеют статус `JobStatus.RUNNING`.
        """


class WorkerJournal(ABC):
    """
    Абстрактный журнал для отслеживания выполненных операций внутри саги.
    """

    @abstractmethod
    def get_record(self, idempotent_operation_id: str) -> Optional[JobRecord]:
        """
        Получить запись `JobRecord`, связанную с уникальным ключом idempotent_operation_id.
        """

    @abstractmethod
    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        """
        Создать запись `JobRecord`, с уникальным ключом idempotent_operation_id.
        """

    @abstractmethod
    def update_record(self, idempotent_operation_id: str, fields: List[str],
                      values: List[Any]) -> None:
        """
        Обновить запись `JobRecord` с уникальным ключом idempotent_operation_id.

        :param idempotent_operation_id: Уникальный ключ записи `JobRecord`.
        :param fields: Поля, которые необходимо обновить.
        :param values: Значения обновляемых полей. Значения гарантированно того же типа, что и
                       поле.
        """

    @abstractmethod
    def delete_records(self, *records: JobRecord) -> None:
        """
        Deletes job records.
        """


class MemorySagaJournal(SagaJournal):

    _lock = threading.Lock()

    def __init__(self, sagas: Optional[Dict[str, SagaRecord]] = None) -> None:
        self._sagas: Dict[str, SagaRecord] = sagas if sagas is not None else {}

    def get_saga(self, idempotent_key: str) -> Optional[SagaRecord]:
        with self._lock:
            return self._sagas.get(idempotent_key)

    def create_saga(self, idempotent_key: str) -> SagaRecord:
        with self._lock:
            self._sagas[idempotent_key] = SagaRecord(
                idempotent_key=idempotent_key
            )
            return self._sagas[idempotent_key]

    def update_saga(self, idempotent_key: str, fields: List[str], values: List[Any]) -> None:
        with self._lock:
            saga = self._sagas[idempotent_key]
            for field, value in zip(fields, values):
                setattr(saga, field, value)

    def delete_sagas(self, *idempotent_keys: str) -> None:
        with self._lock:
            for key in idempotent_keys:
                del self._sagas[key]

    def get_incomplete_saga(self) -> List[SagaRecord]:
        with self._lock:
            return [x for x in self._sagas.values() if x.status == JobStatus.RUNNING]


class MemoryJournal(WorkerJournal):
    _lock = threading.Lock()

    def __init__(self, records: Optional[Dict[str, JobRecord]] = None) -> None:
        self._records: Dict[str, JobRecord] = records if records is not None else {}

    def get_record(self, idempotent_operation_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._records.get(idempotent_operation_id)

    def create_record(self, idempotent_operation_id: str) -> JobRecord:
        with self._lock:
            self._records[idempotent_operation_id] = JobRecord(
                idempotent_operation_id=idempotent_operation_id
            )
            return self._records[idempotent_operation_id]

    def update_record(self, idempotent_operation_id: str, fields: List[str],
                      values: List[Any]) -> None:
        with self._lock:
            record = self._records[idempotent_operation_id]
            for field, value in zip(fields, values):
                setattr(record, field, value)

    def delete_records(self, *records: JobRecord) -> None:
        with self._lock:
            for r in records:
                del self._records[r.idempotent_operation_id]
