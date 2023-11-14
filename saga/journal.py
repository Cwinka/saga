import collections
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from uuid import UUID

from saga.models import JobRecord, JobStatus, SagaRecord


class SagaJournal(ABC):
    """
    Абстрактный журнал саги для отслеживания выполнения любой саги.
    С каждой сагой связан уникальный идемпотентный ключ, который может быть использован в
    качестве первичного ключа в базе данных.
    """

    @abstractmethod
    def get_saga(self, uuid: UUID) -> Optional[SagaRecord]:
        """
        Получить запись ``SagaRecord``, связанную с ``idempotent_key``.
        """

    @abstractmethod
    def create_saga(self, uuid: UUID, saga_name: str) -> SagaRecord:
        """
        Создать новую запись ``SagaRecord`` с уникальным ключом ``uuid`` и именем ``saga_name``.
        """

    @abstractmethod
    def update_saga(self, uuid: UUID, fields: List[str], values: List[Any]) -> None:
        """
        Обновить запись ``SagaRecord`` с уникальным ключом ``uuid``.

        :param uuid: Уникальный ключ записи ``SagaRecord``.
        :param fields: Поля, которые необходимо обновить.
        :param values: Значения обновляемых полей. Значения гарантированно того же типа, что и
                       поле.
        """

    @abstractmethod
    def delete_sagas(self, *uuid: UUID) -> None:
        """
        Удалить записи ``SagaRecord`` с уникальными ключами ``uuid``.
        """

    @abstractmethod
    def get_incomplete_saga(self) -> List[SagaRecord]:
        """
        Получить все незавершенные ``SagaRecord``. Незавершенными записи считаются те, которые
        имеют статус ``JobStatus.RUNNING``.
        """


class WorkerJournal(ABC):
    """
    Абстрактный журнал для отслеживания выполненных операций внутри саги.
    """

    @abstractmethod
    def get_record(self, uuid: UUID, operation_id: int) -> Optional[JobRecord]:
        """
        Получить запись ``JobRecord``, связанную с уникальным ключом ``uuid` и номером операции
        ``operation_id``.
        """

    @abstractmethod
    def create_record(self,  uuid: UUID, operation_id: int) -> JobRecord:
        """
        Создать запись ``JobRecord``, с уникальным ключом ``uuid` и номером операции
        ``operation_id``.
        """

    @abstractmethod
    def update_record(self, uuid: UUID, operation_id: int, fields: List[str],
                      values: List[Any]) -> None:
        """
        Обновить запись ``JobRecord`` с уникальным ключом ``idempotent_operation_id``.

        :param uuid: Идентификатор саги.
        :param operation_id: Номер операции.
        :param fields: Поля, которые необходимо обновить.
        :param values: Значения обновляемых полей. Значения гарантированно того же типа, что и
                       поле.
        """

    @abstractmethod
    def delete_records(self, *uuid: UUID) -> None:
        """
        Удалить группы записей ``JobRecord`` имеющие ключи ``uuid``.
        """


class MemorySagaJournal(SagaJournal):

    _lock = threading.Lock()

    def __init__(self, sagas: Optional[Dict[UUID, SagaRecord]] = None) -> None:
        self._sagas: Dict[UUID, SagaRecord] = sagas if sagas is not None else {}

    def get_saga(self, uuid: UUID) -> Optional[SagaRecord]:
        with self._lock:
            return self._sagas.get(uuid)

    def create_saga(self, uuid: UUID, saga_name: str) -> SagaRecord:
        with self._lock:
            self._sagas[uuid] = SagaRecord(
                uuid=uuid,
                saga_name=saga_name
            )
            return self._sagas[uuid]

    def update_saga(self, uuid: UUID, fields: List[str], values: List[Any]) -> None:
        with self._lock:
            saga = self._sagas[uuid]
            for field, value in zip(fields, values):
                setattr(saga, field, value)

    def delete_sagas(self, *uuid: UUID) -> None:
        with self._lock:
            for key in uuid:
                del self._sagas[key]

    def get_incomplete_saga(self) -> List[SagaRecord]:
        with self._lock:
            return [x for x in self._sagas.values() if x.status == JobStatus.RUNNING]


class MemoryJournal(WorkerJournal):
    _lock = threading.Lock()

    def __init__(self, records: Optional[Dict[UUID, Dict[int, JobRecord]]] = None) -> None:
        dct: Dict[UUID, Dict[int, JobRecord]] = collections.defaultdict(dict)
        dct.update(records if records is not None else {})
        self._records = dct

    def get_record(self, uuid: UUID, operation_id: int) -> Optional[JobRecord]:
        with self._lock:
            group = self._records.get(uuid)
            if group is not None:
                return group.get(operation_id)
            return None

    def create_record(self, uuid: UUID, operation_id: int) -> JobRecord:
        with self._lock:
            self._records[uuid][operation_id] = JobRecord(
                uuid=uuid,
                operation_id=operation_id
            )
            return self._records[uuid][operation_id]

    def update_record(self, uuid: UUID, operation_id: int, fields: List[str],
                      values: List[Any]) -> None:
        with self._lock:
            record = self._records[uuid][operation_id]
            for field, value in zip(fields, values):
                setattr(record, field, value)

    def delete_records(self, *uuid: UUID) -> None:
        for key in uuid:
            del self._records[key]
