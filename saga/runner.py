import inspect
from typing import Any, Callable, Dict, Optional, Type, TypeVar
from uuid import UUID

from saga.compensator import SagaCompensator
from saga.events import CommunicationFactory
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.logger import logger
from saga.models import M, SagaRecord
from saga.saga import Saga, SagaJob, model_from_initial_data, model_to_initial_data
from saga.worker import SagaWorker, join_key, split_key

T = TypeVar('T')
SAGA_NAME_ATTR = '__saga_name__'


class SagaRunner:
    """
    ``SagaRunner`` - это фабричный класс, для создания объектов ``SagaJob``.

    Чтобы зарегистрировать функцию в качестве саги, существует два метода:

        # использование декоратора
        @idempotent_saga('saga_name')
        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...

        # регистрация функции саги вручную:
        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...
        SagaRunner.register_saga('saga_name', my_saga)

    Для создания саги используется метод ``new``:

        runner = SagaRunner(...)
        saga = runner.new(UUID, my_saga, Ok())

    Первым аргументом метода ``new`` является уникальный идемпотентный ключ, представленный в виде
    любого ``UUID``. Если сага запускается с используемым одного и того же идемпотентного ключа,
    сага вернет одинаковый результат для обоих запусков.

    Любая запущенная сага может оказаться в незавершенном состоянии (ни в ``SagaStatus.DONE``
    ни в ``SagaStatus.FAILED``, а в ``SagaStatus.RUNNING`` состоянии) из-за неожиданного завершения
    работы программы. Незавершенные саги могут быть выполнены повторно:

        journal = SagaJournalImplementation()  # реализация журнала
        runner = SagaRunner(saga_journal=journal)  # по умолчанию используются журналы в памяти

        runner.run_incomplete()  # возвращает количество запущенных саг.
    """
    _sagas: Dict[str, Callable[[SagaWorker, M], Any]] = {}
    _r_prefix = '[Runner]'

    def __init__(self,
                 saga_journal: Optional[SagaJournal] = None,
                 worker_journal: Optional[WorkerJournal] = None,
                 cfk: Optional[CommunicationFactory] = None,
                 forget_done: bool = False,
                 model_to_b: Callable[[M], bytes] = model_to_initial_data,
                 model_from_b: Callable[[Type[M], bytes], M] = model_from_initial_data,
                 job_max_retries: int = 1, job_retry_interval: float = 2.0,
                 event_job_timeout: float = 5.0,
                 compensation_max_retries: int = 10, compensation_interval: float = 1,
                 compensation_event_timeout: float = 5):
        """
        :param saga_journal: Журнал записей саг.
        :param worker_journal: Журнал записей шагов саги.
        :param cfk: Фабрика для использования событий внутри саги.
        :param forget_done: Если True, после выполнения, все записи выполненной саги удаляться.
        :param model_to_b: Функция конвертации модели данных в байты.
        :param model_from_b: Функция конвертации байт в модель данных.
        :param job_max_retries: Максимальное количество повторов задания. При указании больше чем
                                1 задания должны быть идемпотентными.
        :param job_retry_interval: Время, через которое будет совершен повторный вызов задания в
                                   случае исключения.
        :param event_job_timeout: Максимальное время ожидания ответа события.
        :param compensation_max_retries: Количество возможных повторов функции компенсации в случае
                                 исключения в ней. Если количество повторов 0,
                                 тогда будет поднято оригинальное исключение.
        :param compensation_interval: Интервал времени в секундах, через который будет вызван повтор
                                      функции компенсации в случае исключения.
        :param compensation_event_timeout: Время ожидания события компенсации.
        """
        self._forget_done = forget_done
        self._cfk = cfk
        self._worker_journal = worker_journal or MemoryJournal()
        self._saga_journal = saga_journal or MemorySagaJournal()
        self._model_to_b = model_to_b
        self._model_from_b = model_from_b
        self._job_max_retries = job_max_retries
        self._job_retry_interval = job_retry_interval
        self._event_job_timeout = event_job_timeout
        self._compensation_max_retries = compensation_max_retries
        self._compensation_interval = compensation_interval
        self._compensation_event_timeout = compensation_event_timeout

    def new(self, uuid: UUID, saga: Saga[M, T], data: M) -> SagaJob[T, M]:
        """
        Создать новый объект ``SagaJob``.

        :param uuid: Уникальный ключ саги.
        :param saga: Зарегистрированная функция саги.
        :param data: Входные данные саги.
        """
        saga_name = self.get_saga_name(saga)
        worker = SagaWorker(uuid=uuid,
                            saga_name=saga_name,
                            journal=self._worker_journal,
                            compensator=SagaCompensator(),
                            sender=self._cfk.sender() if self._cfk is not None else None,
                            job_max_retries=self._job_max_retries,
                            job_retry_interval=self._job_retry_interval,
                            event_job_timeout=self._event_job_timeout,
                            compensation_max_retries=self._compensation_max_retries,
                            compensation_interval=self._compensation_interval,
                            compensation_event_timeout=self._compensation_event_timeout)
        logger.info(f'{self._r_prefix} Создание контекста саги: SJ: {uuid} S: {saga_name}.')
        return SagaJob(self._saga_journal, worker, saga, data, forget_done=self._forget_done,
                       model_to_b=self._model_to_b)

    def new_from(self, uuid: UUID, saga: Saga[M, T],
                 _record: Optional[SagaRecord] = None) -> Optional[SagaJob[T, M]]:
        """
        Создать экземпляр ``SagaJob`` по существующей записи ``SagaRecord``. Существующая запись
        ``SagaRecord`` говорит о том, что сага была запущена ранее, и может находиться в любом
        состоянии.
        """
        saga_name = self.get_saga_name(saga)
        record = _record if _record is not None else self._saga_journal.get_saga(
            join_key(uuid, saga_name)
        )
        if record is None:
            return None
        model = self._model_from_saga_f(saga)
        logger.info(f'{self._r_prefix} Воссоздание саги: SJ: {uuid} S: {saga_name}.')
        return self.new(uuid, saga, self._model_from_b(model, record.initial_data))

    def run_incomplete(self) -> int:
        """
        Запустить все незавершенные саги. Возвращает количество запущенных саг. Не блокирует
        выполнение.
        """
        i = 0
        logger.info(f'{self._r_prefix} Перезапуск незавершенных саг.')
        for i, record in enumerate(self._saga_journal.get_incomplete_saga(), 1):
            uuid, saga_name = split_key(record.idempotent_key)
            saga_f: Callable[[SagaWorker, M], Any] = self.get_saga(saga_name)
            self.new_from(UUID(uuid), saga_f, _record=record).run()  # type: ignore[union-attr]
        return i

    @staticmethod
    def _model_from_saga_f(saga_f: Callable[[SagaWorker, M], Any]) -> Type[M]:
        data_arg = inspect.getfullargspec(saga_f).args[1]
        model: Type[M] = saga_f.__annotations__.get(data_arg)  # type: ignore[assignment]
        assert not isinstance(model, str), ('Используйте явную аннотацию типа данных саги '
                                            f'"{saga_f.__name__}", без '
                                            'оборачивания его в строку.')
        assert model is not None, f'Модель данных саги "{saga_f.__name__}" не может быть None.'
        return model

    @classmethod
    def register_saga(cls, name: str, saga: Saga[M, Any]) -> None:
        """
        Зарегистрировать функцию saga с именем name.
        """
        logger.debug(f'{cls._r_prefix} Регистрация саги "{name}".')
        setattr(saga, SAGA_NAME_ATTR, name)
        cls._sagas[name] = saga

    @classmethod
    def get_saga(cls, name: str) -> Saga[M, Any]:
        """
        Получить зарегистрированную функцию саги с именем name.
        """
        saga = cls._sagas.get(name)
        assert saga is not None, (f'Сага "{name}" не найдена. Возможно: сагу переименовали, '
                                  f'но в базе данных осталось старое имя саги; сага '
                                  f'"{name}" не была запущена; сага "{name}" не зарегистрирована.')
        return saga

    @classmethod
    def get_saga_name(cls, saga: Saga[M, T]) -> str:
        """
        Получить имя зарегистрированной саги saga.
        """
        assert hasattr(saga, SAGA_NAME_ATTR), cls._not_a_saga_msg(saga)
        return getattr(saga, SAGA_NAME_ATTR)  # type: ignore[no-any-return]

    def get_saga_record_by_uid(self, idempotent_key: UUID, saga: Saga[M, Any]) \
            -> Optional[SagaRecord]:
        """
        Получить запись о состоянии запущенной саги saga, с идемпотентным ключом
        idempotent_key.
        """
        key = join_key(idempotent_key, self.get_saga_name(saga))
        return self._saga_journal.get_saga(key)

    def get_saga_record_by_wkey(self, worker_idempotent_key: str) -> Optional[SagaRecord]:
        """
        Получить запись о состоянии запущенной саги по идемпотентному ключу
        worker_idempotent_key.
        :param worker_idempotent_key: Идемпотентный ключ SagaWorker.
        """
        return self._saga_journal.get_saga(worker_idempotent_key)

    @staticmethod
    def _not_a_saga_msg(f: Callable[..., Any]) -> str:
        return (f'Функция "{f.__name__}" не является сагой. '
                f'Используйте декоратор "{idempotent_saga.__name__}"'
                f' чтобы отметить функцию как сагу.')


def idempotent_saga(name: str) -> Callable[[Saga[M, T]], Saga[M, T]]:
    """
    Зарегистрировать функцию, как сагу, в ``SagaRunner`` с именем name.
    """
    def decorator(f: Saga[M, T]) -> Saga[M, T]:
        SagaRunner.register_saga(name, f)
        return f
    return decorator
