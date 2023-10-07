import functools
from typing import Any, Callable, Optional, ParamSpec, Tuple, TypeVar
from uuid import UUID

from saga.compensator import SagaCompensator
from saga.events import EventSender
from saga.journal import WorkerJournal
from saga.logger import logger
from saga.memo import Memoized
from saga.models import Event, In, JobSpec, NotAnEvent, Ok, Out
from saga.worker_job import WorkerJob

P = ParamSpec('P')
T = TypeVar('T')
SAGA_KEY_SEPARATOR = '&'


class SagaWorker:
    """
    `SagaWorker` отвечает за создание `WorkerJob`.
    `SagaWorker` создает точки сохранения для каждого запускаемого `WorkerJob`,
     а также собирает все компенсации, которые были в них добавлены.

         journal = WorkerJournal()  # журнал сохранения контрольных точек
         worker = SagaWorker('1')

         worker.job(any_function).with_compensation(rollback_any_function).run()
         worker.compensate()
    """

    def __init__(self, uuid: UUID, saga_name: str, journal: WorkerJournal,
                 compensator: SagaCompensator, sender: Optional[EventSender],
                 compensation_max_retries: int = 10, compensation_interval: float = 1,
                 compensation_event_timeout: float = 5):
        """
        :param uuid: Уникальный ключ `SagaWorker`.
        :param saga_name: Имя саги.
        :param journal: Журнал для хранения результатов выполнения `WorkerJob`.
        :param compensator: Объект `SagaCompensator` хранения компенсационных функций.
        :param sender: Объект `EventSender` для отправки событий,
                       если опущенный метод `event_job` не может быть использован.
        :param compensation_max_retries: Количество возможных повторов функции компенсации в случае
                                         исключения в ней. Если количество повторов 0,
                                         тогда будет поднято оригинальное исключение.
        :param compensation_interval: Интервал времени в секундах, через который будет вызван повтор
                                      функции компенсации в случае исключения.
        :param compensation_event_timeout: Время ожидания события компенсации.
        """
        self._uuid = uuid
        self._saga_name = saga_name
        self._idempotent_key = join_key(uuid, saga_name)
        self._memo = Memoized(self._idempotent_key, journal)
        self._sender = sender
        self._journal = journal
        self._compensate = compensator or SagaCompensator()
        self._compensation_max_retries = compensation_max_retries
        self._compensation_interval = compensation_interval
        self._compensation_event_timeout = compensation_event_timeout
        self._w_prefix = f'[W: {uuid} S: {saga_name}]'

    @property
    def idempotent_key(self) -> str:
        """
        Уникальный идемпотентный ключ `SagaWorker`.
        """
        return self._idempotent_key

    @property
    def uuid(self) -> UUID:
        """
        Уникальный идентификатор `SagaWorker`.
        """
        return self._uuid

    def compensate(self) -> None:
        """
        Запустить все компенсационные функции.
        """
        logger.info(f'{self._w_prefix} Запуск заданий компенсаций.')
        self._compensate.run()

    def forget_done(self) -> None:
        """
        Удалить все добавленные записи в журнал, позволяя запуск с тем же идемпотентным ключом.
        """
        logger.info(f'{self._w_prefix} Удаление записей выполнение заданий из журнала.')
        self._memo.forget_done()

    def job(self, spec: JobSpec[T, P], retries: int = 1, retry_interval: float = 2.0)\
            -> WorkerJob[T, None, P]:
        """
        Создать `WorkerJob` со спецификацией `spec`.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        """
        spec.f = self._memo.memoize(spec.f, retries=retries, retry_interval=retry_interval)
        logger.debug(f'{self._w_prefix} Создание простого задания с функцией "{spec.name}".')
        return WorkerJob[T, None, P](spec, comp_set_callback=self._place_compensation,
                                     uuid=self._uuid, saga_name=self._saga_name)

    def event_job(self, spec: JobSpec[Event[In, Out], P], retries: int = 1,
                  retry_interval: float = 2.0, timeout: float = 10.0) \
            -> WorkerJob[Out, Event[Any, Any], P]:
        """
        Создать `WorkerJob`, который отправляет возвращаемое событие и ожидает его результат.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        :param timeout: Время ожидания ответного события.
        """
        assert self._sender is not None, 'Не установлен отправитель событий.'
        spec.f = self._memo.memoize(self._auto_send(spec.f,  # type: ignore[arg-type]
                                                    timeout=timeout),
                                    retries=retries, retry_interval=retry_interval)
        logger.debug(f'{self._w_prefix} Создание событийного задания с функцией "{spec.name}".')
        return WorkerJob[Out, Event[Any, Any], P](spec,  # type: ignore[arg-type]
                                                  comp_set_callback=self._place_event_compensation,
                                                  uuid=self._uuid, saga_name=self._saga_name)

    def _place_event_compensation(self, spec: JobSpec[Event[Any, Ok], ...]) -> None:
        spec.f = self._memo.memoize(self._auto_send(spec.f,  # type: ignore[arg-type]
                                                    timeout=self._compensation_event_timeout,
                                                    cancel_previous_uuid=True),
                                    retries=self._compensation_max_retries,
                                    retry_interval=self._compensation_interval)
        logger.debug(f'{self._w_prefix} Добавление событийного задания компенсации "{spec.name}".')
        self._compensate.add_compensate(spec)

    def _place_compensation(self, spec: JobSpec[None, ...]) -> None:
        spec.f = self._memo.memoize(spec.f, retries=self._compensation_max_retries,
                                    retry_interval=self._compensation_interval)
        logger.debug(f'{self._w_prefix} Добавление простого задания компенсации "{spec.name}".')
        self._compensate.add_compensate(spec)

    def _auto_send(self, f: Callable[P, Event[Any, Out]], timeout: float,
                   cancel_previous_uuid: bool = False) -> Callable[P, Out]:
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> Out:
            assert self._sender is not None, 'Не установлен отправитель событий.'
            event = f(*args, **kwargs)
            if isinstance(event, NotAnEvent):
                return Ok()  # type: ignore[return-value]
            self._sender.send(self._uuid, event, cancel_previous_uuid=cancel_previous_uuid)
            return self._sender.wait(self._uuid, event, timeout=timeout)
        return wrap


def join_key(uuid: UUID, saga_name: str) -> str:
    """
    Вернуть строку, которую можно использовать для получения объекта
    `SagaRecord` из `SagaJournal`.
    """
    return f'{uuid}{SAGA_KEY_SEPARATOR}{saga_name}'


def split_key(joined_key: str) -> Tuple[str, str]:
    uuid, *name = joined_key.split(SAGA_KEY_SEPARATOR)
    return uuid, ''.join(name)
