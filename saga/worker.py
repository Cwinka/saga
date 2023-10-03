import functools
from typing import Any, Callable, Concatenate, Generic, Optional, ParamSpec, Tuple, TypeVar
from uuid import UUID

from saga.compensator import SagaCompensator
from saga.events import EventSender
from saga.journal import WorkerJournal
from saga.memo import Memoized
from saga.models import Event, In, JobSpec, NotAnEvent, Ok, Out

P = ParamSpec('P')
T = TypeVar('T')
C = TypeVar('C')
CompensationCallback = Callable[[JobSpec[C]], None]
SAGA_KEY_SEPARATOR = '&'


class WorkerJob(Generic[T, C]):
    """
    `WorkerJob` унифицированный объект для запуска функций:

        def any_function() -> int:
            ...

        job = WorkerJob(JobSpec(any_function, *args, **kwargs))
        job.run()

    Для того, чтобы откатить результат выполнения функции `any_function` необходимо добавить
    компенсирующую функцию, которая первым аргументом принимает возвращаемое значение
    `any_function`:

        def rollback_any_function(result_of_any_function: int) -> None:
            ...

        job.with_compensation(rollback_any_function)
        job.run()

        ...

        job.compensate()
    """

    def __init__(self, spec: JobSpec[T],
                 comp_set_callback: CompensationCallback[C] = lambda *_: None) -> None:
        """
        :param spec: A function specification.
        :param comp_set_callback: Compensation callback that is called when a function in spec is
                                  executed and a compensation function is set to the job.

        :param spec: Спецификация функции.
        :param comp_set_callback: Обратный вызов компенсации, вызывается, когда spec выполнена,
                                  и установлена функция компенсации.
        """
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[C]] = None
        self._run: bool = False
        self._crun: bool = False

    def run(self) -> T:
        """
        Запускает spec функцию. Этот метод можно выполнить только один раз.

        :return: Результат spec функции.
        """
        assert not self._run, 'Повторный вызов функции не позволен.'
        r = self._spec.call()
        self._run = True
        if self._compensation_spec:
            self._compensation_callback(self._compensation_spec.with_arg(r))
        return r

    def with_compensation(self, f: Callable[Concatenate[T, P], C], *args: P.args,
                          **kwargs: P.kwargs) -> 'WorkerJob[T, C]':
        """
        Добавляет функцию компенсации.

        :param f: Функция компенсации. Первый аргумент всегда является результатом spec функции.
        :param args: Любые аргументы для передачи в функцию f.
        :param kwargs: Любые ключевые аргументы для передачи в функцию f.
        :return: Тот же объект `WorkerJob`.
        """
        self._compensation_spec = JobSpec(f, *args, **kwargs)
        return self

    def compensate(self) -> None:
        """
        Запускает компенсацию, если она существует. Этот метод можно выполнить только один раз.
        """
        assert self._run, 'Функция не была вызвана. Нечего компенсировать.'
        assert not self._crun, 'Повторный вызов компенсационной функции не позволен.'
        if self._compensation_spec is not None:
            self._compensation_spec.call()


class SagaWorker:
    """
    A SagaWorker is responsible for creating jobs (WorkerJob) inside saga function.
    SagaWorker creates execution control points on every job created and run and also collects
    all compensations that has been linked to jobs to run all of them on exception.

    `SagaWorker` отвечает за создание `WorkerJob`.
    `SagaWorker` создает точки сохранения для каждого запускаемого `WorkerJob`,
     а также собирает все компенсации, которые были в них добавлены.

         journal = WorkerJournal()  # журнал сохранения контрольных точек
         worker = SagaWorker('1')

         worker.job(any_function).with_compensation(rollback_any_function).run()
         worker.compensate()
    """

    def __init__(self, uuid: UUID, saga_name: str, journal: WorkerJournal,
                 compensator: SagaCompensator, sender: Optional[EventSender]):
        """
        :param uuid: Уникальный ключ `SagaWorker`.
        :param saga_name: Имя саги.
        :param journal: Журнал для хранения результатов выполнения `WorkerJob`.
        :param compensator: Объект `SagaCompensator` хранения компенсационных функций.
        :param sender: Объект `EventSender` для отправки событий,
                       если опущенный метод `event_job` не может быть использован.
        """
        self._uuid = uuid
        self._idempotent_key = join_key(uuid, saga_name)
        self._memo = Memoized(self._idempotent_key, journal)
        self._sender = sender
        self._journal = journal
        self._compensate = compensator or SagaCompensator()
        self._no_event_comp: bool = False

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
        self._compensate.run()

    def forget_done(self) -> None:
        """
        Удалить все добавленные записи в журнал, позволяя запуск с тем же идемпотентным ключом.
        """
        self._memo.forget_done()

    def job(self, spec: JobSpec[T], retries: int = 1, retry_interval: float = 2.0)\
            -> WorkerJob[T, None]:
        """
        Создать `WorkerJob` со спецификацией `spec`.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        """
        spec.f = self._memo.memoize(spec.f, retries=retries, retry_interval=retry_interval)
        return WorkerJob[T, None](spec, comp_set_callback=self._place_compensation)

    def event_job(self, spec: JobSpec[Event[In, Out]], retries: int = 1,
                  retry_interval: float = 2.0) -> WorkerJob[Out, Event[Any, Any]]:
        """
        Создать `WorkerJob`, который отправляет возвращаемое событие и ожидает его результат.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        """
        assert self._sender is not None, 'Не установлен отправитель событий.'
        spec.f = self._memo.memoize(self._auto_send(spec.f),  # type: ignore[arg-type]
                                    retries=retries, retry_interval=retry_interval)
        return WorkerJob[Out, Event[Any, Any]](spec,  # type: ignore[arg-type]
                                               comp_set_callback=self._place_event_compensation)

    def _place_event_compensation(self, spec: JobSpec[Event[In, Ok]]) -> None:
        if self._no_event_comp:
            self._no_event_comp = False
            return
        spec.f = self._memo.memoize(self._auto_send(spec.f))  # type: ignore[arg-type]
        self._compensate.add_compensate(spec)

    def _place_compensation(self, spec: JobSpec[None]) -> None:
        spec.f = self._memo.memoize(spec.f)
        self._compensate.add_compensate(spec)

    def _auto_send(self, f: Callable[P, Event[Any, Out]]) -> Callable[P, Out]:
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> Out:
            assert self._sender is not None, 'Не установлен отправитель событий.'
            event = f(*args, **kwargs)
            if isinstance(event, NotAnEvent):
                self._no_event_comp = True
                return Ok()  # type: ignore[return-value]
            event.ret_name = f'{self._idempotent_key}_{event.ret_name}'
            self._sender.send(self._uuid, event)
            return self._sender.wait(event)
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
