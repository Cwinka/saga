import functools
from typing import Any, Callable, Concatenate, Generic, Optional, ParamSpec, TypeVar

from saga.compensator import SagaCompensator
from saga.events import EventSender
from saga.journal import WorkerJournal
from saga.memo import Memoized
from saga.models import Event, In, JobSpec, NotAnEvent, Ok, Out

P = ParamSpec('P')
T = TypeVar('T')
C = TypeVar('C')
CompensationCallback = Callable[[JobSpec[..., C]], None]


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

    def __init__(self, spec: JobSpec[..., T],
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
        self._compensation_spec: Optional[JobSpec[..., C]] = None
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

    def __init__(self, idempotent_key: str, journal: WorkerJournal,
                 compensator: SagaCompensator, sender: Optional[EventSender]):
        """
        :param idempotent_key: Уникальный ключ `SagaWorker`.
        :param journal: Журнал для хранения результатов выполнения `WorkerJob`.
        :param compensator: Объект `SagaCompensator` хранения компенсационных функций.
        :param sender: Объект `EventSender` для отправки событий,
                       если опущенный метод `event_job` не может быть использован.
        """
        self._memo = Memoized(idempotent_key, journal)
        self._sender = sender
        self._idempotent_key = idempotent_key
        self._journal = journal
        self._compensate = compensator or SagaCompensator()
        self._no_event_comp: bool = False

    @property
    def idempotent_key(self) -> str:
        """
        Уникальный идемпотентный ключ.
        """
        return self._idempotent_key

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

    def job(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> WorkerJob[T, None]:
        """
        Создать `WorkerJob` с основной функцией f.

        :param f: Основная функция.
        :param args: Любые аргументы для передачи в функцию f.
        :param kwargs: Любые ключевые аргументы для передачи в функцию f.
        """
        return WorkerJob[T, None](
            JobSpec(self._memo.memoize(f), *args, **kwargs),
            comp_set_callback=self._place_compensation
        )

    def event_job(self, f: Callable[P, Event[In, Out]], *args: P.args,
                  **kwargs: P.kwargs) -> WorkerJob[Out, Event[Any, Any]]:
        """
        Создать `WorkerJob`, который отправляет возвращаемое событие и ожидает его результат.

        :param f: Функция, возвращающая событие.
        :param args: Любые аргументы для передачи в функцию f.
        :param kwargs: Любые ключевые аргументы для передачи в функцию f.
        """
        assert self._sender is not None, 'Не установлен отправитель событий.'
        return WorkerJob[Out, Event[Any, Any]](
            JobSpec(self._memo.memoize(self._auto_send(f)), *args, **kwargs),
            comp_set_callback=self._place_event_compensation
        )

    def _place_event_compensation(self, spec: JobSpec[..., Event[In, Ok]]) -> None:
        if self._no_event_comp:
            self._no_event_comp = False
            return
        spec.f = self._memo.memoize(self._auto_send(spec.f))  # type: ignore[arg-type]
        self._compensate.add_compensate(spec)

    def _place_compensation(self, spec: JobSpec[..., None]) -> None:
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
            self._sender.send(event)
            return self._sender.wait(event)
        return wrap
