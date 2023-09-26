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
    A WorkerJob is responsible for running functions inside any saga function.
    The main goal of this object is to make control points where execution can continue if
    unexpected shutdown happened.

    def any_function() -> int:
        ...

    job = WorkerJob(JobSpec(any_function, *args, **kwargs))

    After creating WorkerJob with function "any_function" there's a feature called "compensation".
    This feature adds associated method with the return value of function "any_function" which
    can be used to rollback "any_function":

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
        """
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[..., C]] = None
        self._run: bool = False
        self._crun: bool = False

    def run(self) -> T:
        """
        Runs the main function.
        This method can only be executed once.
        :return: Result of the main funtion.
        """
        assert not self._run, 'Main function has already been executed. Create a new job to run ' \
                              'another function.'
        r = self._spec.call()
        self._run = True
        if self._compensation_spec:
            self._compensation_callback(self._compensation_spec.with_arg(r))
        return r

    def with_compensation(self, f: Callable[Concatenate[T, P], C], *args: P.args,
                          **kwargs: P.kwargs) -> 'WorkerJob[T, C]':
        """
        Adds a compensation function which will be called if any exception happens after running the
        main function but only if the main function has run.
        :param f: Compensation function. The first argument is always a result of the main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        :return: The same WorkerJob object.
        """
        self._compensation_spec = JobSpec(f, *args, **kwargs)
        return self

    def compensate(self) -> None:
        """
        Runs a compensation of the main function if a compensation exists.
        This method can only be executed once.
        """
        assert self._run, 'Main function has not been executed. Nothing to compensate.'
        assert not self._crun, 'Compensation function has been already executed.'
        if self._compensation_spec is not None:
            self._compensation_spec.call()


class SagaWorker:
    """
    A SagaWorker is responsible for creating jobs (WorkerJob) inside saga function.
    SagaWorker creates execution control points on every job created and run and also collects
    all compensations that has been linked to jobs to run all of them on exception.

    journal = WorkerJournal()  # a journal where control points are stored
    worker = SagaWorker('1')
    try:
        worker.job(any_function).with_compensation(rollback_any_function).run()
        raise StrangeException
    except StrangeException:
        worker.compensator.run()
        raise
    """

    def __init__(self, idempotent_key: str, journal: WorkerJournal,
                 compensator: SagaCompensator, sender: Optional[EventSender]):
        """
        :param idempotent_key: Unique key of a worker.
        :param journal: Worker journal to store job execution results.
        :param compensator: Compensator object to rollback executed jobs.
        :param sender: EventSender object to send events if omitted `event` method cannot be used.
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
        Unique idempotent key of the worker.
        """
        return self._idempotent_key

    def compensate(self) -> None:
        """
        Runs all compensations if any.
        """
        self._compensate.run()

    def forget_done(self) -> None:
        """
        Forgets all records in the journal allowing run with the same idempotent key.
        """
        self._memo.forget_done()

    def job(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> WorkerJob[T, None]:
        """
        Creates a WorkerJob with main function f.
        :param f: Main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        """
        return WorkerJob[T, None](
            JobSpec(self._memo.memoize(f), *args, **kwargs),
            comp_set_callback=self._place_compensation
        )

    def event_job(self, f: Callable[P, Event[In, Out]], *args: P.args,
                  **kwargs: P.kwargs) -> WorkerJob[Out, Event[Any, Any]]:
        """
        Creates a WorkerJob that sends returning event and waits it to come back.
        :param f: A function that returns an event.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
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
