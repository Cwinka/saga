import copy
from typing import Any, Callable, Concatenate, Generic, Optional, ParamSpec, Type, TypeVar

from saga.compensator import SagaCompensator
from saga.journal import MemoryJournal, WorkerJournal
from saga.memo import Memoized
from saga.models import JobSpec

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
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[..., C]] = None
        self._run: bool = False
        self._crun: bool = False

    def run(self) -> T:
        """
        Runs a main function with associated arguments and keyword arguments.
        This method can only be executed once.
        :return: Result of a funtion.
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
        Adds a compensation function which will be called if any exception happens after running a
        main function but only if the main function has run.
        :param f: Compensation function. The first argument is always a result of a main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        :return: The same WorkerJob object.
        """
        self._compensation_spec = JobSpec(f, *args, **kwargs)
        return self

    def compensate(self) -> None:
        """
        Runs a compensation of the main function is a compensation exists.
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

    default_journal: WorkerJournal = MemoryJournal()  # one journal for all workers

    def __init__(self, idempotent_key: str, journal: WorkerJournal = default_journal,
                 compensator: Optional[SagaCompensator] = None,
                 _memo: Optional[Memoized] = None):
        self._memo = _memo or Memoized(idempotent_key, journal)
        self._compensate = compensator or SagaCompensator()

    @classmethod
    def with_journal(cls, journal: WorkerJournal) -> 'Type[SagaWorker]':
        """
        Sets default_journal to worker
        """
        s = copy.copy(SagaWorker)
        s.default_journal = journal
        return s

    def compensate(self) -> None:
        self._compensate.run()

    def job(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> WorkerJob[T, None]:
        """
        Create a WorkerJob with main function f.
        :param f: Main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        """
        return WorkerJob[T, None](
            JobSpec(self._memo.memoize(f), *args, **kwargs),
            comp_set_callback=self._place_compensation
        )

    def _place_compensation(self, spec: JobSpec[..., None]) -> None:
        spec.f = self._memo.memoize(spec.f)
        self._compensate.add_compensate(spec)
