import datetime
import functools
import pickle
import traceback
from typing import Any, Callable, Concatenate, Generic, Optional, ParamSpec, TypeVar

from saga.compensator import SagaCompensator
from saga.journal import MemoryJournal, WorkerJournal
from saga.models import JobSpec, JobStatus

P = ParamSpec('P')
T = TypeVar('T')
CompensationCallback = Callable[[JobSpec[..., Any]], None]


class WorkerJob(Generic[T]):
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
                 comp_set_callback: CompensationCallback = lambda *_: None) -> None:
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[..., None]] = None
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

    def with_compensation(self, f: Callable[Concatenate[T, P], Any], *args: P.args,
                          **kwargs: P.kwargs) -> 'WorkerJob[T]':
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


class Memoized:
    def __init__(self, memo_prefix: str, journal: WorkerJournal):
        self._journal = journal
        self._memo_prefix = memo_prefix
        self._operation_id = 0

    def _next_op_id(self) -> str:
        self._operation_id += 1
        return f'{self._memo_prefix}_{self._operation_id}'

    def memoize(self, f: Callable[P, T]) -> Callable[P, T]:
        op_id = self._next_op_id()

        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._journal.get_record(op_id)
            if record is None:
                record = self._journal.create_record(op_id)
            if record.status == JobStatus.DONE:
                return pickle.loads(record.payload)  # type: ignore[no-any-return]
            try:
                r = f(*args, **kwargs)
            except Exception as e:
                record.status = JobStatus.FAILED
                record.traceback = traceback.format_exc()
                record.failed_time = record.failed_time or datetime.datetime.now()
                record.error = str(e)
                self._journal.update_record(record)
                raise
            record.payload = pickle.dumps(r)
            record.status = JobStatus.DONE
            self._journal.update_record(record)
            return r
        return wrap


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

    @property
    def compensator(self) -> SagaCompensator:
        """
        A compensator that is used with worker object.
        """
        return self._compensate

    def job(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> WorkerJob[T]:
        """
        Create a WorkerJob with main function f.
        :param f: Main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        """
        job = WorkerJob(JobSpec(self._memo.memoize(f), *args, **kwargs),
                        comp_set_callback=self._place_compensation)
        return job

    def _place_compensation(self, spec: JobSpec[..., Any]) -> None:
        spec.f = self._memo.memoize(spec.f)
        self._compensate.add_compensate(spec)
