import functools
import multiprocessing.pool
import os
import pickle
import traceback
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Concatenate, Generic, List, Optional, ParamSpec, TypeVar

from saga.models import JobRecord, JobStatus

P = ParamSpec('P')
T = TypeVar('T')


class JobSPec(Generic[P, T]):
    def __init__(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def call(self) -> T:
        return self.f(*self.args, **self.kwargs)


class SagaCompensate:
    """
    A SagaCompensate is responsible to hold and run compensation functions which has been added
    to it.
    SagaCompensate is used if an exception happens in saga function. When an exception is raised
    first of all compensation functions are executed (in reverse order which they were added) and
    then exception is reraised.
    """
    def __init__(self) -> None:
        self._compensations: List[JobSPec[..., None]] = []

    def add_compensate(self, spec: JobSPec[P, None]) -> None:
        """
        Add compensation function.
        """
        self._compensations.append(spec)

    def clear(self) -> None:
        """
        Clear all added compensation functions.
        """
        self._compensations.clear()

    def run(self) -> None:
        """
        Runs all added compensation functions.
        """
        self._compensations.reverse()
        while self._compensations:
            self._compensations.pop().call()


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


class WorkerJob(Generic[T]):
    """
    A WorkerJob is responsible for running functions inside any saga function.
    The main goal of this object is to make control points where execution can continue if
    unexpected shutdown happened.

    @idempotent_saga
    def any_saga(worker: SagaWorker, *args, **kwargs):
        result = worker.job(any_function, *args, **kwargs).run()

    After creating object with function f there's a feature called "compensation". This feature
    adds associated method with function f which will be called if any exception happens after it:

    @idempotent_saga
    def any_saga(worker: SagaWorker, *args, **kwargs):
        result = worker.job(any_function, *args, **kwargs)\
            .with_compensation(rollback_any_function).run()

    NOTE: when WorkerJob is used outside any saga no compensations will be executed on exception.
    Use compensate object directly to call compensation functions.

    compensate = SagaCompensate()
    try:
        WorkerJob(compensate, any_function).with_compensation(rollback_any_function).run()
        raise StrangeException
    except StrangeException:
        compensate.run()
        raise
    """

    def __init__(self, idempotent_operation_id: str, compensate: SagaCompensate,
                 journal: WorkerJournal, spec: JobSPec[P, T]):
        self._idempotent_operation_id = idempotent_operation_id
        self._compensate = compensate
        self._journal = journal
        spec.f = self._wrap_to_savable(spec.f)
        self._spec = spec
        self._compensation_spec: Optional[JobSPec[..., None]] = None

    def run(self) -> T:
        """
        Runs a main function with associated arguments and keyword arguments.
        :return: Result of a funtion.
        """
        r = self._spec.call()
        if self._compensation_spec is not None:
            self._compensation_spec.args = (r, *self._compensation_spec.args)
            self._compensate.add_compensate(self._compensation_spec)
        return r

    def with_compensation(self, f: Callable[Concatenate[T, P], None], *args: P.args,
                          **kwargs: P.kwargs) -> 'WorkerJob[T]':
        """
        Adds a compensation function which will be called if any exception happens after running a
        main function.
        :param f: Compensation function. The first argument is always a result of a main function.
        :param args: Any arguments to pass in f function.
        :param kwargs: Any keyword arguments to pass in f function.
        :return: The same WorkerJob object.
        """
        self._compensation_spec = JobSPec(
            self._wrap_compensation_to_savable(f),
            *args, **kwargs
        )
        return self

    def _wrap_to_savable(self, f: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._journal.get_record(self._idempotent_operation_id)
            if record is None:
                record = self._journal.create_record(self._idempotent_operation_id)
            if record.status in {JobStatus.RUNNING, JobStatus.FAILED}:
                try:
                    r = f(*args, **kwargs)
                except Exception as e:
                    record.status = JobStatus.FAILED
                    record.traceback = traceback.format_exc()
                    record.error = str(e)
                    self._journal.update_record(record)
                    raise
                record.payload = pickle.dumps(r)
                record.status = JobStatus.DONE
                self._journal.update_record(record)
            else:
                r = pickle.loads(record.payload)
            return r
        return wrap

    def _wrap_compensation_to_savable(self, f: Callable[P, None]) -> Callable[P, None]:
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> None:
            op_id = f'cpm_{self._idempotent_operation_id}'
            record = self._journal.get_record(op_id)
            if record is None:
                record = self._journal.create_record(op_id)
            if record.status in {JobStatus.RUNNING, JobStatus.FAILED}:
                try:
                    f(*args, **kwargs)
                except Exception as e:
                    # компенсация не может выполниться
                    record.status = JobStatus.FAILED
                    record.traceback = traceback.format_exc()
                    record.error = str(e)
                    self._journal.update_record(record)
                    raise
                record.status = JobStatus.COMPENSATED
                self._journal.update_record(record)
        return wrap


class SagaWorker:
    """
    A SagaWorker is responsible for creating jobs (WorkerJob) inside saga function.
    The main reason there is WorkerJob and SagaWorker is that a main function inside WorkerJob can
    be associated with a compensation function and the first argument of compensation function is a
    result of a main function. So WorkerJob is typed object to properly link a compensation to
    a main function.

    NOTE: when SagaWorker is used outside any saga no compensations will be executed on exception.
    Use compensate object directly to call compensation functions.

    compensate = SagaCompensate()
    worker = SagaWorker('1', compensate)
    try:
        worker.job(any_function).with_compensation(rollback_any_function).run()
        raise StrangeException
    except StrangeException:
        compensate.run()
        raise
    """

    def __init__(self, idempotent_key: str, journal: WorkerJournal,
                 compensate: Optional[SagaCompensate] = None):
        self._idempotent_key = idempotent_key
        self._journal = journal
        self._compensate = compensate or SagaCompensate()
        self._operation_id = 0

    @property
    def compensator(self) -> SagaCompensate:
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
        job = WorkerJob(f'{self._idempotent_key}_{self._operation_id}', self._compensate,
                        self._journal, JobSPec(f, *args, **kwargs))
        self._operation_id += 1
        return job


class SagaJob(Generic[T]):
    """
    A SagaJob is responsible for creating saga objects.
    Saga is a function marked with "idempotent_saga" decorator or created via initializing SagaJob
    on a function.

    @idempotent_saga
    def saga1(worker: SagaWorker, my_arg: int):
        pass

    def saga2(worker: SagaWorker, my_arg: int):
        pass

    job1 = saga1(SagaWorker('1'), 42)
    job2 = SagaJob(saga2, SagaWorker('1'), 42)  # same job as above

    SagaJob is used to run sagas and sometimes wait for its execution done.

    job1.run()
    job1.wait()  # "wait" method implies "run" method, so call "run" is redundant.
    """

    _pool = multiprocessing.pool.ThreadPool(os.cpu_count())

    def __init__(self, f: Callable[Concatenate[SagaWorker, P], T], worker: SagaWorker,
                 *args: P.args, **kwargs: P.kwargs):
        self._worker = worker
        self._f_with_compensation = self._compensate_on_exception(f)
        self._args = args
        self._kwargs = kwargs
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        """
        Run a main function. Non-blocking.
        """
        if self._result is None:
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker, *self._args),
                                                  kwds=self._kwargs)

    def wait(self, timeout: Optional[float] = None) -> T:
        """
        Wait for a main function is executed. Automatically implies run.
        :param timeout: Time in seconds to wait for execution is done.
        :return: Result of a main function.
        """
        if self._result is None:
            self.run()
        assert self._result is not None
        return self._result.get(timeout)

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[P, T]:
        """
        Wraps function f with try except block that runs compensation functions on any exception
        inside f and then reraise an exception.
        """
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return f(*args, **kwargs)
            except Exception:
                self._worker.compensator.run()
                self._worker.compensator.clear()
                raise
        return wrap


def idempotent_saga(f: Callable[Concatenate[SagaWorker, P], T]) -> \
        Callable[Concatenate[SagaWorker, P], SagaJob[T]]:
    """
    Decorator to mark a function as saga function.
    """
    @functools.wraps(f)
    def wrap(worker: SagaWorker, /, *args: P.args, **kwargs: P.kwargs) -> SagaJob[T]:
        return SagaJob(f, worker, *args, **kwargs)
    return wrap
