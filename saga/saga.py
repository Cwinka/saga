import datetime
import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import Concatenate, Generic, Optional, ParamSpec, TypeVar

from saga.models import JobRecord, JobStatus
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')


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

    def __init__(self, worker: SagaWorker, f: Callable[Concatenate[SagaWorker, P], T],
                 *args: P.args, **kwargs: P.kwargs):
        self._worker = worker
        self._record = self._get_initial_record(worker)
        self._f_with_compensation = self._compensate_on_exception(f)
        self._args = args
        self._kwargs = kwargs
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self, forget_on_complete: bool = False) -> None:
        """
        Run a main function. Non-blocking.
        """
        if self._result is None:
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker,
                                                        forget_on_complete, *self._args),
                                                  kwds=self._kwargs)

    def wait(self, timeout: Optional[float] = None, forget_on_complete: bool = False) -> T:
        """
        Wait for a main function is executed. Automatically implies run.
        :param timeout: Time in seconds to wait for execution is done.
        :param forget_on_complete: If True when saga completes it purge all saved journal records
                                   and can be run with same idempotent key.
        :return: Result of a main function.
        """
        if self._result is None:
            self.run(forget_on_complete)
        assert self._result is not None
        return self._result.get(timeout)

    @staticmethod
    def _get_initial_record(worker: SagaWorker) -> JobRecord:
        record = worker.journal.get_record(worker.idempotent_key)
        if record is None:
            record = worker.journal.create_record(worker.idempotent_key)
        return record

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[Concatenate[bool, P], T]:
        """
        Wraps function f with try except block that runs compensation functions on any exception
        inside f and then reraise an exception.
        """
        @functools.wraps(f)
        def wrap(forget: bool, *args: P.args, **kwargs: P.kwargs) -> T:
            try:
                return f(*args, **kwargs)
            except Exception as e:
                self._record.failed_time = datetime.datetime.now()
                self._record.error = str(e)
                self._record.traceback = traceback.format_exc()
                self._worker.compensate()
                raise
            finally:
                self._record.status = JobStatus.DONE
                self._worker.journal.update_record(self._record)
                if forget:
                    self._worker.journal.delete_records(self._record)
                    self._worker.forget_done()
        return wrap  # type: ignore[return-value]


def idempotent_saga(f: Callable[Concatenate[SagaWorker, P], T]) -> \
        Callable[Concatenate[SagaWorker, P], SagaJob[T]]:
    """
    Decorator to mark a function as saga function.
    """
    @functools.wraps(f)
    def wrap(worker: SagaWorker, *args: P.args, **kwargs: P.kwargs) -> SagaJob[T]:
        return SagaJob(worker, f, *args, **kwargs)
    return wrap  # type: ignore[return-value]
