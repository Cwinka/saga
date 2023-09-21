import datetime
import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import Concatenate, Generic, Optional, ParamSpec, TypeVar

from pydantic import BaseModel

from saga.compensator import SagaCompensator
from saga.events import EventSender
from saga.journal import WorkerJournal, MemoryJournal
from saga.models import JobRecord, JobStatus
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')
M = TypeVar('M', bound=BaseModel)


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

    def __init__(self, worker: SagaWorker, saga: Callable[[SagaWorker, M], T], data: M,
                 forget_done: bool = False) -> None:
        """
        :param worker: Обработчик функций саги.
        :param saga: Функция саги.
        :param data: Входные данные саги, если данных нет, используется Ok.
        :param forget_done: If True when saga completes it purge all saved journal records
                           and can be run with same idempotent key.
        """
        self._worker = worker
        self._record = self._get_initial_record(worker)
        self._f_with_compensation = self._compensate_on_exception(saga)
        self._forget_done = forget_done
        self._data = data
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        """
        Run a main function. Non-blocking.
        """
        if self._result is None:
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker, self._data))

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
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            try:
                self._record.status = JobStatus.DONE
                return f(*args, **kwargs)
            except Exception as e:
                self._record.status = JobStatus.FAILED
                # self._record.failed_time = datetime.datetime.now()
                # self._record.error = str(e)
                # self._record.traceback = traceback.format_exc()
                self._worker.compensate()
                raise
            finally:
                self._worker.journal.update_record(self._record)
                if self._forget_done:
                    self._worker.journal.delete_records(self._record)
                    self._worker.forget_done()
        return wrap  # type: ignore[return-value]


class SagaRunner:

    def __init__(self, journal: Optional[WorkerJournal] = None,
                 sender: Optional[EventSender] = None,
                 forget_done: bool = False):
        self._forget_done = forget_done
        self._sender = sender
        self._journal = journal or MemoryJournal()

    def new(self, idempotent_key: str, saga: Callable[[SagaWorker, M], T], data: M) -> (
            SagaJob)[T]:
        # TODO: добавить имя саги к ключу
        worker = SagaWorker(idempotent_key, journal=self._journal,
                            compensator=SagaCompensator(), sender=self._sender)
        return SagaJob(worker, saga, data, forget_done=self._forget_done)
