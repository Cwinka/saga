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
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.models import JobStatus, SagaRecord
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')
M = TypeVar('M', bound=BaseModel)
SAGA_NAME_ATTR = '__saga_name__'


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

    def __init__(self, journal: SagaJournal, worker: SagaWorker,
                 saga: Callable[[SagaWorker, M], T], data: M,
                 forget_done: bool = False) -> None:
        """
        :param journal: Журнал саги.
        :param worker: Обработчик функций саги.
        :param saga: Функция саги.
        :param data: Входные данные саги, если данных нет, используется Ok.
        :param forget_done: If True when saga completes it purge all saved journal records
                           and can be run with same idempotent key.
        """
        self._journal = journal
        self._worker = worker
        self._f_with_compensation = self._compensate_on_exception(saga)
        self._forget_done = forget_done
        self._data = data
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        """
        Run a main function. Non-blocking.
        """
        if self._result is None:
            saga = self._get_saga()
            saga.initial_data = self._data.model_dump_json().encode('utf8')
            self._journal.update_saga(saga)
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

    def _get_saga(self) -> SagaRecord:
        record = self._journal.get_saga(self._worker.idempotent_key)
        if record is None:
            record = self._journal.create_saga(self._worker.idempotent_key)
        return record

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[Concatenate[bool, P], T]:
        """
        Wraps function f with try except block that runs compensation functions on any exception
        inside f and then reraise an exception.
        """
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            saga = self._get_saga()
            try:
                saga.status = JobStatus.DONE
                return f(*args, **kwargs)
            except Exception as e:
                saga.status = JobStatus.FAILED
                saga.failed_time = datetime.datetime.now()
                saga.error = str(e)
                saga.traceback = traceback.format_exc()
                self._worker.compensate()
                raise
            finally:
                self._journal.update_saga(saga)
                if self._forget_done:
                    self._journal.delete_sagas(saga.idempotent_key)
                    self._worker.forget_done()
        return wrap  # type: ignore[return-value]


class SagaRunner:

    def __init__(self,
                 saga_journal: Optional[SagaJournal] = None,
                 worker_journal: Optional[WorkerJournal] = None,
                 sender: Optional[EventSender] = None,
                 forget_done: bool = False):
        self._forget_done = forget_done
        self._sender = sender
        self._worker_journal = worker_journal or MemoryJournal()
        self._saga_journal = saga_journal or MemorySagaJournal()

    def new(self, idempotent_key: str, saga: Callable[[SagaWorker, M], T], data: M) -> (
            SagaJob)[T]:
        assert hasattr(saga, SAGA_NAME_ATTR), (f'Функция "{saga.__name__}" не является сагой. '
                                               f'Используйте декоратор "{idempotent_saga.__name__}"'
                                               f' чтобы отметить функцию как сагу.')
        # TODO: добавить имя саги к ключу
        worker = SagaWorker(idempotent_key, journal=self._worker_journal,
                            compensator=SagaCompensator(), sender=self._sender)
        return SagaJob(self._saga_journal, worker, saga, data, forget_done=self._forget_done)


def idempotent_saga(name: str) \
        -> Callable[[Callable[[SagaWorker, M], T]], Callable[[SagaWorker, M], T]]:
    def decorator(f: Callable[[SagaWorker, M], T]) -> Callable[[SagaWorker, M], T]:
        setattr(f, SAGA_NAME_ATTR, name)
        return f
    return decorator
