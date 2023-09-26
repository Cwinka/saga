import datetime
import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import Any, Concatenate, Dict, Generic, Optional, ParamSpec, Tuple, Type, TypeVar
from uuid import UUID

from pydantic import BaseModel

from saga.compensator import SagaCompensator
from saga.events import CommunicationFactory
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.models import JobStatus, SagaRecord
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')
M = TypeVar('M', bound=BaseModel)
SAGA_NAME_ATTR = '__saga_name__'
SAGA_KEY_SEPARATOR = '&'


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
            saga.set_initial_data(self._data)
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
    """
    SagaRunner is a factory class that manipulates SagaJob objects.

    To register a faction as a saga two methods can be used:

        # using decorator
        @idempotent_saga('saga_name')
        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...

        # or register function manually:

        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...

        SagaRunner.register_saga('saga_name', my_saga)

    First argument of any saga is a SagaWorker object that SagaRunner provides automatically to
    any saga. Second argument is an input data of a saga, and it must be a subtype of the
    `pydantic.BaseModel` class. If a saga function does not need input data a special `Ok` class
    can be used to pass it in a saga.

    To start any saga method `new` is used:

        runner = SagaRunner()

        saga = runner.new('idempotent_key', my_saga, Ok())
        saga.run()  # to run in a non-blocking mode.
        saga.wait()  # to run in a blocking mode.

    The first argument of `new` method is idempotent key string that is unique across runs. If
    the same saga runs with a used idempotent key it provides the same result as the previous
    run with this idempotent key.

    The `SagaJob.wait` method can raise an exception if it happens inside a saga, so it must be
    used with try except block to process code further.

    Any saga can end up in an incomplete state (neither in `SagaStatus.DONE` nor
    `SagaStatus.FAILED` but in `SagaStatus.RUNNING` state) because of unexpected exit or power
    failure. Incomplete sagas can be re-executed to finish it properly:

        journal = SagaJournalImplementation()
        runner = SagaRunner(saga_journal=journal)

        runner.run_incomplete()  # returns number of started sagas.
    """
    _sagas: Dict[str, Callable[[SagaWorker, M], Any]] = {}

    def __init__(self,
                 saga_journal: Optional[SagaJournal] = None,
                 worker_journal: Optional[WorkerJournal] = None,
                 cfk: Optional[CommunicationFactory] = None,
                 forget_done: bool = False):
        self._forget_done = forget_done
        self._cfk = cfk
        self._worker_journal = worker_journal or MemoryJournal()
        self._saga_journal = saga_journal or MemorySagaJournal()

    def new(self, idempotent_key: UUID, saga: Callable[[SagaWorker, M], T], data: M) -> (
            SagaJob)[T]:
        """
        Creates new SagaJob object to run saga.

        :param idempotent_key: Unique key for saga execution.
        :param saga: Registered saga function.
        :param data: Input data of a saga.
        """
        assert hasattr(saga, SAGA_NAME_ATTR), (f'Функция "{saga.__name__}" не является сагой. '
                                               f'Используйте декоратор "{idempotent_saga.__name__}"'
                                               f' чтобы отметить функцию как сагу.')
        key = self.join_key(idempotent_key, getattr(saga, SAGA_NAME_ATTR))
        worker = SagaWorker(key, journal=self._worker_journal,
                            compensator=SagaCompensator(),
                            sender=self._cfk.sender() if self._cfk is not None else None)
        return SagaJob(self._saga_journal, worker, saga, data, forget_done=self._forget_done)

    def run_incomplete(self) -> int:
        """
        Rerun all incomplete sagas. Returns the number of running sagas. Does not block execution.
        """
        i = 0
        for i, saga in enumerate(self._saga_journal.get_incomplete_saga(), 1):
            idempotent_key, saga_name = self._split_key(saga.idempotent_key)
            saga_f = self.get_saga(saga_name)
            if saga_f is None:
                # FIXME: warning, сагу переименовали, но было найдено старое имя в базе
                pass
            else:
                model: Type[BaseModel] = list(saga_f.__annotations__.values())[1]
                assert not isinstance(model, str), ('Используйте явную аннотацию типа данных саги '
                                                    f'"{saga_f.__name__}", без '
                                                    'оборачивания его в строку.')
                self.new(
                    UUID(idempotent_key), saga_f, model.model_validate_json(saga.get_initial_data())
                ).run()
        return i

    @classmethod
    def register_saga(cls, name: str, saga: Callable[[SagaWorker, M], Any]) -> None:
        """
        Registers a saga function with a given name.
        """
        setattr(saga, SAGA_NAME_ATTR, name)
        cls._sagas[name] = saga

    @classmethod
    def get_saga(cls, name: str) -> Optional[Callable[[SagaWorker, M], Any]]:
        """
        Returns registered saga function with a given name.
        """
        return cls._sagas.get(name)

    @staticmethod
    def join_key(idempotent_key: UUID, saga_name: str) -> str:
        """
        Returns a string that can be used to retrieve SagaRecord object from SagaJournal.
        """
        return f'{idempotent_key}{SAGA_KEY_SEPARATOR}{saga_name}'

    @staticmethod
    def _split_key(joined_key: str) -> Tuple[str, str]:
        key, *name = joined_key.split(SAGA_KEY_SEPARATOR)
        return key, ''.join(name)


def idempotent_saga(name: str) \
        -> Callable[[Callable[[SagaWorker, M], T]], Callable[[SagaWorker, M], T]]:
    """
    Register a function like a saga function in SagaRunner.
    """
    def decorator(f: Callable[[SagaWorker, M], T]) -> Callable[[SagaWorker, M], T]:
        SagaRunner.register_saga(name, f)
        return f
    return decorator
