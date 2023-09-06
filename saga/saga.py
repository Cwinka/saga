import functools
import multiprocessing.pool
import os
from collections.abc import Callable
from types import TracebackType
from typing import Any, Concatenate, Dict, Generic, List, Optional, ParamSpec, Tuple, Type, TypeVar

P = ParamSpec('P')
T = TypeVar('T')


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
    """

    def __init__(self, compensate: 'SagaCompensate',
                 f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self._compensate = compensate
        self._f = f
        self._args = args
        self._kwargs = kwargs
        self._compensation: Optional[Callable[Concatenate[T, P], None]] = None
        self._compensation_args: Tuple[Any, ...] = ()
        self._compensation_kwargs: Dict[str, Any] = {}

    def run(self) -> T:
        """
        Runs a main function with associated arguments and keyword arguments.
        :return: Result of a funtion.
        """
        r = self._f(*self._args, **self._kwargs)
        # Здесь должно быть сохранение результата f, для того, чтобы получить его в случае
        # непредвиденного завершения работы
        if self._compensation is not None:
            self._compensate.add_compensate(self._compensation,
                                            *(r, *self._compensation_args),
                                            **self._compensation_kwargs)
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
        self._compensation = f
        self._compensation_args = args
        self._compensation_kwargs = kwargs
        return self


class SagaCompensate:
    def __init__(self) -> None:
        self._compensations: List[Tuple[Callable[..., None], Tuple[Any, ...], Dict[str, Any]]] = []

    def add_compensate(self, f: Callable[P, None], *args: P.args, **kwargs: P.kwargs) -> None:
        self._compensations.append((f, args, kwargs))

    def clear(self) -> None:
        self._compensations.clear()

    def run(self) -> None:
        """
        Запуск всех добавленных компенсаций.
        """
        self._compensations.reverse()
        while self._compensations:
            f, args, kwargs = self._compensations.pop()
            f(*args, **kwargs)


class SagaWorker:
    """
    A SagaWorker is responsible for creating jobs (WorkerJob) inside saga function.
    The main reason there is WorkerJob and SagaWorker is that a main function inside WorkerJob can
    be associated with a compensation function and the first argument of compensation function is a
    result of main function. So WorkerJob is typed object to properly pass link a compensation to
    a main function.
    """

    def __init__(self, idempotent_key: str, compensate: Optional[SagaCompensate] = None):
        self.idempotent_key = idempotent_key
        self._compensate = compensate or SagaCompensate()

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
        return WorkerJob(self._compensate, f, *args, **kwargs)


class SagaJob(Generic[T]):

    _pool = multiprocessing.pool.ThreadPool(os.cpu_count())

    def __init__(self, f: Callable[Concatenate[SagaWorker, P], T], worker: SagaWorker,
                 *args: P.args, **kwargs: P.kwargs):
        self._worker = worker
        self._f_with_compensation = self._compensate_on_exception(f)
        self._args = args
        self._kwargs = kwargs
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        if self._result is None:
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker, *self._args),
                                                  kwds=self._kwargs)

    def wait(self, timeout: Optional[float] = None) -> T:
        if self._result is None:
            self.run()
        assert self._result is not None
        return self._result.get(timeout)

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[P, T]:
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
    @functools.wraps(f)
    def wrap(worker: SagaWorker, /, *args: P.args, **kwargs: P.kwargs) -> SagaJob[T]:
        return SagaJob(f, worker, *args, **kwargs)
    return wrap
