import functools
import multiprocessing.pool
import os
from collections.abc import Callable
from typing import ParamSpec, Optional, Concatenate, TypeVar, Generic, Tuple, List

P = ParamSpec('P')
T = TypeVar('T')


class WorkerJob(Generic[T]):

    def __init__(self, compensate: 'SagaCompensate',
                 f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self._compensate = compensate
        self._f = f
        self._args = args
        self._kwargs = kwargs
        self._compensation: Optional[Callable[[T], None]] = None

    def run(self) -> T:
        r = self._f(*self._args, **self._kwargs)
        # Здесь должно быть сохранение результата f, для того, чтобы получить его в случае
        # непредвиденного завершения работы
        if self._compensation is not None:
            self._compensate.add_compensate(self._compensation, r)
        return r

    def with_compensation(self, f: Callable[[T], None]) -> 'WorkerJob[T]':
        self._compensation = f
        return self


class SagaWorker:

    def __init__(self, idempotent_key: str):
        self.idempotent_key = idempotent_key
        self._compensate = SagaCompensate()

    def job(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs,) -> WorkerJob[T]:
        return WorkerJob(self._compensate, f, *args, **kwargs)

    def compensate(self) -> 'SagaWorker':
        self._compensate = SagaCompensate()
        return self

    def __enter__(self) -> 'SagaWorker':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type is not None:
            self._compensate.run()


class SagaCompensate:
    def __init__(self) -> None:
        self._compensations: List[Tuple[Callable[P, None], P.args, P.kwargs]] = []

    def add_compensate(self, f: Callable[P, None], *args: P.args, **kwargs: P.kwargs) -> None:
        self._compensations.append((f, args, kwargs))

    def run(self) -> None:
        """
        Запуск всех добавленных компенсаций.
        """
        self._compensations.reverse()
        while self._compensations:
            f, args, kwargs = self._compensations.pop()
            f(*args, **kwargs)


class SagaJob(Generic[T]):

    _pool = multiprocessing.pool.ThreadPool(os.cpu_count())

    def __init__(self, f: Callable[Concatenate[SagaWorker, P], T], worker: SagaWorker,
                 *args: P.args, **kwargs: P.kwargs):
        self._worker = worker
        self._f = f
        self._args = args
        self._kwargs = kwargs
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        if self._result is None:
            self._result = self._pool.apply_async(self._f, args=(self._worker, *self._args),
                                                  kwds=self._kwargs)

    def wait(self, timeout: Optional[float] = None) -> T:
        if self._result is None:
            self.run()
        assert self._result is not None
        return self._result.get(timeout)


def idempotent_saga(f: Callable[Concatenate[SagaWorker, P], T]) -> \
        Callable[Concatenate[SagaWorker, P], SagaJob[T]]:
    @functools.wraps(f)
    def wrap(worker: SagaWorker, *args: P.args, **kwargs: P.kwargs) -> SagaJob[T]:
        return SagaJob(f, worker, *args, **kwargs)
    return wrap
