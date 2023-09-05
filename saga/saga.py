import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import ParamSpec, Optional, Concatenate, TypeVar, Generic

P = ParamSpec('P')
T = TypeVar('T')


class SagaResult(Generic[T]):
    def __init__(self, r: Optional[T], error: str = '', trace: str = ''):
        self._r = r
        self._err = error
        self._trace = trace

    def error(self) -> str:
        return self._err

    def traceback(self) -> str:
        return self._trace

    def is_err(self) -> bool:
        return bool(self._err or self._trace)

    def value(self) -> T:
        assert self._r is not None, 'Невозможно получить результат операции, так как возникла ' \
                                    f'ошибка при ее выполнении: {self._err}'
        return self._r


class SagaWorker:
    def __init__(self, idempotent_key: str):
        self.idempotent_key = idempotent_key

    def run(self, f: Callable[P, SagaResult[T]], *args: P.args, **kwargs: P.kwargs) -> SagaResult[T]:
        # Здесь должно быть сохранение результата f, для того, чтобы получить его в случае
        # непредвиденного завершения работы
        return f(*args, **kwargs)


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


def saga_participant(f: Callable[P, T]) -> Callable[P, SagaResult[T]]:
    @functools.wraps(f)
    def wrap(*args: P.args, **kwargs: P.kwargs) -> SagaResult[T]:
        try:
            result = f(*args, **kwargs)
            return SagaResult(result)
        except Exception as e:
            return SagaResult(None, str(e), traceback.format_exc())
    return wrap
