import base64
import functools
import pickle
import time
from typing import Any, Callable, List, ParamSpec, TypeVar

from saga.journal import WorkerJournal
from saga.models import JobStatus

P = ParamSpec('P')
T = TypeVar('T')


def object_to_bytes(obj: Any) -> bytes:
    """
    Конвертировать объект obj в ascii байты.
    """
    return base64.b64encode(pickle.dumps(obj))


def object_from_bytes(b: bytes) -> Any:
    """
    Восстановить объект из ascii байт b.
    """
    return pickle.loads(base64.b64decode(b))


class NotEnoughRetries(Exception):
    pass


class Memoized:
    """
    Memoized используется для сохранения результата работы функции.
    """
    def __init__(self, memo_prefix: str, journal: WorkerJournal,
                 obj_to_b: Callable[[Any], bytes] = object_to_bytes,
                 obj_from_b: Callable[[bytes], Any] = object_from_bytes):
        self._journal = journal
        self._memo_prefix = memo_prefix
        self._operation_id = 0
        self._done: List[str] = []
        self._obj_to_b = obj_to_b
        self._obj_from_b = obj_from_b

    def _next_op_id(self) -> str:
        self._operation_id += 1
        return f'{self._memo_prefix}_{self._operation_id}'

    def forget_done(self) -> None:
        """
        Удалить все сохраненные результаты работы.
        """
        self._journal.delete_records(*self._done)

    def memoize(self, f: Callable[P, T], retries: int = 1, retry_interval: float = 2.0) \
            -> Callable[P, T]:
        """
        Декоратор функции f. После декорирования, возвращаемое значение функции будет сохранено.
        При повторном вызове f будет возвращен сохраненный результат вместо вызова функции.
        :param f: Функция, результат которой будет сохранен.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение или
                        NotEnoughRetries.
        :param retry_interval: Интервал времени (в секундах), через который будет вызван повтор
                               функции в случае исключения.
        """
        op_id = self._next_op_id()

        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._journal.get_record(op_id)
            if record is None:
                record = self._journal.create_record(op_id)
                self._done.append(record.idempotent_operation_id)
            # FIXME: также нужно запомнить аргументы вызова, чтобы при запуске с другими
            #  аргументами возвращался новый результат.
            if record.status == JobStatus.DONE:
                return object_from_bytes(record.result)  # type: ignore[no-any-return]
            if record.runs >= retries:
                exc = object_from_bytes(record.result)
                if isinstance(exc, Exception):
                    raise exc
                raise NotEnoughRetries()
            self._journal.update_record(record.idempotent_operation_id,
                                        ['runs'],
                                        [record.runs + 1])
            try:
                r = f(*args, **kwargs)
                self._journal.update_record(record.idempotent_operation_id,
                                            ['status', 'result'],
                                            [JobStatus.DONE, object_to_bytes(r)])
                return r
            except Exception as e:
                self._journal.update_record(record.idempotent_operation_id,
                                            ['status', 'result'],
                                            [JobStatus.FAILED, object_to_bytes(e)])
                if record.runs < retries:
                    time.sleep(retry_interval)
                    return wrap(*args, **kwargs)
                raise e
        return wrap
