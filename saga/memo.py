import base64
import functools
import pickle
import time
import traceback
from typing import Any, Callable, Dict, ParamSpec, TypeVar
from uuid import UUID

from saga.journal import WorkerJournal
from saga.models import JobRecord, JobStatus, JobSpec

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
    def __init__(self, uuid: UUID, journal: WorkerJournal, metadata: Dict[str, Any],
                 obj_to_bytes: Callable[[Any], bytes] = object_to_bytes,
                 obj_from_bytes: Callable[[bytes], Any] = object_from_bytes):
        self._journal = journal
        self._metadata = metadata
        self._uuid = uuid
        self._operation_id = 0
        self._obj_to_b = obj_to_bytes
        self._obj_from_b = obj_from_bytes

    def forget_done(self) -> None:
        """
        Удалить все сохраненные результаты работы.
        """
        self._journal.delete_records(self._uuid)

    def memoize(self, f: Callable[P, T], retries: int = 1, retry_interval: float = 2.0) \
            -> Callable[P, T]:
        """
        Декоратор функции ``f``. После декорирования, возвращаемое значение функции будет сохранено.
        При повторном вызове ``f`` будет возвращен сохраненный результат вместо вызова функции.
        :param f: Функция, результат которой будет сохранен.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение или
                        ``NotEnoughRetries``. Если ``retries=-1``, тогда количество
                        повторов не ограничено.
        :param retry_interval: Интервал времени (в секундах), через который будет вызван повтор
                               функции в случае исключения.
        """
        op_id = self._next_op_id()
        if retries < 0:
            retries = float('+inf')  # type: ignore[assignment]

        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._get_record(op_id)
            if record.status == JobStatus.DONE:
                return self._obj_from_b(record.result)  # type: ignore[no-any-return]
            if record.runs >= retries:
                trace = self._obj_from_b(record.result)
                raise NotEnoughRetries(trace)
            spec = JobSpec(f, *args, **kwargs)
            return self._run_in_exception_block(spec, retries, retry_interval, record)
        return wrap

    def _run_in_exception_block(self, spec: JobSpec[T, Any], retries: int, retry_interval: float,
                                record: JobRecord) -> T:
        exc: Exception = NotEnoughRetries()
        trace = ''
        while record.runs < retries:
            record.runs += 1
            self._journal.update_record(record.uuid, record.operation_id,
                                        ['runs'], [record.runs])
            try:
                r = spec.call()
                self._journal.update_record(record.uuid, record.operation_id,
                                            ['status', 'result'],
                                            [JobStatus.DONE, self._obj_to_b(r)])
                return r
            except Exception as e:
                exc = e
                trace = traceback.format_exc()
                if record.runs < retries:
                    time.sleep(retry_interval)
        self._journal.update_record(record.uuid, record.operation_id,
                                    ['status', 'result'],
                                    [JobStatus.FAILED, self._obj_to_b(trace)])
        raise exc

    def _next_op_id(self) -> int:
        self._operation_id += 1
        return self._operation_id

    def _get_record(self, op_id: int) -> JobRecord:
        record = self._journal.get_record(self._uuid, op_id)
        if record is None:
            record = self._journal.create_record(self._uuid, op_id, self._metadata)
        return record
