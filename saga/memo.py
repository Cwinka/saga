import base64
import functools
import pickle
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


class Memoized:
    """
    Memoized object is used to memorize return of any function and store it in journal.
    Object can be used with single SagaWorker and must reinitialize for another worker.
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
        Clears all done operations
        """
        self._journal.delete_records(*self._done)

    def memoize(self, f: Callable[P, T]) -> Callable[P, T]:
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
            fields: List[str] = []
            values: List[Any] = []
            try:
                r = f(*args, **kwargs)
                fields.extend(['status', 'result'])
                values.extend([JobStatus.DONE, object_to_bytes(r)])
                return r
            except Exception:
                fields.append('status')
                values.append(JobStatus.FAILED)
                raise
            finally:
                self._journal.update_record(record.idempotent_operation_id, fields, values)
        return wrap
