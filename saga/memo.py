import datetime
import functools
import pickle
import traceback
from typing import Callable, ParamSpec, TypeVar

from saga.journal import WorkerJournal
from saga.models import JobStatus

P = ParamSpec('P')
T = TypeVar('T')


class Memoized:
    def __init__(self, memo_prefix: str, journal: WorkerJournal):
        self._journal = journal
        self._memo_prefix = memo_prefix
        self._operation_id = 0

    def _next_op_id(self) -> str:
        self._operation_id += 1
        return f'{self._memo_prefix}_{self._operation_id}'

    def memoize(self, f: Callable[P, T]) -> Callable[P, T]:
        op_id = self._next_op_id()

        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._journal.get_record(op_id)
            if record is None:
                record = self._journal.create_record(op_id)
            # FIXME: также нужно запомнить аргументы вызова, чтобы при запуске с другими
            #  аргументами возвращался новый результат.
            if record.status == JobStatus.DONE:
                return pickle.loads(record.payload)  # type: ignore[no-any-return]
            try:
                r = f(*args, **kwargs)
            except Exception as e:
                record.status = JobStatus.FAILED
                record.traceback = traceback.format_exc()
                record.failed_time = record.failed_time or datetime.datetime.now()
                record.error = str(e)
                self._journal.update_record(record)
                raise
            record.payload = pickle.dumps(r)
            record.status = JobStatus.DONE
            self._journal.update_record(record)
            return r
        return wrap
