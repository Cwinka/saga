import functools
import pickle
from typing import Callable, List, ParamSpec, TypeVar

from saga.journal import WorkerJournal
from saga.models import JobRecord, JobStatus

P = ParamSpec('P')
T = TypeVar('T')


class Memoized:
    def __init__(self, memo_prefix: str, journal: WorkerJournal):
        self._journal = journal
        self._memo_prefix = memo_prefix
        self._operation_id = 0
        self._done: List[JobRecord] = []

    def _next_op_id(self) -> str:
        self._operation_id += 1
        return f'{self._memo_prefix}_{self._operation_id}'

    def forget_done(self) -> None:
        self._journal.delete_records(*self._done)

    def memoize(self, f: Callable[P, T]) -> Callable[P, T]:
        op_id = self._next_op_id()

        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            record = self._journal.get_record(op_id)
            if record is None:
                record = self._journal.create_record(op_id)
                self._done.append(record)
            # FIXME: также нужно запомнить аргументы вызова, чтобы при запуске с другими
            #  аргументами возвращался новый результат.
            if record.status == JobStatus.DONE:
                return pickle.loads(record.result)  # type: ignore[no-any-return]
            try:
                r = f(*args, **kwargs)
            except Exception:
                record.status = JobStatus.FAILED
                self._journal.update_record(record)
                raise
            record.result = pickle.dumps(r)
            record.status = JobStatus.DONE
            self._journal.update_record(record)
            return r
        return wrap
