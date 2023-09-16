import pickle
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Callable, List, Any, Generic, Optional, ParamSpec, TypeVar

P = ParamSpec('P')
T = TypeVar('T')


class JobStatus(str, Enum):
    RUNNING = 'RUNNING'
    """ Method running inside saga. """
    COMPENSATED = 'COMPENSATED'
    """ Compensation method has been executed inside saga. """
    DONE = 'DONE'
    """ Method has been executed inside saga. """
    FAILED = 'FAILED'
    """ Method has encourage an exception during execution. """


@dataclass
class JobRecord:
    idempotent_operation_id: str
    """ A unique key op an operation inside saga. """
    status: JobStatus = JobStatus.RUNNING
    """ Current status of an operation. """
    payload: bytes = pickle.dumps(None)
    """ Return content of an operation. """
    traceback: Optional[str] = None
    """ Traceback of an operation """
    error: Optional[str] = None
    """ Error message of an operation """
    failed_time: Optional[datetime] = None
    """ Error time when exception happened. """


class JobSpec(Generic[P, T]):
    """
    Function specification. Used to create a specification of any function to run as needed.
    """
    def __init__(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self.f = f
        self.args = args
        self._args: List[Any] = []
        self.kwargs = kwargs

    def with_arg(self, arg: Any) -> 'JobSpec[P, T]':
        """
        Adds argument arg as the first positional argument of the main function.
        Use this method with care because there are no checks the main function can
        accept arg argument.
        :param arg: Any value to pass to the main function.
        """
        self._args.insert(0, arg)
        return self

    def call(self) -> T:
        """
        Execute the main function. Can be called multiple times.
        :return: Result of the main function.
        """
        return self.f(*self._args, *self.args, **self.kwargs)
