import pickle
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generic, List, Optional, ParamSpec, Type, TypeVar, Union

from pydantic import BaseModel

P = ParamSpec('P')
T = TypeVar('T')
In = TypeVar('In', bound=BaseModel)
Out = TypeVar('Out', bound=BaseModel)


class JobStatus(str, Enum):
    RUNNING = 'RUNNING'
    """ Method running inside saga. """
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
        self._orig_args = args
        self._args: List[Any] = []
        self._orig_kwargs = kwargs

    @property
    def args(self) -> tuple[Any, ...]:
        return tuple([*self._args, *self._orig_args])

    @property
    def kwargs(self) -> P.kwargs:
        return self._orig_kwargs

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
        return self.f(*self.args, **self.kwargs)


class Ok(BaseModel):
    ok: int = 1


class Event(Generic[In, Out]):
    def __init__(self, name: str, rt_name: str, data: In, model_in: Type[In],
                 model_out: Type[Out]):
        self.name = name
        self.data = data
        self.model_in = model_in
        self.model_out = model_out
        self.ret_name = rt_name


class EventSpec(Generic[In, Out]):
    def __init__(self, name: str, model_in: Type[In], model_out: Type[Out]):
        self.name = name
        self.model_in = model_in
        self.model_out = model_out
        self.ret_name = f'r_{name}'

    def make(self, inp: In) -> Event[In, Out]:
        return Event(self.name, self.ret_name, inp, self.model_in, self.model_out)
