import base64
import pickle
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generic, List, Optional, ParamSpec, Type, TypeVar

from pydantic import BaseModel

P = ParamSpec('P')
T = TypeVar('T')
In = TypeVar('In', bound=BaseModel)
Out = TypeVar('Out', bound=BaseModel)


class Ok(BaseModel):
    """
    Class is used to tell that is nothing to return or accept.
    """
    ok: int = 1


class JobStatus(str, Enum):
    RUNNING = 'RUNNING'
    """ Method/saga is running. """
    DONE = 'DONE'
    """ Method/saga is executed with no errors. """
    FAILED = 'FAILED'
    """ Method/saga is executed with errors. """


class SagaRecord(BaseModel):
    idempotent_key: str
    """ A unique key of a saga. """
    status: JobStatus = JobStatus.RUNNING
    """ Current status of an operation. """
    initial_data: bytes = base64.b64encode(Ok().model_dump_json().encode('utf8'))
    """ Return content of an operation. """
    traceback: Optional[str] = None
    """ Traceback of an operation """
    error: Optional[str] = None
    """ Error message of an operation """
    failed_time: Optional[datetime] = None
    """ Error time when exception happened. """


class JobRecord(BaseModel):
    idempotent_operation_id: str
    """ A unique key of an operation inside saga. """
    status: JobStatus = JobStatus.RUNNING
    """ Current status of an operation. """
    result: bytes = base64.b64encode(pickle.dumps(None))
    """ Return content of an operation. """


class JobSpec(Generic[P, T]):
    """
    Function specification. Used to create a specification of any function to run as needed.
    """
    def __init__(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self.f = f
        self._orig_args = args
        self._args: List[Any] = []
        self._orig_kwargs = kwargs

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
        return self.f(*self._args, *self._orig_args, **self._orig_kwargs)


class Event(Generic[In, Out]):
    """
    Event is an object that route event and holds its data and models.
    """
    def __init__(self, name: str, rt_name: str, data: In, model_in: Type[In],
                 model_out: Type[Out]):
        """
        :param name: Event name.
        :param rt_name: Returning event name.
        :param data: Instance of input data.
        :param model_in: Input model of event.
        :param model_out: Output model of event.
        """
        self.name = name
        self.data = data
        self.model_in = model_in
        self.model_out = model_out
        self.ret_name = rt_name


class EventSpec(Generic[In, Out]):
    """
    Event specification/factory object. It holds information about an event like its input and
    output models.
    Example of EventSpec:

        spec = EventSpec('create_it', InputModel, OutputModel)

    EventSpec can be used to create an Event with annotated models:

        spec.make(InputModel())

    EventSpec can be used with a SagaEvents like this:

        events = SagaEvents()

        @events.entry(spec)
        def event(inp: InputModel) -> OutputModel:
            ...
    """
    def __init__(self, name: str, model_in: Type[In], model_out: Type[Out]):
        """
        :param name: Event name.
        :param model_in: Input model of event.
        :param model_out: Output model of event.
        """
        self.name = name
        self.model_in = model_in
        self.model_out = model_out
        self.ret_name = f'r_{name}'

    def make(self, inp: In) -> Event[In, Out]:
        """
        Creates annotated Event with input data inp.
        """
        return Event(self.name, self.ret_name, inp, self.model_in, self.model_out)


class NotAnEvent(Event[Ok, Ok]):
    """
    Represents absent event, so it won't be sent by `SagaWorker`.
    This event should be used in the `event` function of `SagaWorker`:

        @idempotent_saga(...)
        def saga(worker: SagaWorker)
            result = worker.event(check_something, random.randint(0, 20)).run()
            # result in case of `NotAnEvent` will be `Ok`.

        def check_something(value: int)
            if value > 10:
                return NotAnEvent()
            return Event(...)
    """
    def __init__(self) -> None:
        super().__init__('not_an_event', 'r_not_an_event', data=Ok(),
                         model_in=Ok, model_out=Ok)
