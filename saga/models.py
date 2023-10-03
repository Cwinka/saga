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
    Класс используется для указания, что функция ничего не возвращает или ничего не принимает.
    Атрибут ok не имеет смысла, но нужен чтобы корректно управлять данными.
    """
    ok: int = 1


class JobStatus(str, Enum):
    RUNNING = 'RUNNING'
    """ Функция/сага выполняется. """
    DONE = 'DONE'
    """ Функция/сага выполнена без ошибок. """
    FAILED = 'FAILED'
    """ Функция/сага подняла исключение. """


class SagaRecord(BaseModel):
    idempotent_key: str
    """ Идемпотентный уникальный ключ саги. """
    status: JobStatus = JobStatus.RUNNING
    """ Текущий статус выполнения саги. """
    initial_data: bytes = base64.b64encode(Ok().model_dump_json().encode('utf8'))
    """ Изначальные данные саги. """
    traceback: Optional[str] = None
    """ Трассировка ошибки. """
    error: Optional[str] = None
    """ Сообщение об ошибке. """
    failed_time: Optional[datetime] = None
    """ Время возникновения ошибки. """


class JobRecord(BaseModel):
    idempotent_operation_id: str
    """ Уникальный ключ операции. """
    status: JobStatus = JobStatus.RUNNING
    """ Текущий статус операции. """
    result: bytes = base64.b64encode(pickle.dumps(None))
    """ Результат операции. """
    runs: int = 0
    """ Количество запусков. """


class JobSpec(Generic[P, T]):
    """
    Спецификация функции.
    """
    def __init__(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self.f = f
        self._orig_args = args
        self._args: List[Any] = []
        self._orig_kwargs = kwargs

    def with_arg(self, arg: Any) -> 'JobSpec[P, T]':
        """
        Добавляет аргумент `arg` в качестве первого аргумента основной функции.
        Метод следует использовать с осторожностью, так как он не проверяет, может ли основная
        функция принят аргумент `arg`.
        """
        self._args.insert(0, arg)
        return self

    def call(self) -> T:
        """
        Выполнить основную функцию.
        """
        return self.f(*self._args, *self._orig_args, **self._orig_kwargs)


class Event(Generic[In, Out]):
    """
    Событие, цель которого маршрутизация и хранение данных для передачи на удаленный хост.
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
    Спецификация события, описывающее событие.
    Пример спецификации:

        spec = EventSpec('create_it', InputModel, OutputModel)

    Спецификация может быть использована для создания `Event` с аннотированными типами.

        spec.make(InputModel())

    Спецификация может быть использована в `SagaEvents`:

        events = SagaEvents()

        @events.entry(spec)
        def event(inp: InputModel) -> OutputModel:
            ...
    """
    def __init__(self, name: str, model_in: Type[In], model_out: Type[Out]):
        """
        :param name: Имя события.
        :param model_in: Входная модель события.
        :param model_out: Выходная модель события.
        """
        self.name = name
        self.model_in = model_in
        self.model_out = model_out
        self.ret_name = f'r_{name}'

    def make(self, inp: In) -> Event[In, Out]:
        """
        Создает аннотированные `Event` с данными `inp`.
        """
        return Event(self.name, self.ret_name, inp, self.model_in, self.model_out)


class NotAnEvent(Event[Ok, Ok]):
    """
    Событие, говорящее что события не существует.
    Данное событие не будет отправлено при передаче его в `SagaWorker`:

        @idempotent_saga(...)
        def saga(worker: SagaWorker)
            result = worker.event(check_something, random.randint(0, 20)).run()
            # результат `NotAnEvent` будет `Ok`.

        def check_something(value: int)
            if value > 10:
                return NotAnEvent()  # событие не будет отправлено `SagaWorker`
            return Event(...)
    """
    def __init__(self) -> None:
        super().__init__('not_an_event', 'r_not_an_event', data=Ok(),
                         model_in=Ok, model_out=Ok)
