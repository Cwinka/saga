import base64
import pickle
from datetime import datetime
from enum import Enum
from typing import Callable, Generic, Optional, ParamSpec, Type, TypeVar
from uuid import UUID
from pydantic import BaseModel

P = ParamSpec('P')
T = TypeVar('T')
M = TypeVar('M', bound=BaseModel)

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
    uuid: UUID
    """Уникальный идентификатор саги."""
    saga_name: str
    """Имя саги."""
    status: JobStatus = JobStatus.RUNNING
    """Текущий статус выполнения саги."""
    initial_data: bytes = base64.b64encode(Ok().model_dump_json().encode('utf8'))
    """Изначальные данные саги."""
    traceback: Optional[str] = None
    """Трассировка ошибки."""
    error: Optional[str] = None
    """Сообщение об ошибке."""
    failed_time: Optional[datetime] = None
    """Время возникновения ошибки."""


class JobRecord(BaseModel):
    operation_id: int
    """Ключ операции."""
    uuid: UUID
    """Идентификатор саги."""
    status: JobStatus = JobStatus.RUNNING
    """Текущий статус операции."""
    result: bytes = base64.b64encode(pickle.dumps(None))
    """Результат операции."""
    runs: int = 0
    """Количество запусков."""


class JobSpec(Generic[T, P]):
    """Спецификация функции. Аналог функции ``functools.partial``."""
    def __init__(self, f: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs

    @property
    def name(self) -> str:
        """Имя основной функции."""
        return self.f.__name__

    def call(self) -> T:
        """Выполнить основную функцию."""
        return self.f(*self.args, **self.kwargs)


class Event(Generic[In, Out]):
    """
    Событие, цель которого маршрутизация и хранение данных для передачи на удаленный хост.
    """

    def __init__(self, name: str, data: In, model_out: Type[Out]):
        """
        :param name: Имя события.
        :param data: Входные данные события.
        :param model_out: Тип данных, возвращаемых событием.
        """
        self.name = name
        self.data = data
        self.model_out = model_out


class EventSpec(Generic[In, Out]):
    """
    Спецификация события, описывающее событие.
    Пример спецификации:

        spec = EventSpec('create_it', InputModel, OutputModel)

    Спецификация может быть использована для создания ``Event`` с аннотированными типами.

        spec.make(InputModel())

    Спецификация может быть использована в ``SagaEvents``:

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

    def make(self, inp: In) -> Event[In, Out]:
        """
        Создает аннотированные ``Event`` с данными ``inp``.
        """
        return Event(self.name, inp, self.model_out)


class NotAnEvent(Event[Ok, Ok]):
    """
    Событие, говорящее что события не существует.
    Данное событие не будет отправлено при передаче его в ``SagaWorker``:

        @idempotent_saga(...)
        def saga(worker: SagaWorker)
            result = worker.event(check_something, random.randint(0, 20)).run()
            # результат ``NotAnEvent`` будет ``Ok``.

        def check_something(value: int)
            if value > 10:
                return NotAnEvent()  # событие не будет отправлено ``SagaWorker``
            return Event(...)
    """
    def __init__(self) -> None:
        super().__init__('not_an_event', data=Ok(), model_out=Ok)
