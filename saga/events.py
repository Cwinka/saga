import functools
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, ParamSpec, Tuple, Type
from uuid import UUID

import redis
from pydantic import BaseModel

from saga.models import Event, EventSpec, In, Out

P = ParamSpec('P')
EventMap = Dict[str, Tuple[Type[BaseModel],
                           Type[BaseModel],
                           Callable[[UUID, BaseModel], BaseModel]]]


class EventSender(ABC):
    """
    `EventSender` отвечает за отправку событий в `EventListener`.
    """

    @abstractmethod
    def send(self, uuid: UUID, event: Event[Any, Any]) -> None:
        """
        Отправить событие event.
        """

    @abstractmethod
    def wait(self, uuid: UUID, event: Event[Any, Out], timeout: float) -> Out:
        """
        Подождать результат события.
        Метод может быть использован только в том случае, если событие было отправлено.
        Метод может поднять `TimeoutError` в случае превышения времени ожидания `timeout`.
        """


class EventListener(ABC):
    """
    `EventListener` отвечает за получение событий от `EventSender`.
    Когда событие принято, `EventListener` перенаправляет его в соответствующую функцию обработчик.
    Функции обработчики могут быть получены методом `events_map`.
    """

    @abstractmethod
    def run_in_thread(self) -> None:
        """ Запустить прослушивание событий в отдельном потоке. """""

    @staticmethod
    def events_map(*events: 'SagaEvents') -> EventMap:
        """
        Возвращает спецификацию событий `EventMap`.
        Значения `EventMap`: (
            Имя события.
            Модель данных, которую принимает событие.
            Модель данных, которую возвращает событие.
            Обратный вызов, который принимает экземпляр входной модели и возвращает экземпляр
            выходной модели.
        )
        """
        _map: EventMap = {}
        for ev in events:
            for spec, handler in ev.handlers.items():
                _map[spec.name] = (spec.model_in, spec.model_out, handler)
        return _map


class CommunicationFactory(ABC):
    """
    Фабричный класс для создания объектов `EventListener` и `EventSender`.
    """

    @abstractmethod
    def listener(self, *events: 'SagaEvents') -> EventListener:
        """
        Объект слушателя, который ожидает входящего события и перенаправляет его в
        соответствующую функцию обработчик.
        """

    @abstractmethod
    def sender(self) -> EventSender:
        """
        Объект отправителя событий, который отправляет события слушателю.
        """


class SagaEvents:
    """
    SagaEvents - это хранилище событий, подобное ApiRouter.
    """

    def __init__(self) -> None:
        self._handlers: Dict[EventSpec[Any, Any], Callable[[UUID, Any], Any]] = {}

    @property
    def handlers(self) -> Dict[EventSpec[BaseModel, BaseModel],
                               Callable[[UUID, BaseModel], BaseModel]]:
        """
        Возвращает словарь обработчиков событий. Ключами являются спецификации событий,
        значениями - их обработчики.
        """
        return self._handlers

    def entry(self, spec: EventSpec[In, Out]) -> Callable[[Callable[[UUID, In], Out]],
                                                          Callable[[UUID, In], Out]]:
        """
        Зарегистрировать спецификацию spec.
        Зарегистрированные спецификации могут быть получены методом `handlers`.
        """
        def wrap(f: Callable[[UUID, In], Out]) -> Callable[[UUID, In], Out]:
            @functools.wraps(f)
            def inner(uuid: UUID, data: In) -> Out:
                return f(uuid, data)
            self._handlers[spec] = inner
            return inner
        return wrap


class RedisEventSender(EventSender):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def send(self, uuid: UUID, event: Event[Any, Any]) -> None:
        self._rd.xadd(event.name, {'return': f'{uuid}${event.name}',
                                   'model': event.data.model_dump_json(),
                                   'uuid': str(uuid)})

    def wait(self, uuid: UUID, event: Event[Any, Out], timeout: float) -> Out:
        return_channel = f'{uuid}${event.name}'
        block_time_ms = 100
        stream_reader = self._rd.xread({return_channel: '0'}, 1, block=block_time_ms)
        rest = timeout
        while rest > 0:
            for channel, messages in stream_reader:
                for _id, payload in messages:
                    self._rd.delete(return_channel)
                    return event.model_out.model_validate_json(payload['model'])
            rest -= block_time_ms / 1000
        raise TimeoutError(f'Превышено время ожидания ответа сообщения в очереди '
                           f'"{return_channel}". Время ожидания: {timeout} сек.')


class RedisEventListener(EventListener):

    def __init__(self, rd: redis.Redis, *events: SagaEvents):
        self._rd = rd
        self._bind = self.events_map(*events)
        self._streams = {}
        for ev in self._bind:
            self._streams[ev] = '0'

    def run_in_thread(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while True:
            for channel, messages in self._rd.xread(self._streams, len(self._streams), block=1000):
                for _id, payload in messages:
                    self._handle_event(channel, _id, payload)

    def _handle_event(self, channel: str, _id: str, payload: Dict[str, str]) -> None:
        model_in, _, handler = self._bind[channel]
        model = model_in.model_validate_json(payload['model'])
        uuid = UUID(payload['uuid'])
        ret = handler(uuid, model)
        self._rd.xadd(payload['return'], {'model': ret.model_dump_json()})
        self._rd.xdel(channel, _id)


class RedisCommunicationFactory(CommunicationFactory):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def listener(self, *events: SagaEvents) -> RedisEventListener:
        return RedisEventListener(self._rd, *events)

    def sender(self) -> RedisEventSender:
        return RedisEventSender(self._rd)
