import functools
import json
import socket
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, ParamSpec, Tuple, Type
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
    def wait(self, event: Event[Any, Out]) -> Out:
        """
        Подождать результат события.
        Метод может быть использован только в том случае, если событие было отправлено.
        """


class EventListener(ABC):
    """
    `EventListener` отвечает за получение событий от `EventSender`.
    Когда событие принято, `EventListener` перенаправляет его в соответствующую функцию обработчик.
    Функции обработчики могут быть получены методом `events_map`.
    """

    @abstractmethod
    def run_in_thread(self) -> None:
        pass

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


class SocketEventSender(EventSender):
    def __init__(self, file: str):
        self._file = file
        self._sock: Optional[socket.socket] = None

    def _connect(self) -> None:
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self._file)

    def _disconnect(self) -> None:
        assert self._sock is not None
        self._sock.close()
        self._sock = None

    def send(self, uuid: UUID, event: Event[Any, Any]) -> None:
        data = {'event': event.name, 'return': event.ret_name,
                'model': event.data.model_dump_json(), 'uuid': str(uuid)}
        self._connect()
        assert self._sock is not None
        self._sock.send(json.dumps(data).encode('utf8'))

    def wait(self, event: Event[Any, Out]) -> Out:
        assert self._sock is not None, 'Данные не были отправлены.'
        data = self._sock.recv(1024)
        self._disconnect()
        return event.model_out.model_validate_json(data)


class SocketEventListener(EventListener):
    def __init__(self, file: str, *events: SagaEvents):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(file)
        self._sock = sock
        self._map = self.events_map(*events)

    def run_in_thread(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        self._sock.listen()
        while True:
            conn, _ = self._sock.accept()
            self._handle(conn)
            conn.close()

    def _handle(self, conn: socket.socket) -> None:
        data = conn.recv(1024)
        json_data = json.loads(data)
        model_in, _, handler = self._map[json_data['event']]
        model = model_in.model_validate_json(json_data['model'])
        uuid = UUID(json_data['uuid'])
        ret = handler(uuid, model)
        conn.send(ret.model_dump_json().encode('utf8'))


class RedisEventSender(EventSender):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def send(self, uuid: UUID, event: Event[Any, Any]) -> None:
        self._rd.xadd(event.name, {'return': event.ret_name,
                                   'model': event.data.model_dump_json(),
                                   'uuid': str(uuid)})

    def wait(self, event: Event[Any, Out]) -> Out:
        while True:
            for channel, messages in self._rd.xread({event.ret_name: '0'}, 1,
                                                    block=1000):
                for _id, payload in messages:
                    self._rd.xdel(channel, _id)
                    return event.model_out.model_validate_json(payload['model'])
            self._rd.delete(event.ret_name)


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


class SocketCommunicationFactory(CommunicationFactory):

    def __init__(self, file: str):
        self._file = file

    def listener(self, *events: SagaEvents) -> SocketEventListener:
        return SocketEventListener(self._file, *events)

    def sender(self) -> SocketEventSender:
        return SocketEventSender(self._file)
