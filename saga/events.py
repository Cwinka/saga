import functools
import json
import socket
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, ParamSpec, Tuple

import redis
from pydantic import BaseModel

from saga.models import Event, EventSpec, In, Out

P = ParamSpec('P')
EventMap = Dict[str, Tuple[BaseModel, BaseModel, Callable[[BaseModel], BaseModel]]]


class EventSender(ABC):

    @abstractmethod
    def send(self, event: Event[Any, Any]) -> None:
        pass

    @abstractmethod
    def wait(self, event: Event[Any, Out]) -> Out:
        pass


class SagaEvents:

    def __init__(self) -> None:
        self._handlers: Dict[EventSpec[Any, Any], Callable[[Any], Any]] = {}

    @property
    def handlers(self) -> Dict[EventSpec[Any, Any], Callable[[Any], BaseModel]]:
        return self._handlers

    def entry(self, spec: EventSpec[In, Out]) -> Callable[[Callable[[In], Out]],
                                                          Callable[[In], Out]]:
        def wrap(f: Callable[[In], Out]) -> Callable[[In], Out]:
            @functools.wraps(f)
            def inner(data: In) -> Out:
                return f(data)
            self._handlers[spec] = inner
            return inner
        return wrap


class EventListener(ABC):

    @abstractmethod
    def run_in_thread(self) -> None:
        pass

    @staticmethod
    def events_map(*events: 'SagaEvents') -> EventMap:
        _map: EventMap = {}
        for ev in events:
            for spec, handler in ev.handlers.items():
                _map[spec.name] = (spec.model_in, spec.model_out, handler)
        return _map


class SocketEventSender(EventSender):
    def __init__(self, file: str):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(file)
        self._sock = sock

    def send(self, event: Event[Any, Any]) -> None:
        data = {'event': event.name, 'return': event.ret_name,
                'model': event.data.model_dump_json()}
        self._sock.send(json.dumps(data).encode('utf8'))

    def wait(self, event: Event[Any, Out]) -> Out:
        data = self._sock.recv(1024)
        return event.model_out.model_validate_json(data)


class RedisEventSender(EventSender):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def send(self, event: Event[Any, Any]) -> None:
        self._rd.xadd(event.name, {'return': event.ret_name,
                                   'model': event.data.model_dump_json()})

    def wait(self, event: Event[Any, Out]) -> Out:
        while True:
            for channel, messages in self._rd.xread({event.ret_name: '0'}, 1,
                                                    block=1000):
                for _id, payload in messages:
                    self._rd.xdel(channel, _id)
                    return event.model_out.model_validate(payload)
            self._rd.delete(event.ret_name)


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

    def _handle(self, conn) -> None:
        while True:
            data = conn.recv(1024)
            json_data = json.loads(data)
            model_in, _, handler = self._map[json_data['event']]
            ret = handler(model_in.model_validate_json(json_data['model']))
            conn.send(ret.model_dump_json().encode('utf8'))


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
                    ret = handler(model_in.model_validate_json(payload['model']))
                    self._rd.xadd(payload['return'], ret.model_dump())
                    self._rd.xdel(channel, _id)
