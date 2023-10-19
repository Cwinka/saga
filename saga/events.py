import dataclasses
import functools
import os
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, Dict, ParamSpec, Tuple, Type
from uuid import UUID

import redis
from pydantic import BaseModel

from saga.models import Event, EventSpec, In, Out, NotAnEvent, Ok

P = ParamSpec('P')
EventMap = Dict[str, Tuple[Type[BaseModel],
                           Type[BaseModel],
                           Callable[[UUID, BaseModel], BaseModel]]]


class EventReturns(BaseModel):
    json_model: str
    error: str = ''


class EventRaisedException(Exception):
    pass


class EventSender(ABC):
    """
    ``EventSender`` отвечает за отправку событий в ``EventListener``.
    """

    @abstractmethod
    def send(self, uuid: UUID, event: Event[Any, Any], cancel_previous_uuid: bool) -> None:
        """
        Отправить событие ``event``.
        """

    @abstractmethod
    def wait(self, uuid: UUID, event: Event[Any, Out], timeout: float) -> Out:
        """
        Подождать результат события.
        Метод может быть использован только в том случае, если событие было отправлено.
        Метод может поднять ``TimeoutError`` в случае превышения времени ожидания ``timeout``.
        """


class EventListener(ABC):
    """
    ``EventListener`` отвечает за получение событий от ``EventSender``.
    Когда событие принято, ``EventListener`` перенаправляет его в соответствующую функцию
    обработчик. Функции обработчики могут быть получены методом ``events_map``.
    """

    @abstractmethod
    def run_in_thread(self) -> None:
        """ Запустить прослушивание событий в отдельном потоке. """""

    def shutdown(self) -> None:
        """ Завершить прослушивание событий. """

    @staticmethod
    def events_map(*events: 'SagaEvents') -> EventMap:
        """
        Возвращает спецификацию событий ``EventMap``.
        Значения ``EventMap``: (
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
    Фабричный класс для создания объектов ``EventListener`` и ``EventSender``.
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


def auto_send(sender: EventSender, uuid: UUID,
              f: Callable[P, Event[Any, Out]], timeout: float,
              cancel_previous_uuid: bool = False) -> Callable[P, Out]:
    """
    Оборачивает функцию ``f``, автоматически отправляя событие с помощью ``sender`` объекта и
    возвращая результат.

    :param sender: Отправитель событий.
    :param uuid: Уникальный идентификатор операции.
    :param f: Функция, возвращающее событие.
    :param timeout: Максимальное время ожидания ответа.
    :param cancel_previous_uuid: Отменить предыдущую операцию с таким же ``uuid``.
    """
    @functools.wraps(f)
    def wrap(*args: P.args, **kwargs: P.kwargs) -> Out:
        event = f(*args, **kwargs)
        if isinstance(event, NotAnEvent):
            return Ok()  # type: ignore[return-value]
        sender.send(uuid, event, cancel_previous_uuid=cancel_previous_uuid)
        return sender.wait(uuid, event, timeout=timeout)
    return wrap


class SagaEvents:
    """
    ``SagaEvents`` - это хранилище событий, подобное ``ApiRouter``.
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
        Зарегистрированные спецификации могут быть получены методом ``handlers``.
        """
        def wrap(f: Callable[[UUID, In], Out]) -> Callable[[UUID, In], Out]:
            self.add_entry(spec, f)
            return f
        return wrap

    def add_entry(self, spec: EventSpec[In, Out], entrypoint: [Callable[[UUID, In], Out]]) -> None:
        assert self._handlers.get(spec) is None, f'Обработкик события "{spec.name}" уже существует.'
        self._handlers[spec] = entrypoint


class RedisEventSender(EventSender):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def send(self, uuid: UUID, event: Event[Any, Any], cancel_previous_uuid: bool) -> None:
        return_channel = f'{uuid}${event.name}'
        # Удаление канала нужно чтобы в нем не осталось непрочитанных данных. Даже в случае
        # наличия данных в канале, если функция идемпотентна то второй вызов ничего не сделает,
        # если нет, тогда отправка события в неидемпотентного получателя не должна произойти вовсе.
        self._rd.delete(return_channel)
        self._rd.xadd(event.name, {'return': return_channel,
                                   'model': event.data.model_dump_json(),
                                   'uuid': str(uuid),
                                   'cancel': int(cancel_previous_uuid)})

    def wait(self, uuid: UUID, event: Event[Any, Out], timeout: float) -> Out:
        return_channel = f'{uuid}${event.name}'
        block_time_ms = 100
        channels = {return_channel: '0'}
        rest = timeout
        while rest > 0:
            for channel, messages in self._rd.xread(channels, 1, block=block_time_ms):
                for _id, payload in messages:
                    self._rd.delete(return_channel)
                    result = EventReturns.model_validate(payload)
                    if result.error:
                        raise EventRaisedException(result.error)
                    return event.model_out.model_validate_json(result.json_model)
            rest -= block_time_ms / 1000
        raise TimeoutError(f'Превышено время ожидания ответа сообщения в очереди '
                           f'"{return_channel}". Время ожидания: {timeout} сек.')


class RedisEventListener(EventListener):

    @dataclasses.dataclass
    class Message:
        uuid: UUID
        channel: str
        return_channel: str
        message_id: str
        cancel_previous_future: int

    def __init__(self, rd: redis.Redis, *events: SagaEvents):
        self._rd = rd
        self._bind = self.events_map(*events)
        self._streams = {}
        self._running_p: Dict[UUID, Future[Any]] = {}
        self._working = True
        self._executor = ThreadPoolExecutor(os.cpu_count())
        for ev in self._bind:
            self._streams[ev] = '0'

    def run_in_thread(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def shutdown(self) -> None:
        self._working = False
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _run(self) -> None:
        while self._working:
            self._read_channels()

    def _read_channels(self) -> None:
        for channel, messages in self._rd.xread(self._streams, len(self._streams), block=1000):
            model_in, _, handler = self._bind[channel]
            message_id = '0'
            for message_id, payload in messages:
                message = self.Message(UUID(payload['uuid']), channel, payload['return'], message_id,
                                       int(payload['cancel']))
                model = model_in.model_validate_json(payload['model'])
                # Удаление сообщения нужно потому, что если функция не идемпотентная,
                # тогда нельзя запускать ее второй раз, при остановке посередине. Ответственность
                # за перезапуск лежит на отправителе.
                self._rd.xdel(channel, message_id)
                if message.cancel_previous_future:
                    future = self._running_p.get(message.uuid)
                    if future is not None and not future.cancel():
                        shed = functools.partial(self._schedule_fut, message, model, handler)
                        future.add_done_callback(shed)
                        continue
                self._schedule_fut(message, model, handler)
            self._streams[channel] = message_id
        done = []
        for uuid, future in self._running_p.items():
            if future.done():
                done.append(uuid)
        for uuid in done:
            del self._running_p[uuid]

    def _schedule_fut(self, message: Message, model: BaseModel,
                      handler: Callable[[UUID, BaseModel], BaseModel], *_) -> None:
        if not self._working:
            return
        future = self._executor.submit(handler, message.uuid, model)
        future.add_done_callback(functools.partial(self._future_done, message))
        self._running_p[message.uuid] = future

    def _future_done(self, message: Message, future: Future[BaseModel]) -> None:
        try:
            model = future.result(timeout=0.1)
        except Exception as e:
            self._rd.xadd(message.return_channel,
                          EventReturns(json_model='', error=str(e)).model_dump())
            return

        try:
            json_model = model.model_dump_json()
        except AttributeError as e:
            err = f'Обработчик канала "{message.channel}" не вернул экземпляр pydantic модели: {e}'
            self._rd.xadd(message.return_channel,
                          EventReturns(json_model='', error=err).model_dump())
            return

        self._rd.xadd(message.return_channel,
                      EventReturns(json_model=json_model).model_dump())


class RedisCommunicationFactory(CommunicationFactory):

    def __init__(self, rd: redis.Redis):
        self._rd = rd

    def listener(self, *events: SagaEvents) -> RedisEventListener:
        return RedisEventListener(self._rd, *events)

    def sender(self) -> RedisEventSender:
        return RedisEventSender(self._rd)
