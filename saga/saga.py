import base64
import datetime
import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import Any, Concatenate, Dict, Generic, List, Optional, ParamSpec, Tuple, Type, TypeVar
from uuid import UUID

from pydantic import BaseModel

from saga.compensator import SagaCompensator
from saga.events import CommunicationFactory
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.models import JobStatus, SagaRecord
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')
M = TypeVar('M', bound=BaseModel)
SAGA_NAME_ATTR = '__saga_name__'
SAGA_KEY_SEPARATOR = '&'


def model_to_initial_data(data: BaseModel) -> bytes:
    """
    Конвертировать модель в ascii байты.
    """
    return base64.b64encode(data.model_dump_json().encode('utf8'))


def model_from_initial_data(model: Type[M], data: bytes) -> M:
    """
    Привести ascii байты data в модель model.
    """
    return model.model_validate_json(base64.b64decode(data).decode('utf8'))


class SagaJob(Generic[T]):
    """
    `SagaJob` - обертка для функции саги, которая позволяет сохранять состояние выполнения функции.

    def saga1(worker: SagaWorker, my_arg: int):
        pass

    job1 = SagaJob(saga2, SagaWorker('1'), 42)

    `SagaJob` используется для запуска/ожидания саг.

    job1.run()
    job1.wait()  # метод "wait" автоматически вызовет "run", поэтому вызов "run" избыточен.
    """

    _pool = multiprocessing.pool.ThreadPool(os.cpu_count())

    def __init__(self, journal: SagaJournal, worker: SagaWorker,
                 saga: Callable[[SagaWorker, M], T], data: M,
                 forget_done: bool = False,
                 model_to_b: Callable[[M], bytes] = model_to_initial_data) -> None:
        """
        :param journal: Журнал саги.
        :param worker: Обработчик функций саги.
        :param saga: Функция саги.
        :param data: Входные данные саги, если данных нет, используется Ok.
        :param forget_done: Если значение True, то по завершении все сохраненные записи журналов
                            удаляться и сага может быть запущена с тем же идемпотентным ключом.
        """
        self._journal = journal
        self._worker = worker
        self._f_with_compensation = self._compensate_on_exception(saga)
        self._forget_done = forget_done
        self._data = data
        self._model_to_b = model_to_b
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None

    def run(self) -> None:
        """
        Запустить сагу. Не блокирующий метод.
        """
        if self._result is None:
            saga = self._get_saga()
            self._journal.update_saga(saga.idempotent_key, ['initial_data'],
                                      [self._model_to_b(self._data)])
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker, self._data))

    def wait(self, timeout: Optional[float] = None) -> T:
        """
        Подождать выполнения саги. Автоматически вызывает "run".

        :param timeout: Время ожидания завершения выполнения в секундах.
        :return: Результат саги.
        """
        if self._result is None:
            self.run()
        assert self._result is not None
        return self._result.get(timeout)

    def _get_saga(self) -> SagaRecord:
        record = self._journal.get_saga(self._worker.idempotent_key)
        if record is None:
            record = self._journal.create_saga(self._worker.idempotent_key)
        return record

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[Concatenate[bool, P], T]:
        """
        Оборачивает функцию f блоком try except, который запускает компенсационные функции
        при любом исключении внутри f.
        """
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            saga = self._get_saga()
            fields: List[str] = []
            values: List[Any] = []
            try:
                fields.append('status')
                values.append(JobStatus.DONE)
                return f(*args, **kwargs)
            except Exception as e:
                fields.extend(['status', 'failed_time', 'error', 'traceback'])
                values.extend([JobStatus.FAILED, datetime.datetime.now(), str(e),
                               traceback.format_exc()])
                self._worker.compensate()
                raise
            finally:
                self._journal.update_saga(saga.idempotent_key, fields, values)
                if self._forget_done:
                    self._journal.delete_sagas(saga.idempotent_key)
                    self._worker.forget_done()
        return wrap  # type: ignore[return-value]


class SagaRunner:
    """

    SagaRunner - это фабричный класс, который создает объекты SagaJob.

    Чтобы зарегистрировать фракцию в качестве саги, можно использовать два метода:

        # использовать декоратор
        @idempotent_saga('saga_name')
        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...

        # или зарегистрировать функцию вручную:
        def my_saga(worker: SagaWorker, _: Ok) -> Ok:
            ...

        SagaRunner.register_saga('saga_name', my_saga)

    Первым аргументом функции саги является объект `SagaWorker`, предоставляемый `SagaRunner`
    автоматически. Второй аргумент - это входные данные функции saga, наследуемые от
    `pydantic.BaseModel`.
    Если функция saga не нуждается во входных данных, может быть использован класс `Ok`.

    Для создания саги используется метод `new`:

        runner = SagaRunner()

        saga = runner.new('idempotent_key', my_saga, Ok())

    Первым аргументом метода `new` является уникальный идемпотентный ключ. Если сага
    запускается с используемым одного и того же идемпотентного ключа, сага вернет одинаковый
    результат для обоих запусков.

    The `SagaJob.wait` method can raise an exception if it happens inside a saga, so it must be
    used with try except block to process code further.

    Метод `SagaJob.wait` может вернуть исключение, если оно происходит внутри саги.

    Любая запущенная сага может оказаться в незавершенном состоянии (ни в `SagaStatus.DONE`
    ни в `SagaStatus.FAILED`, а в `SagaStatus.RUNNING` состоянии) из-за неожиданного завершения
    работы. Незавершенные саги могут быть выполнены повторно:

        journal = SagaJournalImplementation()  # реализация журнала
        runner = SagaRunner(saga_journal=journal)

        runner.run_incomplete()  # возвращает количество запущенных саг.
    """
    _sagas: Dict[str, Callable[[SagaWorker, M], Any]] = {}

    def __init__(self,
                 saga_journal: Optional[SagaJournal] = None,
                 worker_journal: Optional[WorkerJournal] = None,
                 cfk: Optional[CommunicationFactory] = None,
                 forget_done: bool = False,
                 model_to_b: Callable[[M], bytes] = model_to_initial_data,
                 model_from_b: Callable[[Type[M], bytes], M] = model_from_initial_data):
        self._forget_done = forget_done
        self._cfk = cfk
        self._worker_journal = worker_journal or MemoryJournal()
        self._saga_journal = saga_journal or MemorySagaJournal()
        self._model_to_b = model_to_b
        self._model_from_b = model_from_b

    def new(self, idempotent_key: UUID, saga: Callable[[SagaWorker, M], T], data: M) -> (
            SagaJob)[T]:
        """
        Создать новый объект `SagaJob`.

        :param idempotent_key: Уникальный ключ саги.
        :param saga: Зарегистрированная функция саги.
        :param data: Входные данные саги.
        """
        assert hasattr(saga, SAGA_NAME_ATTR), (f'Функция "{saga.__name__}" не является сагой. '
                                               f'Используйте декоратор "{idempotent_saga.__name__}"'
                                               f' чтобы отметить функцию как сагу.')
        key = self.join_key(idempotent_key, getattr(saga, SAGA_NAME_ATTR))
        worker = SagaWorker(key, journal=self._worker_journal,
                            compensator=SagaCompensator(),
                            sender=self._cfk.sender() if self._cfk is not None else None,)
        return SagaJob(self._saga_journal, worker, saga, data, forget_done=self._forget_done,
                       model_to_b=self._model_to_b)

    def run_incomplete(self) -> int:
        """
        Запустить все незавершенные саги. Возвращает количество запущенных саг. Не блокирует
        выполнение.
        """
        i = 0
        for i, saga in enumerate(self._saga_journal.get_incomplete_saga(), 1):
            idempotent_key, saga_name = self._split_key(saga.idempotent_key)
            saga_f = self.get_saga(saga_name)
            if saga_f is None:
                # FIXME: warning, сагу переименовали, но было найдено старое имя в базе
                pass
            else:
                model: Type[BaseModel] = list(saga_f.__annotations__.values())[1]
                assert not isinstance(model, str), ('Используйте явную аннотацию типа данных саги '
                                                    f'"{saga_f.__name__}", без '
                                                    'оборачивания его в строку.')
                self.new(
                    UUID(idempotent_key), saga_f, self._model_from_b(model, saga.initial_data)
                ).run()
        return i

    @classmethod
    def register_saga(cls, name: str, saga: Callable[[SagaWorker, M], Any]) -> None:
        """
        Зарегистрировать функцию saga с именем name.
        """
        setattr(saga, SAGA_NAME_ATTR, name)
        cls._sagas[name] = saga

    @classmethod
    def get_saga(cls, name: str) -> Optional[Callable[[SagaWorker, M], Any]]:
        """
        Получить зарегистрированную функцию саги с именем name.
        """
        return cls._sagas.get(name)

    @staticmethod
    def join_key(idempotent_key: UUID, saga_name: str) -> str:
        """
        Вернуть строку, которую можно использовать для получения объекта
        `SagaRecord` из `SagaJournal`.
        """
        return f'{idempotent_key}{SAGA_KEY_SEPARATOR}{saga_name}'

    @staticmethod
    def _split_key(joined_key: str) -> Tuple[str, str]:
        key, *name = joined_key.split(SAGA_KEY_SEPARATOR)
        return key, ''.join(name)


def idempotent_saga(name: str) \
        -> Callable[[Callable[[SagaWorker, M], T]], Callable[[SagaWorker, M], T]]:
    """
    Зарегистрировать функцию, как сагу, в `SagaRunner` с именем name.
    """
    def decorator(f: Callable[[SagaWorker, M], T]) -> Callable[[SagaWorker, M], T]:
        SagaRunner.register_saga(name, f)
        return f
    return decorator
