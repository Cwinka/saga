import base64
import datetime
import functools
import multiprocessing.pool
import os
import traceback
from collections.abc import Callable
from typing import Any, Generic, List, Optional, ParamSpec, Type, TypeVar

from pydantic import BaseModel

from saga.journal import SagaJournal
from saga.logger import logger
from saga.models import JobStatus, M
from saga.worker import SagaWorker

P = ParamSpec('P')
T = TypeVar('T')
Saga = Callable[[SagaWorker, M], T]


def model_to_initial_data(data: BaseModel) -> bytes:
    """
    Конвертировать модель в ascii байты.
    """
    return base64.b64encode(data.model_dump_json().encode('utf8'))


def model_from_initial_data(model: Type[M], data: bytes) -> M:
    """
    Привести ascii байты data в модель ``model``.
    """
    return model.model_validate_json(base64.b64decode(data).decode('utf8'))


class SagaJob(Generic[T, M]):
    """
    ``SagaJob`` - обертка для функции саги, которая позволяет сохранять состояние выполнения
    функции.

    Первым аргументом функции саги является объект ``SagaWorker``.
    Второй аргумент - это входные данные функции saga, наследуемые от ``pydantic.BaseModel``.
    Если функция саги не нуждается во входных данных, может быть использован класс ``Ok``.

        def saga1(worker: SagaWorker, my_arg: int):
            pass

        job1 = SagaJob(saga2, SagaWorker('1'), 42)

    ``SagaJob.run``/``SagaJob.wait`` используется для запуска/ожидания саг соответственно.

        job1.run()
        job1.wait()  # метод "wait" автоматически вызовет "run", поэтому вызов "run" избыточен.

    ВАЖНО: Метод ``SagaJob.wait`` может вернуть исключение, если оно происходит внутри саги.
    """

    _pool = multiprocessing.pool.ThreadPool(os.cpu_count())

    def __init__(self, journal: SagaJournal, worker: SagaWorker, saga: Saga[M, T],
                 data: M, forget_done: bool = False,
                 model_to_b: Callable[[M], bytes] = model_to_initial_data) -> None:
        """
        :param journal: Журнал саги.
        :param worker: Обработчик функций саги.
        :param saga: Функция саги.
        :param data: Входные данные саги.
        :param forget_done: Если значение True, то по завершении все сохраненные записи журналов
                            удаляться и сага может быть запущена с тем же идемпотентным ключом.
        :param model_to_b: Функция конвертации модели данных в байты.
        """
        self._journal = journal
        self._worker = worker
        self._f_with_compensation = self._compensate_on_exception(saga)
        self._forget_done = forget_done
        self._data = data
        self._model_to_b = model_to_b
        self._result: Optional[multiprocessing.pool.ApplyResult[T]] = None
        self._s_prefix = f'[Saga job: {worker.uuid} Saga: {worker.saga_name}]'

    def run(self) -> None:
        """
        Запустить сагу. Не блокирующий метод.
        """
        if self._result is None:
            self._create_initial_saga_record()
            logger.info(f'{self._s_prefix} Запуск саги.')
            self._result = self._pool.apply_async(self._f_with_compensation,
                                                  args=(self._worker, self._data))

    def wait(self, timeout: Optional[float] = None) -> T:
        """
        Подождать выполнения саги. Автоматически вызывает ``run``.

        :param timeout: Время ожидания завершения выполнения в секундах.
        :return: Результат саги.
        """
        if self._result is None:
            self.run()
        assert self._result is not None
        logger.info(f'{self._s_prefix} Ожидание завершения саги.')
        return self._result.get(timeout)

    def _create_initial_saga_record(self) -> None:
        if self._journal.get_saga(self._worker.uuid) is None:
            self._journal.create_saga(self._worker.uuid, self._worker.saga_name)
            self._journal.update_saga(self._worker.uuid,
                                      ['initial_data'],
                                      [self._model_to_b(self._data)])

    def _compensate_on_exception(self, f: Callable[P, T]) -> Callable[P, T]:
        """
        Оборачивает функцию ``f`` блоком try except, который запускает компенсационные функции
        при любом исключении внутри ``f``.
        """
        @functools.wraps(f)
        def wrap(*args: P.args, **kwargs: P.kwargs) -> T:
            fields: List[str] = []
            values: List[Any] = []
            try:
                fields.append('status')
                values.append(JobStatus.DONE)
                r = f(*args, **kwargs)
                logger.info(f'{self._s_prefix} Сага успешно завершена.')
                return r
            except Exception as e:
                fields.extend(['status', 'failed_time', 'error', 'traceback'])
                values.extend([JobStatus.FAILED, datetime.datetime.now(), str(e),
                               traceback.format_exc()])
                logger.error(f'{self._s_prefix} Необработанное исключение в саге: {e}')
                self._worker.compensate()
                logger.info(f'{self._s_prefix} Сага завершена с исключением.')
                raise
            finally:
                record = self._journal.get_saga(self._worker.uuid)
                if record is None:
                    logger.warning(f'{self._s_prefix} Запись в журнале о саге '
                                   f'"{self._worker.saga_name}" c uuid: {self._worker.uuid} '
                                   f'не найдена. Невозможно обновить состояние саги.')
                else:
                    self._journal.update_saga(record.uuid, fields, values)
                    if self._forget_done:
                        self._journal.delete_sagas(record.uuid)
                        self._worker.forget_done()
        return wrap


class EagerSagaJob(SagaJob[T, M], Generic[T, M]):
    def run(self) -> None:
        if self._result is None:
            self._create_initial_saga_record()
            self._result = self._f_with_compensation(self._worker,  # type: ignore[assignment]
                                                     self._data)

    def wait(self, timeout: Optional[float] = None) -> T:
        if self._result is None:
            self.run()
        logger.info(f'{self._s_prefix} Ожидание завершения саги.')
        return self._result  # type: ignore[return-value]
