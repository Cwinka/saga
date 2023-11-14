from typing import Any, Callable, Generic, List, Optional, ParamSpec, TypeVar
from uuid import UUID

from saga.compensator import SagaCompensator
from saga.events import EventSender, auto_send
from saga.journal import WorkerJournal
from saga.logger import logger
from saga.memo import Memoized
from saga.models import Event, In, JobSpec, Ok, Out
from saga.worker_job import WorkerJob

P = ParamSpec('P')
T = TypeVar('T')
InputData = TypeVar('InputData')
LastChainResult = TypeVar('LastChainResult')
SAGA_KEY_SEPARATOR = '&'


class SagaWorker:
    """
    ``SagaWorker`` отвечает за создание ``WorkerJob``.
    ``SagaWorker`` создает точки сохранения для каждого запускаемого ``WorkerJob``,
     а также собирает все компенсации, которые были в них добавлены.

         journal = WorkerJournal()  # журнал сохранения контрольных точек
         worker = SagaWorker('1')

         worker.job(any_function).with_compensation(rollback_any_function).run()
         worker.compensate()
    """

    def __init__(self, uuid: UUID, saga_name: str, journal: WorkerJournal,
                 compensator: SagaCompensator, sender: Optional[EventSender],
                 job_max_retries: int = 1, job_retry_interval: float = 2.0,
                 event_job_timeout: float = 5.0,
                 compensation_max_retries: int = 10, compensation_interval: float = 1,
                 compensation_event_timeout: float = 5):
        """
        :param uuid: Уникальный ключ ``SagaWorker``.
        :param saga_name: Имя саги.
        :param journal: Журнал для хранения результатов выполнения ``WorkerJob``.
        :param compensator: Объект ``SagaCompensator`` хранения компенсационных функций.
        :param sender: Объект ``EventSender`` для отправки событий,
                       если опущенный метод ``event_job`` не может быть использован.
        :param job_max_retries: Максимальное количество повторов задания. При указании больше чем
                                1 задания должны быть идемпотентными.
        :param job_retry_interval: Время, через которое будет совершен повторный вызов задания в
                                   случае исключения.
        :param event_job_timeout: Максимальное время ожидания ответа события.
        :param compensation_max_retries: Количество возможных повторов функции компенсации в случае
                                         исключения в ней. Если количество повторов 0,
                                         тогда будет поднято оригинальное исключение.
        :param compensation_interval: Интервал времени в секундах, через который будет вызван повтор
                                      функции компенсации в случае исключения.
        :param compensation_event_timeout: Время ожидания события компенсации.
        """
        self._uuid = uuid
        self._saga_name = saga_name
        self._memo = Memoized(uuid, journal)
        self._sender = sender
        self._journal = journal
        self._job_max_retries = job_max_retries
        self._job_retry_interval = job_retry_interval
        self._event_job_timeout = event_job_timeout
        self._compensate = compensator or SagaCompensator()
        self._compensation_max_retries = compensation_max_retries
        self._compensation_interval = compensation_interval
        self._compensation_event_timeout = compensation_event_timeout
        self._w_prefix = f'[Saga worker: {uuid} Saga: {saga_name}]'

    @property
    def saga_name(self) -> str:
        """Имя саги."""
        return self._saga_name

    @property
    def uuid(self) -> UUID:
        """Уникальный идентификатор ``SagaWorker``."""
        return self._uuid

    def compensate(self) -> None:
        """Запустить все компенсационные функции."""
        logger.info(f'{self._w_prefix} Запуск заданий компенсаций.')
        self._compensate.run()

    def forget_done(self) -> None:
        """
        Удалить все добавленные записи в журнал, позволяя запуск с тем же идемпотентным ключом.
        """
        logger.info(f'{self._w_prefix} Удаление записей заданий из журнала.')
        self._memo.forget_done()

    def job(self, spec: JobSpec[T, P], retries: Optional[int] = None,
            retry_interval: Optional[float] = None) -> WorkerJob[T, None, P]:
        """
        Создать ``WorkerJob`` со спецификацией ``spec``.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        """
        spec.f = self._memo.memoize(spec.f,
                                    retries=retries or self._job_max_retries,
                                    retry_interval=retry_interval or self._job_retry_interval)
        logger.debug(f'{self._w_prefix} Создание простого задания с функцией "{spec.name}".')
        return WorkerJob[T, None, P](spec, comp_set_callback=self._place_compensation,
                                     uuid=self._uuid, saga_name=self._saga_name)

    def event_job(self, spec: JobSpec[Event[In, Out], P], retries: Optional[int] = None,
                  retry_interval: Optional[float] = None, timeout: Optional[float] = None) \
            -> WorkerJob[Out, Event[Any, Any], P]:
        """
        Создать ``WorkerJob``, который отправляет возвращаемое событие и ожидает его результат.

        :param spec: Спецификация функции.
        :param retries: Количество возможных повторов функции в случае исключения. Если
                        количество повторов 0, тогда будет поднято оригинальное исключение.
        :param retry_interval: Интервал времени в секундах, через который будет вызван повтор
                               функции в случае исключения.
        :param timeout: Время ожидания ответного события.
        """
        assert self._sender is not None, 'Не установлен отправитель событий.'
        spec.f = self._memo.memoize(auto_send(self._sender,  # type: ignore[arg-type]
                                              self._uuid, spec.f,
                                              timeout or self._event_job_timeout),
                                    retries=retries or self._job_max_retries,
                                    retry_interval=retry_interval or self._job_retry_interval)
        logger.debug(f'{self._w_prefix} Создание событийного задания с функцией "{spec.name}".')
        return WorkerJob[Out, Event[Any, Any], P](spec,  # type: ignore[arg-type]
                                                  comp_set_callback=self._place_event_compensation,
                                                  uuid=self._uuid, saga_name=self._saga_name)

    def chain(self, input_data: InputData) -> 'WorkerParametrizedChain[InputData, None]':
        """
        Создать ``WorkerParametrizedChain``, цель которого объединить несколько функций в одну
        цепочку функций.
        """
        return WorkerParametrizedChain(self, input_data)

    def _place_event_compensation(self, spec: JobSpec[Event[Any, Ok], ...]) -> None:
        spec.f = self._memo.memoize(auto_send(self._sender,  # type: ignore[arg-type]
                                              self._uuid,
                                              spec.f,
                                              self._compensation_event_timeout,
                                              True),
                                    retries=self._compensation_max_retries,
                                    retry_interval=self._compensation_interval)
        logger.debug(f'{self._w_prefix} Добавление событийного задания компенсации "{spec.name}".')
        self._compensate.add_compensate(spec)

    def _place_compensation(self, spec: JobSpec[None, ...]) -> None:
        spec.f = self._memo.memoize(spec.f, retries=self._compensation_max_retries,
                                    retry_interval=self._compensation_interval)
        logger.debug(f'{self._w_prefix} Добавление простого задания компенсации "{spec.name}".')
        self._compensate.add_compensate(spec)


class WorkerParametrizedChain(Generic[InputData, LastChainResult]):
    """
    Коллекция заданий ``WorkerJob``. Данный класс предназначен для более короткой записи тех заданий
    для которых входные данные одинаковы.
    К примеру задания записанные так:

        def some_saga(worker: SagaWorker, ctx: ...):
            file = Path(...)
            worker.job(JobSpec(do_1_on_file, file)).run()
            worker.job(JobSpec(do_2_on_file, file)).run()
            worker.job(JobSpec(do_3_on_file, file)).run()

    Могут быть переписаны с помощью ``WorkerParametrizedChain``:

        def some_saga(worker: SagaWorker, ctx: ...):
            file = Path(...)
            worker.chain(file)\
                .add_job(do_1_on_file)\
                .add_job(do_2_on_file)\
                .add_job(do_3_on_file)\
                .run()

    Задания в цепочке выполняются друг за другом, а результат, который возвращает метод ``run``
    извлекается из последнего в цепочке задания. Правила для компенсационных заданий
    такие же как и в ``WorkerJob``.
    """
    def __init__(self,
                 worker: SagaWorker,
                 entity: InputData,
                 _chain: Optional[List[WorkerJob[Any, Any, Any]]] = None,):
        self._worker = worker
        self._entity = entity
        self._chain = _chain or []

    def add_job(self, job: Callable[[InputData], T],
                compensation: Optional[Callable[[InputData], Any]] = None,
                retries: Optional[int] = None, retry_interval: Optional[float] = None)\
            -> 'WorkerParametrizedChain[InputData, T]':
        """
        Добавить задание ``job`` в цепочку заданий. Задания будут выполнены в том порядке,
        в котором они добавляются.

        :param job: Задание, принимающее на вход установленные данные.
        :param compensation: Компенсирующее эффекты задания ``job`` задание.
        :param retries: Максимальное количество повторов задания в случае ошибки.
        :param retry_interval: Интервал повтора задания в случае ошибки.
        """
        wk_job = self._worker.job(JobSpec(job, self._entity), retries=retries,
                                  retry_interval=retry_interval)
        if compensation is not None:
            wk_job.with_parametrized_compensation(compensation)
        self._chain.append(wk_job)
        return WorkerParametrizedChain(self._worker, self._entity,
                                       self._chain)

    def add_event_job(self, job: Callable[[InputData], Event[Any, Out]],
                      compensation: Optional[Callable[[InputData], Event[Any, Any]]] = None,
                      retries: Optional[int] = None, retry_interval: Optional[float] = None,
                      timeout: Optional[float] = None) -> \
            'WorkerParametrizedChain[InputData, Out]':
        """
        Добавить событийное задание ``job`` в цепочку заданий. Задания будут выполнены в
        том порядке, в котором они добавляются.

        :param job: Задание, принимающее на вход установленные данные.
        :param compensation: Компенсирующее эффекты задания ``job`` задание.
        :param retries: Максимальное количество повторов задания в случае ошибки.
        :param retry_interval: Интервал повтора задания в случае ошибки.
        :param timeout: Время ожидания ответа на отправленное событие.
        :return:
        """
        e_job = self._worker.event_job(JobSpec(job, self._entity), retries=retries,
                                       retry_interval=retry_interval, timeout=timeout)
        if compensation is not None:
            e_job.with_parametrized_compensation(compensation)
        self._chain.append(e_job)
        return WorkerParametrizedChain(self._worker, self._entity, self._chain)

    def run(self) -> LastChainResult:
        """Запустить выполнение цепочки заданий."""
        last_result = None
        while self._chain:
            last_result = self._chain.pop(0).run()
        return last_result  # type: ignore[return-value]
