from typing import Callable, Generic, Optional, ParamSpec, TypeVar
from uuid import UUID

from saga.logger import logger
from saga.models import JobSpec

T = TypeVar('T')
C = TypeVar('C')
P = ParamSpec('P')
P_2 = ParamSpec('P_2')
CompensationCallback = Callable[[JobSpec[C, ...]], None]


class WorkerJob(Generic[T, C, P]):
    """
    `WorkerJob` унифицированный объект для запуска функций:

        def any_function() -> int:
            ...

        job = WorkerJob(JobSpec(any_function))
        job.run()

    Для того, чтобы откатить результат выполнения функции `any_function` необходимо добавить
    компенсирующую функцию `any_function`:

        def rollback_any_function() -> None:
            ...

        job = WorkerJob(JobSpec(any_function))
        job.with_compensation(JobSpec(rollback_any_function))
        job.run()
        ...
        job.compensate()
    """

    def __init__(self, spec: JobSpec[T, P],
                 comp_set_callback: CompensationCallback[C] = lambda *_: None,
                 uuid: Optional[UUID] = None, saga_name: str = 'unknown') -> None:
        """
        :param spec: Спецификация функции.
        :param comp_set_callback: Обратный вызов компенсации, вызывается перед выполнением spec
                                  и если установлена функция компенсации.
        :param uuid: UUID `SagaWorker`.
        :param saga_name: Имя саги.
        """
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[C, ...]] = None
        self._run: bool = False
        self._crun: bool = False
        self._lg_prefix = f'[WJ: {uuid or "unknown"} S: {saga_name}]'

    def run(self) -> T:
        """
        Выполнить `spec` функцию. Метод можно вызвать только один раз.

        :return: Результат `spec` функции.
        """
        assert not self._run, (f'{self._lg_prefix} Повторное использование '
                               f'"{self.__class__.__name__}" для вызова '
                               f'функции "{self._spec.name}" запрещено.')
        if self._compensation_spec:
            self._compensation_callback(self._compensation_spec)
        self._run = True
        logger.info(f'{self._lg_prefix} Выполняется функция "{self._spec.name}".')
        return self._spec.call()

    def with_compensation(self, spec: JobSpec[C, P_2]) -> 'WorkerJob[T, C, P]':
        """
        Установить функцию компенсации.

        :param spec: Спецификации компенсационной функции.
        :return: Тот же объект `WorkerJob`.
        """
        self._compensation_spec = spec
        logger.debug(f'{self._lg_prefix} Устанавливается компенсационная функция '
                     f'"{self._compensation_spec.name}" для компенсации "{self._spec.name}".')
        return self

    def with_parametrized_compensation(self, compensation: Callable[P, C]) -> 'WorkerJob[T, C, P]':
        """
        Установить функцию компенсации, которая принимает те же аргументы, что и основная функция.
        Важно, что при использовании в основной функции итерируемых объектов, в функцию
        компенсацию они придут пустыми.

        :param compensation: Компенсационная функция.
        :return: Тот же объект `WorkerJob`.
        """
        self._compensation_spec = JobSpec(compensation, *self._spec.args,
                                          **self._spec.kwargs)
        logger.debug(f'{self._lg_prefix} Устанавливается параметризованная компенсационная функция '
                     f'"{self._compensation_spec.name}" для компенсации "{self._spec.name}".')
        return self

    def compensate(self) -> None:
        """
        Запустить компенсацию, если она существует. Метод можно выполнить только один раз.
        """
        assert self._run, 'Функция не была вызвана. Нечего компенсировать.'
        assert not self._crun, 'Повторный вызов компенсационной функции не позволен.'
        if self._compensation_spec is not None:
            logger.info(f'{self._lg_prefix} Выполняется компенсационная функция '
                        f'"{self._compensation_spec.name}".')
            self._compensation_spec.call()

    def wc(self, spec: JobSpec[C, P_2]) -> 'WorkerJob[T, C, P]':
        """
        Сокращение для `with_compensation`.
        """
        return self.with_compensation(spec)

    def wpc(self, compensation: Callable[P, C]) -> 'WorkerJob[T, C, P]':
        """
        Сокращение для `with_parametrized_compensation`.
        """
        return self.with_parametrized_compensation(compensation)
