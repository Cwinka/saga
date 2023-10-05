from typing import Callable, Optional, TypeVar, Generic

from saga.models import JobSpec

T = TypeVar('T')
C = TypeVar('C')
CompensationCallback = Callable[[JobSpec[C]], None]


class WorkerJob(Generic[T, C]):
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

    def __init__(self, spec: JobSpec[T],
                 comp_set_callback: CompensationCallback[C] = lambda *_: None) -> None:
        """
        :param spec: Спецификация функции.
        :param comp_set_callback: Обратный вызов компенсации, вызывается перед выполнением spec
                                  и если установлена функция компенсации.
        """
        self._spec = spec
        self._compensation_callback = comp_set_callback
        self._compensation_spec: Optional[JobSpec[C]] = None
        self._run: bool = False
        self._crun: bool = False

    def run(self) -> T:
        """
        Выполнить spec функцию. Метод можно выполнить только один раз.

        :return: Результат spec функции.
        """
        assert not self._run, 'Повторный вызов функции не позволен.'
        if self._compensation_spec:
            self._compensation_callback(self._compensation_spec)
        self._run = True
        return self._spec.call()

    def with_compensation(self, spec: JobSpec[C]) -> 'WorkerJob[T, C]':
        """
        Установить функцию компенсации.

        :param spec: Спецификации компенсационной функции.
        :return: Тот же объект `WorkerJob`.
        """
        self._compensation_spec = spec
        return self

    def compensate(self) -> None:
        """
        Запустить компенсацию, если она существует. Метод можно выполнить только один раз.
        """
        assert self._run, 'Функция не была вызвана. Нечего компенсировать.'
        assert not self._crun, 'Повторный вызов компенсационной функции не позволен.'
        if self._compensation_spec is not None:
            self._compensation_spec.call()
