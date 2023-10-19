from typing import Any, List, ParamSpec

from saga.logger import logger
from saga.models import JobSpec

P = ParamSpec('P')


class SagaCompensator:
    """
    ``SagaCompensator`` отвечает за временное хранение и запуск функций компенсации, которые были
    в него добавлены.
    ``SagaCompensator`` может использоваться одновременно с одним ``SagaWorker``.
    """
    def __init__(self) -> None:
        self._compensations: List[JobSpec[None, ...]] = []

    def add_compensate(self, spec: JobSpec[Any, P]) -> None:
        """
        Добавить компенсационную функцию spec к существующим компенсациям.
        """
        self._compensations.append(spec)

    def run(self) -> None:
        """
        Запустить все добавленные компенсационные функции.
        Может быть запущено только один раз. При повторном запуске не имеет эффекта.
        """
        while self._compensations:
            comp = self._compensations.pop()
            logger.info(f'[Compensation] Выполнение функции компенсации "{comp.name}".')
            comp.call()
