from typing import List, Any

from saga.models import JobSpec


class SagaCompensator:
    """
    A SagaCompensator is responsible to hold and run compensation functions which has been added
    to it.
    SagaCompensator is used if an exception happens in saga function. When an exception is raised
    first of all compensation functions are executed (in reverse order which they were added) and
    then exception is reraised.
    """
    def __init__(self) -> None:
        self._compensations: List[JobSpec[..., None]] = []

    def add_compensate(self, spec: JobSpec[Any, Any]) -> None:
        """
        Add compensation function.
        """
        self._compensations.append(spec)

    def clear(self) -> None:
        """
        Clear all added compensation functions.
        """
        self._compensations.clear()

    def run(self) -> None:
        """
        Runs all added compensation functions.
        """
        self._compensations.reverse()
        while self._compensations:
            self._compensations.pop().call()
