import pytest

from saga.compensator import SagaCompensator
from saga.worker import SagaWorker
from saga.journal import MemoryJournal


@pytest.fixture()
def worker() -> SagaWorker:
    return SagaWorker('1', journal=MemoryJournal(), compensator=SagaCompensator())
