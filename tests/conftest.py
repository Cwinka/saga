import pytest

from saga.saga import SagaCompensator, SagaWorker
from saga.journal import MemoryJournal


@pytest.fixture()
def worker() -> SagaWorker:
    return SagaWorker('1', journal=MemoryJournal(), compensator=SagaCompensator())
