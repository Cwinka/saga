import pytest

from saga.compensator import SagaCompensator
from saga.worker import SagaWorker
from saga.journal import MemoryJournal, WorkerJournal


@pytest.fixture()
def journal() -> WorkerJournal:
    return MemoryJournal()


@pytest.fixture()
def compensator() -> SagaCompensator:
    return SagaCompensator()


@pytest.fixture()
def worker(journal: WorkerJournal, compensator: SagaCompensator) -> SagaWorker:
    return SagaWorker('1', journal=journal, compensator=compensator, sender=None)
