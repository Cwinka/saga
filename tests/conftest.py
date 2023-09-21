import pytest

from saga.compensator import SagaCompensator
from saga.worker import SagaWorker
from saga.journal import MemoryJournal, WorkerJournal, SagaJournal, MemorySagaJournal


@pytest.fixture()
def wk_journal() -> WorkerJournal:
    return MemoryJournal()


@pytest.fixture()
def saga_journal() -> SagaJournal:
    return MemorySagaJournal()


@pytest.fixture()
def compensator() -> SagaCompensator:
    return SagaCompensator()


@pytest.fixture()
def worker(wk_journal: WorkerJournal, compensator: SagaCompensator) -> SagaWorker:
    return SagaWorker('1', journal=wk_journal, compensator=compensator, sender=None)
