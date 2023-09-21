import pytest

from saga.compensator import SagaCompensator
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.saga import SagaRunner
from saga.worker import SagaWorker


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


@pytest.fixture()
def runner(saga_journal, wk_journal) -> SagaRunner:
    return SagaRunner(saga_journal, wk_journal)
