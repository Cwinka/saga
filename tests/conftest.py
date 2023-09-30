import uuid

import pytest

from saga.compensator import SagaCompensator
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.saga import SagaRunner
from saga.worker import SagaWorker
from saga.events import SocketCommunicationFactory, CommunicationFactory


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
def communication_fk(tmp_path) -> CommunicationFactory:
    sock = tmp_path / 'sock'
    return SocketCommunicationFactory(sock.as_posix())


@pytest.fixture()
def worker(communication_fk, wk_journal, compensator) -> SagaWorker:
    return SagaWorker(uuid.uuid4(), 'foo', journal=wk_journal,
                      compensator=compensator, sender=communication_fk.sender())


@pytest.fixture()
def runner(saga_journal, wk_journal) -> SagaRunner:
    return SagaRunner(saga_journal, wk_journal)
