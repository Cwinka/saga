import uuid

import pytest
import redis

from saga.compensator import SagaCompensator
from saga.events import RedisCommunicationFactory, CommunicationFactory
from saga.journal import MemoryJournal, MemorySagaJournal, SagaJournal, WorkerJournal
from saga.memo import Memoized
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
def communication_fk() -> CommunicationFactory:
    return RedisCommunicationFactory(redis.Redis('127.0.0.1', 6379))


@pytest.fixture()
def worker(communication_fk, wk_journal, compensator) -> SagaWorker:
    return SagaWorker(uuid.uuid4(), 'foo', journal=wk_journal,
                      compensator=compensator, sender=communication_fk.sender())


@pytest.fixture()
def runner(saga_journal, wk_journal) -> SagaRunner:
    return SagaRunner(saga_journal, wk_journal)


@pytest.fixture()
def memoized(wk_journal) -> Memoized:
    return Memoized('memo', wk_journal)
