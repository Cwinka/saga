import pytest

from saga.models import Ok
from saga.saga import SagaJob, SagaRunner, idempotent_saga
from saga.worker import SagaWorker


def test_saga_runner_new(saga_journal, wk_journal):

    @idempotent_saga('foo')
    def foo(_worker: SagaWorker, _) -> int:
        return 1

    fk = SagaRunner(saga_journal, wk_journal)

    job = fk.new('1', foo, Ok())
    assert isinstance(job, SagaJob)


def test_saga_runner_wrong_saga(saga_journal, wk_journal):
    fk = SagaRunner(saga_journal, wk_journal)

    with pytest.raises(AssertionError):
        job = fk.new('1', lambda x, y: 1, Ok())
