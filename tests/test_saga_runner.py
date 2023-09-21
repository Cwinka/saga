from saga.models import Ok
from saga.saga import SagaJob, SagaRunner
from saga.worker import SagaWorker


def test_saga_runner_new(saga_journal, wk_journal):
    def foo(_worker: SagaWorker, _) -> int:
        return 1

    fk = SagaRunner(saga_journal, wk_journal)

    job = fk.new('1', foo, Ok())
    assert isinstance(job, SagaJob)

