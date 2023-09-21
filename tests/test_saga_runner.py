import pytest

from saga.models import Ok, JobStatus
from saga.saga import SagaJob, SagaRunner, idempotent_saga
from saga.worker import SagaWorker


@idempotent_saga('foo')
def foo(_worker: SagaWorker, _: Ok) -> int:
    return 1


def test_get_saga(runner):
    assert runner.get_saga('foo') is foo


def test_register_saga(runner):
    def boo(worker: SagaWorker, _: Ok) -> None:
        return None

    runner.register_saga('boo', boo)
    assert runner.get_saga('boo') is boo


def test_saga_runner_new(runner):
    job = runner.new('1', foo, Ok())
    assert isinstance(job, SagaJob)


def test_saga_runner_wrong_saga(runner):
    with pytest.raises(AssertionError):
        runner.new('1', lambda x, y: 1, Ok())


def test_saga_runner_rerun_0(runner):
    runner.new('1', foo, Ok()).wait()

    assert runner.run_incomplete() == 0, 'Завершенные саги не должны быть запущены.'


def test_saga_runner_rerun_1(saga_journal):
    saga = saga_journal.create_saga(SagaRunner.join_key('1', 'foo'))
    saga.initial_data = Ok().model_dump_json().encode('utf8')
    saga.status = JobStatus.RUNNING
    saga_journal.update_saga(saga)

    runner = SagaRunner(saga_journal)

    assert runner.run_incomplete() == 1, 'Незавершенные саги должны быть запущены.'


def test_saga_runner_rerun_exc(runner):

    class Err(Exception):
        pass

    @idempotent_saga('boo')
    def boo(worker: SagaWorker, _: Ok) -> None:
        raise Err

    try:
        runner.new('1', boo, Ok()).wait()
    except Err:
        pass

    assert runner.run_incomplete() == 0, 'Саги, завершенные с ошибкой не должны быть перезапущены.'
