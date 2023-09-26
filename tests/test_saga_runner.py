import uuid

import pytest

from saga.models import JobStatus, Ok
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
    job = runner.new(uuid.uuid4(), foo, Ok())
    assert isinstance(job, SagaJob)


def test_saga_runner_wrong_saga(runner):
    with pytest.raises(AssertionError):
        runner.new(uuid.uuid4(), lambda x, y: 1, Ok())


def test_saga_runner_rerun_0(runner):
    runner.new(uuid.uuid4(), foo, Ok()).wait()

    assert runner.run_incomplete() == 0, 'Завершенные саги не должны быть запущены.'


def test_saga_runner_rerun_1(saga_journal):
    id_key = SagaRunner.join_key(uuid.uuid4(), 'foo')
    saga_journal.create_saga(id_key)
    saga_journal.update_saga(id_key, ['status'], [JobStatus.RUNNING])

    runner = SagaRunner(saga_journal)

    assert runner.run_incomplete() == 1, 'Незавершенные саги должны быть запущены.'


def test_saga_runner_rerun_exc(runner):

    class Err(Exception):
        pass

    @idempotent_saga('boo')
    def boo(worker: SagaWorker, _: Ok) -> None:
        raise Err

    try:
        runner.new(uuid.uuid4(), boo, Ok()).wait()
    except Err:
        pass

    assert runner.run_incomplete() == 0, 'Саги, завершенные с ошибкой не должны быть перезапущены.'
