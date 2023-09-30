import uuid
from typing import Optional

import pytest

from saga.models import JobStatus, Ok, SagaRecord
from saga.saga import SagaJob, SagaRunner, idempotent_saga
from saga.worker import SagaWorker, join_key


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
    id_key = join_key(uuid.uuid4(), 'foo')
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


def test_get_saga_name_via_decorator(runner):
    name = 'foo'
    saga = idempotent_saga(name)(lambda *a: None)
    assert runner.get_saga_name(saga) == name


def test_get_saga_name_via_method(runner):
    name = 'foo'
    saga = lambda *a: None
    runner.register_saga(name=name, saga=saga)
    assert runner.get_saga_name(saga) == name


def test_get_saga_record(runner):
    saga = idempotent_saga('foo')(lambda *a: None)
    key = uuid.uuid4()
    runner.new(key, saga, Ok()).wait()

    record = runner.get_saga_record_by_uid(key, saga)
    assert isinstance(record, SagaRecord)


def test_get_saga_record_by_wkey(runner):
    key = uuid.uuid4()

    @idempotent_saga('foo')
    def saga(worker: SagaWorker, _) -> Optional[SagaRecord]:
        return runner.get_saga_record_by_wkey(worker.idempotent_key)

    result = runner.new(key, saga, Ok()).wait()

    assert isinstance(result, SagaRecord)
