import uuid
from typing import Callable

import pytest

from saga.models import JobStatus, Ok, SagaRecord
from saga.saga import SagaJob
from saga.worker import SagaWorker, join_key


@pytest.fixture()
def registered_saga(runner) -> Callable[[SagaWorker, Ok], int]:
    def saga(_worker: SagaWorker, data: Ok) -> int:
        return data.ok
    runner.register_saga('saga', saga, Ok)
    return saga


@pytest.fixture()
def runner_with_incomplete_saga(saga_journal, registered_saga, runner):
    id_key = join_key(uuid.uuid4(), runner.get_saga_name(registered_saga))
    saga_journal.create_saga(id_key)
    saga_journal.update_saga(id_key, ['status'], [JobStatus.RUNNING])
    return runner


@pytest.fixture()
def runner_with_failed_saga(saga_journal, registered_saga, runner):
    id_key = join_key(uuid.uuid4(), runner.get_saga_name(registered_saga))
    saga_journal.create_saga(id_key)
    saga_journal.update_saga(id_key, ['status'], [JobStatus.FAILED])
    return runner


def test_get_saga(runner, registered_saga):
    name = runner.get_saga_name(registered_saga)
    assert runner.get_saga(name)[0] is registered_saga


def test_after_register_saga_it_accessible_via_get_saga(runner):
    def saga(wk, ok): ...

    runner.register_saga('saga', saga, Ok)
    assert runner.get_saga('saga')[0] is saga


def test_decorated_sagas_accessible_via_get_saga(runner):
    @runner.saga('saga', Ok)
    def saga(wk, ok): ...

    assert runner.get_saga('saga')[0] is saga


def test_after_register_method_as_saga_it_is_accessible_via_get_saga(runner):
    class Foo:
        def a(self, wk, data):
            pass

    foo = Foo()
    link_to_a_method = foo.a  # без ссылки foo.a is not foo.a
    runner.register_saga('saga', link_to_a_method, Ok)
    assert runner.get_saga('saga')[0] is link_to_a_method


def test_when_trying_to_create_saga_with_unregistered_saga_raises_exception(runner):
    with pytest.raises(AssertionError):
        runner.new(uuid.uuid4(), lambda x, y: 1, Ok())


def test_when_done_saga_exists_run_incomplete_does_not_run_it(runner, registered_saga):
    runner.new(uuid.uuid4(), registered_saga, Ok()).wait()

    assert runner.run_incomplete() == 0, 'Завершенные саги не должны быть запущены.'


def test_when_running_saga_exists_run_incomplete_do_run_it(runner_with_incomplete_saga):
    assert runner_with_incomplete_saga.run_incomplete() == 1, \
        'Незавершенные саги должны быть запущены.'


def test_when_failed_saga_exists_run_incomplete_does_no_run_it(runner_with_failed_saga):
    assert runner_with_failed_saga.run_incomplete() == 0, \
        'Саги, завершенные с ошибкой не должны быть перезапущены.'


def test_saga_record_accessible_via_its_uuid_and_link_to_saga(runner, registered_saga):
    uid = uuid.uuid4()
    runner.new(uid, registered_saga, Ok()).wait()

    record = runner.get_saga_record_by_uid(uid, registered_saga)
    assert isinstance(record, SagaRecord)


def test_new_from_returns_none_with_never_executed_sagas(runner):
    @runner.saga('saga', Ok)
    def saga(wk, ok): ...

    assert runner.new_from(uuid.uuid4(), saga) is None


def test_new_from_returns_saga_job_with_executed_sagas(runner, registered_saga):
    uid = uuid.uuid4()
    runner.new(uid, registered_saga, Ok()).wait()

    job = runner.new_from(uid, registered_saga)
    assert isinstance(job, SagaJob), \
        'Когда либо запущенные саги должны воссоздаваться через метод new_from'
