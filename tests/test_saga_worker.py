import time

import pytest
from uuid import UUID

from saga.events import SagaEvents
from saga.models import NotAnEvent, Ok, JobSpec, Event, EventSpec, JobStatus
from saga.worker import WorkerJob


class SomeError(Exception):
    pass


def test_worker_job_create(worker):
    job = worker.job(JobSpec(lambda: 1))
    assert isinstance(job, WorkerJob)


def test_worker_run(worker):
    x = 42
    result = worker.job(JobSpec(lambda: x)).run()
    assert result == x


def test_worker_run_done_status(worker, wk_journal):
    worker.job(JobSpec(lambda: 1)).run()
    assert wk_journal.get_record(f'{worker.idempotent_key}_1').status == JobStatus.DONE


def test_worker_run_exception(worker):
    def foo() -> None:
        raise SomeError()

    with pytest.raises(SomeError):
        worker.job(JobSpec(foo)).run()


def test_worker_run_fail_status(worker, wk_journal):
    def foo() -> None:
        raise SomeError()
    try:
        worker.job(JobSpec(foo)).run()
    except SomeError:
        pass
    assert wk_journal.get_record(f'{worker.idempotent_key}_1').status == JobStatus.FAILED


def test_worker_run_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check = _x

    worker.job(JobSpec(lambda: None))\
        .with_compensation(JobSpec(foo, x)).run()
    worker.compensate()
    assert compensate_check == x


def test_worker_loop(worker):
    x = 10
    results = []
    for i in range(x):
        results.append(worker.job(JobSpec(lambda _i: _i, i)).run())
    assert results == list(range(x))


def test_worker_loop_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check += _x
    for i in range(1, x+1):
        worker.job(JobSpec(lambda: None))\
            .with_compensation(JobSpec(foo, i)).run()
    worker.compensate()

    assert compensate_check == x**2/2 + x/2


def test_worker_no_compensate_if_no_run(worker):
    x = 42
    compensate_check = 0

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check += _x

    for i in range(1, x + 1):
        worker.job(JobSpec(lambda: None))\
            .with_compensation(JobSpec(foo, i))
    worker.compensate()

    assert compensate_check == 0, ('Метод `with_compensation` не должен добавлять компенсацию, '
                                   'если функция не была запущена.')


events = SagaEvents()
always_ok_event = EventSpec('always_ok_event', model_in=Ok, model_out=Ok)
always_ok_sleep_event = EventSpec('always_ok_sleep_event', model_in=Ok, model_out=Ok)


@events.entry(always_ok_event)
def always_ok(uuid: UUID, want: Ok) -> Ok:
    return want


@events.entry(always_ok_sleep_event)
def always_ok_sleep(uuid: UUID, want: Ok) -> Ok:
    time.sleep(want.ok / 1000)
    return want


def test_worker_event_send(worker, communication_fk):

    lis = communication_fk.listener(events)
    lis.run_in_thread()
    time.sleep(0.05)  # wait to wake up
    result = worker.event_job(JobSpec(always_ok_event.make, Ok(ok=12)), timeout=1).run()
    lis.shutdown()

    assert result.ok == 12, 'Событий должно быть доставлено принимающей стороне.'


def test_worker_not_an_event_send(worker):
    result = worker.event_job(JobSpec(lambda: NotAnEvent())).run()
    assert isinstance(result, Ok), 'Событие NotAnEvent должно возвращать Ok.'


def test_worker_event_comp(worker, communication_fk):
    lis = communication_fk.listener(events)
    lis.run_in_thread()

    with pytest.raises(TimeoutError):
        worker.event_job(JobSpec(always_ok_sleep_event.make, Ok(ok=200)), timeout=0.1)\
            .with_compensation(JobSpec(always_ok_event.make, Ok())).run()
    worker.compensate()
    lis.shutdown()
