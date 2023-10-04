import time

import pytest

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


def test_worker_event_send(worker, communication_fk):

    events = SagaEvents()
    spec = EventSpec('', model_in=Ok, model_out=Ok)
    event_delivered = False

    @events.entry(spec)
    def receiver(*args) -> Ok:
        nonlocal event_delivered
        event_delivered = True
        return Ok(ok=10)

    def event() -> Event[Ok, Ok]:
        return spec.make(Ok())

    communication_fk.listener(events).run_in_thread()
    time.sleep(0.05)  # wait socket to wake up
    result = worker.event_job(JobSpec(event)).run()

    assert event_delivered, 'Событий должно быть доставлено принимающей стороне.'
    assert result.ok == 10


def test_worker_not_an_event_send(worker):
    result = worker.event_job(JobSpec(lambda: NotAnEvent())).run()
    assert isinstance(result, Ok), 'Событие NotAnEvent должно возвращать Ok.'


def test_worker_event_comp(worker, communication_fk):
    events = SagaEvents()
    spec = EventSpec('e', model_in=Ok, model_out=Ok)
    comp_spec = EventSpec('c', model_in=Ok, model_out=Ok)
    comp_delivered = False

    @events.entry(comp_spec)
    def comp(*args) -> Ok:
        nonlocal comp_delivered
        comp_delivered = True
        return Ok()

    @events.entry(spec)
    def ev(*args):
        time.sleep(0.2)
        return Ok()

    communication_fk.listener(events).run_in_thread()

    with pytest.raises(TimeoutError):
        worker.event_job(JobSpec(spec.make, Ok()), timeout=0)\
            .with_compensation(JobSpec(comp_spec.make, Ok())).run()
    worker.compensate()
    assert comp_delivered, 'Событий компенсации должно быть доставлено принимающей стороне.'
