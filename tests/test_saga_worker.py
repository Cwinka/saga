import time
import uuid

import pytest

from saga.events import Event, EventSpec, SagaEvents
from saga.journal import MemoryJournal
from saga.models import NotAnEvent, Ok
from saga.worker import SagaWorker, WorkerJob, join_key


def run_in_worker(x: int) -> str:
    return str(x)


def test_worker_job_create(worker):
    job = worker.job(run_in_worker, 1)
    assert isinstance(job, WorkerJob)


def test_worker_journal(compensator):
    journal = MemoryJournal()
    k = uuid.uuid4()
    worker = SagaWorker(k, '1', journal, compensator, None)
    worker.job(lambda: 1).run()
    assert journal.get_record(f'{join_key(k, "1")}_1') is not None


def test_worker_run(worker):
    x = 42
    result = worker.job(run_in_worker, x).run()
    assert result == str(x)


def test_worker_run_exception(worker):
    class SomeError(Exception):
        pass

    def foo() -> None:
        raise SomeError

    with pytest.raises(SomeError):
        worker.job(foo).run()


def test_worker_run_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    worker.job(run_in_worker, x).with_compensation(foo).run()
    worker.compensate()
    assert compensate_check == x


def test_worker_loop(worker):
    x = 10
    results = []
    for i in range(x):
        results.append(worker.job(run_in_worker, i).run())
    assert results == [str(i) for i in range(x)]


def test_worker_loop_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    for i in range(1, x+1):
        worker.job(run_in_worker, i).with_compensation(foo).run()
    worker.compensate()

    assert compensate_check == x**2/2 + x/2


def test_worker_no_compensate_if_no_run(worker):
    x = 42
    compensate_check = 0

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check += _x

    for i in range(1, x + 1):
        worker.job(run_in_worker, i).with_compensation(foo)
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
    result = worker.event_job(event).run()

    assert event_delivered, 'Событий должно быть доставлено принимающей стороне.'
    assert result.ok == 10


def test_worker_not_an_event_send(worker):
    result = worker.event_job(lambda: NotAnEvent()).run()
    assert isinstance(result, Ok), 'Событие NotAnEvent должно возвращать Ok.'


def test_worker_not_an_event_comp(worker):
    compensation_run = False

    def comp(_: Ok) -> NotAnEvent:
        nonlocal compensation_run
        compensation_run = True
        return NotAnEvent()

    worker.event_job(lambda: NotAnEvent()).with_compensation(comp).run()
    worker.compensate()

    assert not compensation_run, 'Компенсационная функция для NotAnEvent не должна запускаться.'
