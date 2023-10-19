import time
from uuid import UUID

import pytest

from saga.events import EventRaisedException, SagaEvents
from saga.models import EventSpec, JobSpec, JobStatus, NotAnEvent, Ok
from saga.worker_job import WorkerJob


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
always_ok_event = EventSpec[Ok, Ok]('always_ok_event', model_in=Ok, model_out=Ok)
always_ok_sleep_event = EventSpec[Ok, Ok]('always_ok_sleep_event', model_in=Ok, model_out=Ok)
always_raise_event = EventSpec[Ok, Ok]('always_raise_event', model_in=Ok, model_out=Ok)


@events.entry(always_ok_event)
def always_ok(_: UUID, want: Ok) -> Ok:
    return want


@events.entry(always_ok_sleep_event)
def always_ok_sleep(_: UUID, want: Ok) -> Ok:
    time.sleep(want.ok / 1000)
    return want


@events.entry(always_raise_event)
def always_raise(_: UUID, data: Ok) -> Ok:
    raise AttributeError('always_raise')


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


def test_worker_empty_chain_run(worker):
    chain = worker.chain(1)

    result = chain.run()

    assert result is None, 'Запуск цепочки заданий без добавленных заданий должен возвращать None.'


def test_worker_chain_return_last_result(worker):
    chain = worker.chain(1)

    result = chain.add_job(lambda x: x + 1).add_job(lambda x: x + 2).run()

    assert result == 3, 'Запуск цепочки заданий должен возвращать последний результат из цепочки.'


def test_worker_chain_event_result(worker, communication_fk):
    lis = communication_fk.listener(events)
    lis.run_in_thread()
    chain = worker.chain(10)

    result = chain.add_event_job(lambda x: always_ok_event.make(Ok(ok=10))).run()

    lis.shutdown()

    assert result.ok == 10, ('Запуск цепочки заданий не возвращает ожидаемый результат после '
                             'запуска события')


def test_worker_chain_compensation_run_order(worker):
    chain = worker.chain(2)

    initial = 13
    comp_check = initial
    called = 0
    def compensate(_x):
        nonlocal comp_check, called
        called += 1
        if called % 2 == 0:
            comp_check -= _x
        else:
            comp_check /= _x

    chain\
        .add_job(lambda _: None, compensation=compensate)\
        .add_job(lambda _: None, compensation=compensate)\
        .run()
    worker.compensate()

    assert initial / 2 - 2 == comp_check, ('Компенсационные задания в цепочке заданий запускаются '
                                           'некорректно.')


def test_worker_event_raise_error_propagation(worker, communication_fk):
    lis = communication_fk.listener(events)
    lis.run_in_thread()
    time.sleep(0.05)  # wait to wake up

    with pytest.raises(EventRaisedException):
        worker.event_job(JobSpec(always_raise_event.make, Ok(ok=12)), timeout=1).run()
    lis.shutdown()
