import random
import time
import uuid

import pytest

from saga.models import Ok, JobSpec
from saga.saga import SagaJob
from saga.worker import SagaWorker


def test_saga_job(saga_journal, worker):
    def foo(_worker: SagaWorker, _) -> int:
        return 1

    job = SagaJob(saga_journal, worker, foo, Ok())
    assert isinstance(job, SagaJob)


def test_saga_job_data(saga_journal, worker):
    data = Ok()
    job = SagaJob(saga_journal, worker, lambda *_: None, data)
    assert job.data == data, 'Полученные данные не совпадают с переданными.'


@pytest.mark.parametrize('test_function, expected_result', [
    (lambda _, r: '42', '42'),
    (lambda _, r: 42.0, 42.0),
])
def test_saga_job_run(saga_journal, worker, test_function, expected_result):

    job = SagaJob(saga_journal, worker, test_function, Ok())
    job.run()

    result = job.wait()
    assert isinstance(result, type(expected_result)) and \
           result == expected_result, 'Результат выполнения саги возвращается некорректно.'


@pytest.mark.parametrize('forget_done, assert_f', [
    (False, lambda x, y: x == y,),
    (True, lambda x, y: x != y)
])
def test_saga_job_idempotent(saga_journal, wk_journal, compensator, forget_done, assert_f):
    def sum_return(worker: SagaWorker, _) -> int:
        s = 0
        for _ in range(10):
            s += worker.job(JobSpec(random.randint, 0, 1000)).run()
        return s

    uid = uuid.uuid4()
    wk1 = SagaWorker(uid, '1', wk_journal, compensator, None)
    wk2 = SagaWorker(uid, '1', wk_journal, compensator, None)

    result1 = SagaJob(saga_journal, wk1, sum_return, Ok(), forget_done=forget_done).wait()
    result2 = SagaJob(saga_journal, wk2, sum_return, Ok()).wait()

    assert assert_f(result1, result2), ('Запуск саг с одинаковыми ключами не дает ожидаемого '
                                        'результата.')


def test_saga_job_compensation(worker, saga_journal):

    x = 42
    compensation_check = 0

    class SomeError(Exception):
        pass

    def compensate(_x: int) -> None:
        nonlocal compensation_check
        compensation_check += _x

    def function(_worker: SagaWorker, _) -> None:
        for i in range(1, x+1):
            if i == x:
                raise SomeError
            _worker.job(JobSpec(lambda _x: _x, i))\
                .with_compensation(JobSpec(compensate, i)).run()

    job = SagaJob(saga_journal, worker, function, Ok())

    try:
        job.wait()
    except SomeError:
        pass

    assert compensation_check == (x**2/2 + x/2) - x, 'Все компенсационные функции должны быть ' \
                                                     'выполнены при исключении.'


def test_saga_job_running_property_while_running(saga_journal, worker):
    job = SagaJob(saga_journal, worker, lambda *_: time.sleep(1), Ok())

    job.run()

    assert job.running, 'Пока сага запущена, running свойство должно возвращать True.'


def test_saga_job_running_property_when_finished(saga_journal, worker):
    job = SagaJob(saga_journal, worker, lambda *_: None, Ok())

    job.run()
    job.wait()

    assert not job.running, 'Когда сага завершена, running свойство должно возвращать False.'


def test_saga_job_running_property_if_not_running(saga_journal, worker):
    job = SagaJob(saga_journal, worker, lambda *_: None, Ok())

    assert not job.running, 'Когда сага не запущена, running свойство должно возвращать False.'


def test_saga_job_executed_property_if_not_running(saga_journal, worker):
    job = SagaJob(saga_journal, worker, lambda *_: None, Ok())

    assert not job.executed, 'Когда сага не запущена, executed свойство должно возвращать False.'


def test_saga_job_executed_property_if_run(saga_journal, worker):
    job = SagaJob(saga_journal, worker, lambda *_: None, Ok())

    job.run()

    assert job.executed, 'Когда сага была запущена, executed свойство должно возвращать True.'
