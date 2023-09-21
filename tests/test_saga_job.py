import random

import pytest

from saga.saga import SagaJob
from saga.worker import SagaWorker
from saga.models import Ok


def str_return(worker: SagaWorker, _) -> str:
    return '42'


def float_return(worker: SagaWorker, _) -> float:
    return 42.0


def test_saga_job(saga_journal, worker):
    def foo(_worker: SagaWorker, _) -> int:
        return 1

    job = SagaJob(saga_journal, worker, foo, Ok())
    assert isinstance(job, SagaJob)


@pytest.mark.parametrize('test_function, expected_result', [
    (str_return, '42'),
    (float_return, 42.0),
])
def test_saga_job_run(saga_journal, worker, test_function, expected_result):

    job = SagaJob(saga_journal, worker, test_function, Ok())
    job.run()

    result = job.wait()
    assert isinstance(result, type(expected_result)) and \
           result == expected_result, 'Результат выполнения саги возвращается некорректно.'


@pytest.mark.parametrize('forget_done, assert_f, msg', [
    (False, lambda x, y: x == y,
     'Запуск саг с одинаковыми ключами должен давать один результат.'),
    (True, lambda x, y: x != y,
     'Запуск саг с одинаковыми ключами, но с очисткой журнала после выполнения должен давать '
     'разный результат.')
])
def test_saga_job_idempotent(saga_journal, wk_journal, compensator, forget_done, assert_f, msg):
    def sum_return(worker: SagaWorker, _) -> int:
        s = 0
        for _ in range(10):
            s += worker.job(random.randint, 0, 1000).run()
        return s

    wk1 = SagaWorker('1', wk_journal, compensator, None)
    wk2 = SagaWorker('1', wk_journal, compensator, None)

    result1 = SagaJob(saga_journal, wk1, sum_return, Ok(), forget_done=forget_done).wait()
    result2 = SagaJob(saga_journal, wk2, sum_return, Ok()).wait()

    assert assert_f(result1, result2), msg


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
            _worker.job(lambda: i).with_compensation(compensate).run()

    job = SagaJob(saga_journal, worker, function, Ok())

    try:
        job.wait()
    except SomeError:
        pass

    assert compensation_check == (x**2/2 + x/2) - x, 'Все компенсационные функции должны быть ' \
                                                     'выполнены при исключении.'
