import random

import pytest

from saga.saga import SagaJob
from saga.worker import SagaWorker
from saga.models import Ok


def str_return(worker: SagaWorker, _) -> str:
    return '42'


def float_return(worker: SagaWorker, _) -> float:
    return 42.0


def test_saga_job(worker):
    def foo(_worker: SagaWorker, _) -> int:
        return 1

    job = SagaJob(worker, foo, Ok())
    assert isinstance(job, SagaJob)


@pytest.mark.parametrize('test_function, expected_result', [
    (str_return, '42'),
    (float_return, 42.0),
])
def test_saga_job_run(worker, test_function, expected_result):

    job = SagaJob(worker, test_function, Ok())
    job.run()

    result = job.wait()
    assert isinstance(result, type(expected_result)) and \
           result == expected_result, 'Результат выполнения саги возвращается некорректно.'


def test_saga_job_idempotent(journal, compensator):
    def sum_return(worker: SagaWorker, _) -> int:
        s = 0
        for _ in range(10):
            s += worker.job(random.randint, 0, 1000).run()
        return s

    wk1 = SagaWorker('1', journal, compensator, None)
    wk2 = SagaWorker('1', journal, compensator, None)

    result1 = SagaJob(wk1, sum_return, Ok()).wait()
    result2 = SagaJob(wk2, sum_return, Ok()).wait()

    assert result1 == result2, 'Запуск саг с одинаковыми ключами должен давать один результат.'


def test_saga_job_compensation(worker):

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

    job = SagaJob(worker, function, Ok())

    try:
        job.wait()
    except SomeError:
        pass

    assert compensation_check == (x**2/2 + x/2) - x, 'Все компенсационные функции должны быть ' \
                                                     'выполнены при исключении.'
