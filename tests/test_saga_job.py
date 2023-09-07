import random

import pytest

from saga.saga import idempotent_saga, SagaJob, SagaWorker


def str_return(worker: SagaWorker) -> str:
    return '42'


def float_return(worker: SagaWorker) -> float:
    return 42.0


def test_saga_job(wk_journal):
    def foo(worker: SagaWorker) -> int:
        return 1

    job = SagaJob(foo, SagaWorker('1', wk_journal))
    assert isinstance(job, SagaJob)


def test_decorator_return(wk_journal):
    @idempotent_saga
    def foo(worker: SagaWorker) -> int:
        return 1

    job = foo(SagaWorker('1', wk_journal))
    assert isinstance(job, SagaJob)


@pytest.mark.parametrize('test_function, expected_result', [
    (str_return, '42'),
    (float_return, 42.0),
])
def test_saga_job_run(test_function, expected_result, wk_journal):

    job = SagaJob(test_function, SagaWorker('1', wk_journal))
    job.run()

    result = job.wait()
    assert isinstance(result, type(expected_result)) and \
           result == expected_result, 'Результат выполнения саги возвращается некорректно.'


def sum_return(worker: SagaWorker, x: int) -> int:
    s = 0
    for _ in range(x):
        s += worker.job(random.randint, 0, 1000).run()
    return s


def test_saga_job_idempotent(wk_journal):

    result1 = SagaJob(sum_return, SagaWorker('1', wk_journal), 10).wait()
    result2 = SagaJob(sum_return, SagaWorker('1', wk_journal), 10).wait()

    assert result1 == result2, 'Запуск саг с одинаковыми ключами должен давать один результат.'


def test_saga_job_compensation(wk_journal):

    x = 42
    compensation_check = 0

    class SomeError(Exception):
        pass

    def compensate(_x: int) -> None:
        nonlocal compensation_check
        compensation_check += _x

    def function(worker: SagaWorker) -> None:
        for i in range(1, x+1):
            if i == x:
                raise SomeError
            worker.job(lambda: i).with_compensation(compensate).run()

    job = SagaJob(function, SagaWorker('1', wk_journal))

    try:
        job.wait()
    except SomeError:
        pass

    assert compensation_check == (x**2/2 + x/2) - x, 'Все компенсационные функции должны быть ' \
                                                     'выполнены при исключении.'
