import pytest

from saga.saga import SagaJob, SagaWorker


def str_return(worker: SagaWorker) -> str:
    return '42'


def float_return(worker: SagaWorker) -> float:
    return 42.0


@pytest.mark.parametrize('test_function, expected_result', [
    (str_return, '42'),
    (float_return, 42.0),
])
def test_saga_job_run(test_function, expected_result):

    job = SagaJob(test_function, SagaWorker('1'))
    job.run()

    result = job.wait()
    assert isinstance(result, type(expected_result)) and \
           result == expected_result, 'Результат выполнения саги возвращается некорректно.'
