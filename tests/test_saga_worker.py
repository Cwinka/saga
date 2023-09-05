import pytest

from saga.saga import saga_participant, SagaWorker, SagaResult


class SpecialErr(Exception):
    pass


@saga_participant
def run_in_worker(x: int) -> str:
    return str(x)


@saga_participant
def run_in_worker_with_raise() -> str:
    raise SpecialErr()


@pytest.fixture()
def worker() -> SagaWorker:
    return SagaWorker('1')


def test_worker_run(worker):
    x = 42

    result = worker.run(run_in_worker, x)

    assert isinstance(result, SagaResult)
    assert result.value() == str(x)


def test_worker_err(worker):
    result = worker.run(run_in_worker_with_raise)

    assert isinstance(result, SagaResult)
    assert result.is_err()
    with pytest.raises(AssertionError):
        result.value()
