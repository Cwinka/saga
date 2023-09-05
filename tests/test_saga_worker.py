import pytest

from saga.saga import SagaWorker


class SpecialErr(Exception):
    pass


def run_in_worker(x: int) -> str:
    return str(x)


def run_in_worker_with_raise() -> str:
    raise SpecialErr()


@pytest.fixture()
def worker() -> SagaWorker:
    return SagaWorker('1')


def test_worker_run(worker):
    x = 42

    result = worker.job(run_in_worker, x).run()

    assert result == str(x)


def test_worker_err(worker):
    with pytest.raises(SpecialErr):
        worker.job(run_in_worker_with_raise).run()
