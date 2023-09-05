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


def test_worker_job_run(worker):
    x = 42

    result = worker.job(run_in_worker, x).run()

    assert result == str(x)


def test_worker_job_err(worker):
    with pytest.raises(SpecialErr):
        worker.job(run_in_worker_with_raise).run()


def test_worker_job_with_compensation(worker):
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    with pytest.raises(SpecialErr):
        with worker.compensate():
            worker.job(run_in_worker, x).with_compensation(foo).run()
            worker.job(run_in_worker_with_raise).run()

    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_compensation_no_run(worker):
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    with pytest.raises(SpecialErr):
        worker.job(run_in_worker, x).with_compensation(foo).run()
        worker.job(run_in_worker_with_raise).run()

    assert compensate_check == 0, 'Компенсационная функция была запущена без контекстного ' \
                                  'менеджера.'