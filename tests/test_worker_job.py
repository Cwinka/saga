import pytest

from saga.saga import WorkerJob, SagaCompensate


class SpecialErr(Exception):
    pass


def run_in_job(x: int) -> str:
    return str(x)


def run_in_job_with_raise() -> str:
    raise SpecialErr()


@pytest.fixture()
def compensator() -> SagaCompensate:
    return SagaCompensate()


def test_worker_job_run(compensator):
    x = 42

    job = WorkerJob(compensator, run_in_job, x)
    result = job.run()
    assert result == str(x)


def test_worker_job_err(compensator):
    with pytest.raises(SpecialErr):
        WorkerJob(compensator, run_in_job_with_raise).run()


def test_worker_job_with_compensation(compensator):
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    try:
        WorkerJob(compensator, run_in_job, x).with_compensation(foo).run()
        WorkerJob(compensator, run_in_job_with_raise).run()
    except SpecialErr:
        compensator.run()
    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_multiple_compensations(compensator):
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    def job_with_compensation() -> None:
        try:
            WorkerJob(compensator, run_in_job, x).with_compensation(foo).run()
            WorkerJob(compensator, run_in_job_with_raise).run()
        except SpecialErr:
            compensator.run()

    job_with_compensation()
    assert compensate_check == x

    job_with_compensation()
    assert compensate_check == 2 * x, 'Компенсационная функция была запущена дважды.'
