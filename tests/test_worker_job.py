import pytest

from saga.saga import WorkerJob, JobSPec


class SpecialErr(Exception):
    pass


def run_in_job(x: int) -> str:
    return str(x)


def run_in_job_with_raise() -> str:
    raise SpecialErr()


def test_worker_job_run():
    x = 42

    job = WorkerJob(JobSPec(run_in_job, x))
    result = job.run()
    assert result == str(x)


def test_worker_job_err():
    with pytest.raises(SpecialErr):
        WorkerJob(JobSPec(run_in_job_with_raise)).run()


def test_worker_job_with_compensation():
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    job1 = WorkerJob(JobSPec(run_in_job, x))
    job1.with_compensation(foo).run()
    job1.compensate()
    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_multiple_compensations():
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    def job_with_compensation() -> None:
        job1 = WorkerJob(JobSPec(run_in_job, x))
        job1.with_compensation(foo).run()
        job1.compensate()

    job_with_compensation()
    assert compensate_check == x

    job_with_compensation()
    assert compensate_check == 2 * x, 'Компенсационная функция должна была быть запущена во ' \
                                      'второй раз.'
