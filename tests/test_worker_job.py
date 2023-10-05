import pytest

from saga.worker_job import WorkerJob
from saga.models import JobSpec


class SpecialErr(Exception):
    pass


def test_worker_job_run():
    x = 42

    job = WorkerJob(JobSpec(lambda _x: _x, x))
    result = job.run()
    assert result == x


def test_worker_job_err():
    def run_in_job_with_raise() -> str:
        raise SpecialErr()

    with pytest.raises(SpecialErr):
        WorkerJob(JobSpec(run_in_job_with_raise)).run()


def test_worker_job_with_compensation():
    compensate_check = 0
    x = 42

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check = _x

    job1 = WorkerJob(JobSpec(lambda: 1))
    job1.with_compensation(JobSpec(foo, x)).run()
    job1.compensate()
    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_parametrized_compensation():
    compensate_check = 0
    x = 42

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check = _x

    job1 = WorkerJob(JobSpec(lambda _x: _x, x))
    job1.with_parametrized_compensation(foo).run()
    job1.compensate()
    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_multiple_compensations():
    compensate_check = 0
    x = 42

    def foo(_x: int) -> None:
        nonlocal compensate_check
        compensate_check += _x

    def job_with_compensation() -> None:
        job1 = WorkerJob(JobSpec(lambda: 1))
        job1.with_compensation(JobSpec(foo, x)).run()
        job1.compensate()

    job_with_compensation()
    assert compensate_check == x

    job_with_compensation()
    assert compensate_check == 2 * x, 'Компенсационная функция должна была быть запущена во ' \
                                      'второй раз.'
