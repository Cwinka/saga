import pytest

from saga.saga import WorkerJob, SagaCompensate, WorkerJournal, JobSPec


class SpecialErr(Exception):
    pass


def run_in_job(x: int) -> str:
    return str(x)


def run_in_job_with_raise() -> str:
    raise SpecialErr()


def test_worker_job_run(compensator, wk_journal):
    x = 42

    job = WorkerJob('1', compensator, wk_journal, JobSPec(run_in_job, x))
    result = job.run()
    assert result == str(x)


def test_worker_job_err(compensator, wk_journal):
    with pytest.raises(SpecialErr):
        WorkerJob('1', compensator, wk_journal, JobSPec(run_in_job_with_raise)).run()


def test_worker_job_with_compensation(compensator, wk_journal):
    compensate_check = 0
    x = 42

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    job1 = WorkerJob('1', compensator, wk_journal, JobSPec(run_in_job, x))
    job1.with_compensation(foo).run()
    compensator.run()
    assert compensate_check == x, 'Компенсационная функция не была запущена.'


def test_worker_job_with_multiple_compensations(compensator, wk_journal):
    compensate_check = 0
    x = 42
    op_id = 1
    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    def job_with_compensation() -> None:
        nonlocal op_id
        job1 = WorkerJob(str(op_id), compensator, wk_journal, JobSPec(run_in_job, x))
        job1.with_compensation(foo).run()
        op_id += 1
        compensator.run()

    job_with_compensation()
    assert compensate_check == x

    job_with_compensation()
    assert compensate_check == 2 * x, 'Компенсационная функция должна была быть запущена во ' \
                                      'второй раз.'
