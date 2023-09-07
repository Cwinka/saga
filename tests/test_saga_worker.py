import pytest

from saga.saga import SagaWorker, WorkerJob


def run_in_worker(x: int) -> str:
    return str(x)


def test_worker_job_create(wk_journal):
    job = SagaWorker('1', wk_journal).job(run_in_worker, 1)
    assert isinstance(job, WorkerJob)


def test_worker_compensator(wk_journal, compensator):
    worker = SagaWorker('1', wk_journal, compensator)
    assert compensator is worker.compensator


def test_worker_run(wk_journal):
    x = 42
    worker = SagaWorker('1', wk_journal)
    result = worker.job(run_in_worker, x).run()
    assert result == str(x)


def test_worker_run_exception(wk_journal):
    x = 42

    class SomeError(Exception):
        pass

    def foo() -> None:
        raise SomeError

    worker = SagaWorker('1', wk_journal)
    with pytest.raises(SomeError):
        worker.job(foo).run()


def test_worker_run_compensate(wk_journal):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    worker = SagaWorker('1', wk_journal)
    worker.job(run_in_worker, x).with_compensation(foo).run()
    worker.compensator.run()
    assert compensate_check == x


def test_worker_loop(wk_journal):
    x = 10
    results = []
    worker = SagaWorker('1', wk_journal)
    for i in range(x):
        results.append(worker.job(run_in_worker, i).run())
    assert results == [str(i) for i in range(x)]


def test_worker_loop_compensate(wk_journal):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    worker = SagaWorker('1', wk_journal)
    for i in range(1, x+1):
        worker.job(run_in_worker, i).with_compensation(foo).run()
    worker.compensator.run()

    assert compensate_check == x**2/2 + x/2
