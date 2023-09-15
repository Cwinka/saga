import pytest

from saga.saga import SagaWorker, WorkerJob, SagaCompensator
from saga.journal import MemoryJournal


def run_in_worker(x: int) -> str:
    return str(x)


def test_worker_job_create(worker):
    job = worker.job(run_in_worker, 1)
    assert isinstance(job, WorkerJob)


def test_worker_compensator():
    comp = SagaCompensator()
    worker = SagaWorker('1', compensator=comp)
    assert comp is worker.compensator


def test_worker_journal():
    journal = MemoryJournal()
    worker = SagaWorker('1', journal)
    worker.job(lambda: 1).run()
    assert journal.get_record('1_1') is not None


def test_worker_run(worker):
    x = 42
    result = worker.job(run_in_worker, x).run()
    assert result == str(x)


def test_worker_run_exception(worker):
    class SomeError(Exception):
        pass

    def foo() -> None:
        raise SomeError

    with pytest.raises(SomeError):
        worker.job(foo).run()


def test_worker_run_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check = int(_x)

    worker.job(run_in_worker, x).with_compensation(foo).run()
    worker.compensator.run()
    assert compensate_check == x


def test_worker_loop(worker):
    x = 10
    results = []
    for i in range(x):
        results.append(worker.job(run_in_worker, i).run())
    assert results == [str(i) for i in range(x)]


def test_worker_loop_compensate(worker):
    x = 42
    compensate_check = 0

    def foo(_x: str) -> None:
        nonlocal compensate_check
        compensate_check += int(_x)

    for i in range(1, x+1):
        worker.job(run_in_worker, i).with_compensation(foo).run()
    worker.compensator.run()

    assert compensate_check == x**2/2 + x/2
