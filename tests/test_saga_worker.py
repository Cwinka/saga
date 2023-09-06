from saga.saga import SagaWorker, WorkerJob, SagaCompensate


def run_in_worker(x: int) -> str:
    return str(x)


def test_worker_job_create(wk_journal):
    job = SagaWorker('1', wk_journal).job(run_in_worker, 1)
    assert isinstance(job, WorkerJob)


def test_worker_compensator(wk_journal, compensator):
    worker = SagaWorker('1', wk_journal, compensator)

    assert compensator is worker.compensator
