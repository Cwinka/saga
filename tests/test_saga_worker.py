from saga.saga import SagaWorker, WorkerJob, SagaCompensate


def run_in_worker(x: int) -> str:
    return str(x)


def test_worker_job_create():
    job = SagaWorker('1').job(run_in_worker, 1)
    assert isinstance(job, WorkerJob)


def test_worker_compensator():
    compensate = SagaCompensate()
    worker = SagaWorker('1', compensate)

    assert compensate is worker.compensator
