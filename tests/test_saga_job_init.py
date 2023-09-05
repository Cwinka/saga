from saga.saga import idempotent_saga, SagaJob, SagaWorker


def test_saga_job():
    def foo(worker: SagaWorker) -> int:
        return 1

    job = SagaJob(foo, SagaWorker('1'))
    assert isinstance(job, SagaJob)


def test_decorator_return():
    @idempotent_saga
    def foo(worker: SagaWorker) -> int:
        return 1

    job = foo(SagaWorker('1'))
    assert isinstance(job, SagaJob)
