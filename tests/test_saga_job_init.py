from saga.saga import idempotent_saga, SagaJob, SagaWorker


def test_saga_job(wk_journal):
    def foo(worker: SagaWorker) -> int:
        return 1

    job = SagaJob(foo, SagaWorker('1', wk_journal))
    assert isinstance(job, SagaJob)


def test_decorator_return(wk_journal):
    @idempotent_saga
    def foo(worker: SagaWorker) -> int:
        return 1

    job = foo(SagaWorker('1', wk_journal))
    assert isinstance(job, SagaJob)
