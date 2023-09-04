import random

from saga.saga import idempotent_saga, SagaJob, SagaWorker, saga_participant


@saga_participant
def random_int_generator() -> int:
    return random.randint(0, 100)


def test_saga_job():
    def foo(worker: SagaWorker) -> int:
        result = worker.run(random_int_generator)
        if result.err():
            print('an error')
        return result.value()

    job = SagaJob(foo, (SagaWorker('1'),))
    assert isinstance(job, SagaJob)


def test_decorator_return():
    @idempotent_saga
    def foo(worker: SagaWorker) -> int:
        return 1

    job = foo(SagaWorker('1'))
    assert isinstance(job, SagaJob)
