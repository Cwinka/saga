import time
import uuid

from saga import JobStatus, MemorySagaJournal, Ok, SagaRunner, SagaWorker, idempotent_saga
from saga.saga import model_to_initial_data
from saga.worker import join_key


@idempotent_saga('saga')
def saga_1(worker: SagaWorker, _: Ok) -> None:
    print('Hello world')


def main() -> None:

    journal = MemorySagaJournal()
    runner = SagaRunner(saga_journal=journal)

    # Симуляция незавершенной саги, которая могла возникнуть после неожиданного
    # завершения программы
    saga = journal.create_saga(
        join_key(uuid.uuid4(), SagaRunner.get_saga_name(saga_1))
    )
    saga.initial_data = model_to_initial_data(Ok())
    saga.status = JobStatus.RUNNING

    assert runner.run_incomplete() == 1  # Будет запущена saga_1
    # Программа не блокируется после запуска саг
    time.sleep(0.2)


if __name__ == '__main__':
    main()
