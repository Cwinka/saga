import time
import uuid

from saga import JobStatus, MemorySagaJournal, Ok, SagaRunner, SagaWorker, SagaJournal
from saga.saga import model_to_initial_data
from saga.worker import join_key


def saga(worker: SagaWorker, _: Ok) -> None:
    print('Hello world')


def make_incomplete_record(runner: SagaRunner, saga_journal: SagaJournal):
    # Симуляция незавершенной саги, которая могла возникнуть после неожиданного
    # завершения программы
    idempotent_key = join_key(uuid.uuid4(), runner.get_saga_name(saga))
    saga_record = saga_journal.create_saga(idempotent_key)
    saga_record.initial_data = model_to_initial_data(Ok())
    saga_record.status = JobStatus.RUNNING
    saga_journal.update_saga(idempotent_key, ['initial_data', 'status'],
                             [model_to_initial_data(Ok()), JobStatus.RUNNING])


def main() -> None:

    journal = MemorySagaJournal()
    runner = SagaRunner(saga_journal=journal)
    runner.register_saga('saga', saga, Ok)

    make_incomplete_record(runner, journal)

    assert runner.run_incomplete() == 1  # Будет запущена saga
    # Программа не блокируется после запуска саг
    time.sleep(0.2)


if __name__ == '__main__':
    main()
