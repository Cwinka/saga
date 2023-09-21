import os
import tempfile
import time
from pathlib import Path

from pydantic import BaseModel

from saga import SagaRunner, SagaWorker, idempotent_saga, Ok, MemorySagaJournal, JobStatus


@idempotent_saga('saga')
def saga_1(worker: SagaWorker, _: Ok) -> None:
    print('Hello world')


def main() -> None:

    journal = MemorySagaJournal()
    runner = SagaRunner(saga_journal=journal)

    # Симуляция незавершенной саги, которая могла возникнуть после неожиданного
    # завершения программы
    saga = journal.create_saga(SagaRunner.join_key('1', 'saga'))
    saga.initial_data = Ok().model_dump_json().encode('utf8')
    saga.status = JobStatus.RUNNING

    assert runner.run_incomplete() == 1  # Будет запущена saga_1
    # Программа не блокируется после запуска саг
    time.sleep(0.2)


if __name__ == '__main__':
    main()
