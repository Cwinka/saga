import os
import tempfile
import uuid
from pathlib import Path

from pydantic import BaseModel

from saga import SagaRunner, SagaWorker, idempotent_saga
from saga.models import JobSpec


class SpecData(BaseModel):
    path: Path


@idempotent_saga('spec')
def create_spec_in_directory(worker: SagaWorker, data: SpecData) -> None:
    for i in range(10):
        file = data.path / str(i)
        worker.job(JobSpec(create_spec_file, file, 'foo'))\
            .with_compensation(remove_spec_file, file).run()
    raise AttributeError


def create_spec_file(file: Path, content: str) -> None:
    print(f'Writing file {file}')
    with open(file, 'w') as f:
        f.write(content)


def remove_spec_file(_: None, file: Path) -> None:
    print(f'Removing file {file}.')
    os.remove(file)


def main() -> None:
    path = Path(tempfile.gettempdir()) / 'foo'
    path.mkdir(exist_ok=True)

    runner = SagaRunner()
    try:
        runner.new(uuid.uuid4(), create_spec_in_directory, SpecData(path=path)).wait()
    except AttributeError:
        pass
    print(list(path.glob('*')))  # пусто, так как все операции откатились.
    os.rmdir(path)


if __name__ == '__main__':
    main()
