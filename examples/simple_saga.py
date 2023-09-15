import os
import tempfile
from pathlib import Path

from saga.journal import MemoryJournal
from saga.saga import SagaWorker, idempotent_saga


@idempotent_saga
def create_spec_in_directory(worker: SagaWorker, path: Path) -> None:
    for i in range(10):
        file = path / str(i)
        worker.job(create_spec_file, file, 'foo').with_compensation(remove_spec_file, file).run()
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
    path.mkdir()
    try:
        create_spec_in_directory(SagaWorker('1', MemoryJournal()), path).wait()
    except AttributeError:
        pass
    print(list(path.glob('*')))  # пусто, так как все операции откатились.
    os.rmdir(path)


if __name__ == '__main__':
    main()
