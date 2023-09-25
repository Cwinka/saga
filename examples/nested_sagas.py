from pydantic import BaseModel

from saga import SagaRunner, SagaWorker, idempotent_saga


class DataForFirst(BaseModel):
    count: int


class DataForSecond(BaseModel):
    times: int


@idempotent_saga('first')
def first_saga(worker: SagaWorker, data: DataForFirst) -> None:
    for _ in range(data.count):
        worker.job(lambda: print('Some work')).run()
    if data.count > 5:
        # Будет запущена `second_saga`, используя текущего `SagaWorker`.
        # Все шаги в запущенной саге будут сохраняться аналогично, как и в этой.
        # Перезапуск `first_saga`, в случае ошибки в second_saga, будет начат с последней успешной
        # операции `SagaWorker`. Перезапуск только second_saga, используя `SagaWorker` с тем же
        # идемпотентным ключом, как и для `first_saga`, невозможен.
        second_saga(worker, DataForSecond(times=data.count // 2))


@idempotent_saga('second')
def second_saga(worker: SagaWorker, data: DataForSecond) -> None:
    for _ in range(data.times):
        worker.job(lambda: print('Some other work')).run()


def main() -> None:
    runner = SagaRunner()

    runner.new('1', first_saga, DataForFirst(count=10)).wait()


if __name__ == '__main__':
    main()
