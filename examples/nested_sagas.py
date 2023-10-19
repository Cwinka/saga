import uuid

from pydantic import BaseModel

from saga import SagaRunner, SagaWorker, JobSpec


class DataForFirst(BaseModel):
    count: int


class DataForSecond(BaseModel):
    times: int


def first_saga(worker: SagaWorker, data: DataForFirst) -> None:
    for _ in range(data.count):
        worker.job(JobSpec(lambda: print('Some work'))).run()
    if data.count > 5:
        # Будет запущена ``second_saga``, используя текущего ``SagaWorker``.
        # Все шаги в запущенной саге будут сохраняться аналогично, как и в этой.
        # Перезапуск ``first_saga``, в случае ошибки в second_saga, будет начат с последней успешной
        # операции ``SagaWorker``. Перезапуск только ``second_saga``, используя
        # ``SagaWorker`` с тем же идемпотентным ключом, как и для ``first_saga``, невозможен.
        second_saga(worker, DataForSecond(times=data.count // 2))


def second_saga(worker: SagaWorker, data: DataForSecond) -> None:
    for _ in range(data.times):
        worker.job(JobSpec(lambda: print('Some other work'))).run()


def main() -> None:
    runner = SagaRunner()
    runner.register_saga('first', first_saga)
    runner.register_saga('second', second_saga)
    uid = uuid.uuid4()
    runner.new(uid, first_saga, DataForFirst(count=10)).wait()

    assert runner.get_saga_record_by_uid(uid, first_saga) is not None
    assert runner.get_saga_record_by_uid(uid, second_saga) is None


if __name__ == '__main__':
    main()
