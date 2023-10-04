import random
import tempfile
import uuid

import redis
from pydantic import BaseModel

from saga import Event, EventSpec, Ok, SagaEvents, SagaRunner, \
    SagaWorker, idempotent_saga, RedisCommunicationFactory, JobSpec


# shared part
class Boo(BaseModel):
    boo: int


class Foo(BaseModel):
    foo: str


Ev = EventSpec('bob',  model_in=Foo, model_out=Boo)
RollEv = EventSpec('roll_bob', model_in=Foo, model_out=Ok)
##

# remote part
events = SagaEvents()


@events.entry(Ev)
def ev(uid: uuid.UUID, data1: Foo) -> Boo:
    return Boo(boo=random.randint(1, 1000))


@events.entry(RollEv)
def roll_ev(uid: uuid.UUID, x: Foo) -> Ok:
    return Ok()
##


# client part
def event() -> Event[Foo, Boo]:
    return Ev.make(Foo(foo='12'))


def roll_event() -> Event[Foo, Ok]:
    return RollEv.make(Foo(foo='12'))


@idempotent_saga('saga')
def saga_2(worker: SagaWorker, _: Ok) -> None:
    print(
        worker.event_job(JobSpec(event), timeout=1).with_compensation(JobSpec(roll_event)).run()
    )


if __name__ == '__main__':
    rd = redis.Redis('localhost', 6379, decode_responses=True)

    cfk = RedisCommunicationFactory(rd)
    cfk.listener(events).run_in_thread()

    runner = SagaRunner(cfk=cfk)
    runner.new(uuid.uuid4(), saga_2, Ok()).wait()
