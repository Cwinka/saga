import random

import redis
from pydantic import BaseModel

from saga.events import RedisEventListener, RedisEventSender, SagaEvents
from saga.models import Event, EventSpec, Ok
from saga.saga import idempotent_saga
from saga.worker import SagaWorker


# shared part
class Boo(BaseModel):
    boo: int


class Foo(BaseModel):
    foo: str


Ev = EventSpec('bob',  model_in=Foo, model_out=Boo)
RollEv = EventSpec('roll_bob', model_in=Boo, model_out=Ok)
##

# remote part
events = SagaEvents()


@events.entry(Ev)
def ev(data1: Foo) -> Boo:
    return Boo(boo=random.randint(1, 1000))


@events.entry(RollEv)
def roll_ev(x: Boo) -> Ok:
    return Ok()
##


# client part
def event() -> Event[Foo, Boo]:
    return Ev.make(Foo(foo='12'))


def roll_event(x: Boo) -> Event[Boo, Ok]:
    return RollEv.make(x)


@idempotent_saga
def saga_2(worker: SagaWorker) -> None:
    e = worker.event(event).with_compensation(roll_event).run()
    print(worker.job(lambda x: random.randint(1, 1000) + x.boo, e).run())


if __name__ == '__main__':
    rd = redis.Redis('localhost', 6379, decode_responses=True)
    ls = RedisEventListener(rd, events)
    sender = RedisEventSender(rd)
    ls.run_in_thread()

    saga_2(SagaWorker('1', sender=sender)).wait()
