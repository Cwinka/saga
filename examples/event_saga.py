import random

import redis
from pydantic import BaseModel

from saga import Event, EventSpec, Ok, RedisCommunicationFactory, SagaEvents, SagaRunner, \
    SagaWorker, idempotent_saga


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
def ev(data1: Foo) -> Boo:
    return Boo(boo=random.randint(1, 1000))


@events.entry(RollEv)
def roll_ev(x: Foo) -> Ok:
    return Ok()
##


# client part
def event() -> Event[Foo, Boo]:
    return Ev.make(Foo(foo='12'))


def roll_event(x: Boo) -> Event[Foo, Ok]:
    return RollEv.make(Foo(foo=str(x.boo)))


@idempotent_saga('saga')
def saga_2(worker: SagaWorker, _: Ok) -> None:
    e = worker.event(event).with_compensation(roll_event).run()
    print(worker.job(lambda x: random.randint(1, 1000) + x.boo, e).run())


if __name__ == '__main__':
    rd = redis.Redis('localhost', 6379, decode_responses=True)

    # fk = SocketCommunicationFactory('foo')
    fk = RedisCommunicationFactory(rd)
    fk.listener(events).run_in_thread()

    runner = SagaRunner(sender=fk.sender())
    runner.new('1', saga_2, Ok()).wait()
