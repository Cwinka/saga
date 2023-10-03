import random

import pytest

from saga.memo import NotEnoughRetries, object_to_bytes


class Err(Exception):
    pass


def test_memo_return(memoized):
    test_f = memoized.memoize(lambda: random.randint(0, 10000))
    r1 = test_f()
    r2 = test_f()

    assert r1 == r2, 'Результат функции при повторном вызове должен совпадать.'


def test_memo_exc(memoized):
    def f():
        raise Err()
    f = memoized.memoize(f, retries=1)
    with pytest.raises(Err):
        f()


def test_memo_retries(memoized):
    def foo():
        x = random.randint(0, 1)  # будет 1 на втором вызове
        if x == 0:
            raise Err()

    foo = memoized.memoize(foo, retries=2, retry_interval=0.01)
    random.seed(10)
    try:
        foo()
    except Err:
        pytest.fail("При указании retries=2 функция должна запуститься дважды.")


def test_memo_no_enough_retries(memoized, wk_journal):
    wk_journal.create_record('memo_1')
    wk_journal.update_record('memo_1', ['runs'], [2])

    test_f = memoized.memoize(lambda: None, retries=2)
    with pytest.raises(NotEnoughRetries):
        test_f()


def test_memo_return_orig_exc(memoized, wk_journal):
    wk_journal.create_record('memo_1')
    wk_journal.update_record('memo_1',
                             ['runs', 'result'],
                             [2, object_to_bytes(Err())])

    test_f = memoized.memoize(lambda: None, retries=2)
    with pytest.raises(Err):
        test_f()


def test_memo_forget_done(memoized):
    test_f = memoized.memoize(lambda: random.randint(0, 10000))
    random.seed(1)

    r1 = test_f()
    memoized.forget_done()
    r2 = test_f()

    assert r1 != r2, ('При использовании метода "forget_done" повторный вызов функции должен '
                      'вернуть отличающийся результат.')
