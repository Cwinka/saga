import random

import pytest

from saga.memo import NotEnoughRetries


class Err(Exception):
    pass


@pytest.fixture()
def function_with_err():
    def wrap():
        raise Err('error')
    return wrap


@pytest.fixture()
def function_with_err_on_first_call():
    x = 0
    def wrap():
        nonlocal x
        x += 1
        if x == 1:
            raise Err('error')
    return wrap


@pytest.fixture()
def function_with_random_return():
    return lambda: random.randint(0, 10000)


def test_memo_return(memoized, function_with_random_return):
    test_f = memoized.memoize(function_with_random_return)
    r1 = test_f()
    r2 = test_f()

    assert r1 == r2, 'Результат функции при повторном вызове должен совпадать.'


def test_memo_raises_original_exception(memoized, function_with_err):
    f = memoized.memoize(function_with_err, retries=1)
    with pytest.raises(Err):
        f()


def test_memo_run_function_retrie_times_on_exception(memoized, function_with_err_on_first_call):
    foo = memoized.memoize(function_with_err_on_first_call, retries=2, retry_interval=0.01)
    try:
        foo()
    except Err:
        pytest.fail("При указании retries=2 функция должна запуститься дважды.")


def test_memo_no_retries_left_raises_not_enough_retries(memoized, wk_journal):
    test_f = memoized.memoize(lambda: None, retries=0)
    with pytest.raises(NotEnoughRetries):
        test_f()


def test_memo_return_original_exception_on_error(memoized, function_with_err):
    f = memoized.memoize(function_with_err, retries=2, retry_interval=0.1)
    with pytest.raises(Err):
        f()


def test_memo_forget_done(memoized, function_with_random_return):
    func = memoized.memoize(function_with_random_return)
    random.seed(1)
    r1 = func()
    memoized.forget_done()
    r2 = func()

    assert r1 != r2, ('При использовании метода "forget_done" повторный вызов функции должен '
                      'вернуть отличающийся результат.')
