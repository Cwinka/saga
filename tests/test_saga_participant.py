import pytest

from saga.saga import saga_participant


class SpecialErr(Exception):
    pass


@saga_participant
def run_in_worker(x: int) -> str:
    return str(x)


@saga_participant
def run_in_worker_err() -> str:
    raise SpecialErr()


def test_worker_run():
    x = 42
    result = run_in_worker(x)

    assert result.value() == str(x)


def test_worker_err():
    result = run_in_worker_err()
    assert result.is_err()

    with pytest.raises(AssertionError):
        result.value()
