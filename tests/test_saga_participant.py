import pytest

from saga.saga import saga_participant


class SpecialErr(Exception):
    pass


@saga_participant
def run_participant(x: int) -> str:
    return str(x)


@saga_participant
def run_participant_with_raise() -> str:
    raise SpecialErr()


def test_participant_run():
    x = 42
    result = run_participant(x)

    assert result.value() == str(x)


def test_participant_run_with_raise():
    result = run_participant_with_raise()
    assert result.is_err()

    with pytest.raises(AssertionError):
        result.value()
