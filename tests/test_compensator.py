from saga.compensator import SagaCompensator
from saga.models import JobSpec


def test_compensation_order():
    comp_check = ''
    times = 42

    def compensate(_x: str) -> None:
        nonlocal comp_check
        comp_check += _x

    compensator = SagaCompensator()
    for i in range(times):
        compensator.add_compensate(JobSpec(compensate, str(i)))
    compensator.run()

    assert comp_check == ''.join([str(j) for j in range(times-1, -1, -1)]), \
        'Порядок запуска компенсаций должен идти снизу вверх.'
