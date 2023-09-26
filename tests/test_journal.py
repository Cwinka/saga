from saga.journal import MemoryJournal
from saga.models import JobRecord, JobStatus


def test_get():
    record = JobRecord(idempotent_operation_id='1')
    dct = {'1': record}
    journal = MemoryJournal(dct)
    get_record = journal.get_record('1')
    assert get_record is not None
    assert get_record.idempotent_operation_id == record.idempotent_operation_id


def test_create():
    key = '1'
    dct = {}
    journal = MemoryJournal(dct)
    created_record = journal.create_record(key)
    assert isinstance(created_record, JobRecord)
    assert created_record.idempotent_operation_id == key
    assert dct[key] is created_record


def test_update():
    key = '1'
    record = JobRecord(idempotent_operation_id=key)
    record.status = JobStatus.FAILED
    dct = {key: record}
    journal = MemoryJournal(dct)
    journal.update_record(key, ['status'], [JobStatus.DONE])
    assert dct[key].status == JobStatus.DONE


def test_delete():
    key = '1'
    record = JobRecord(idempotent_operation_id=key)
    dct = {key: record}
    journal = MemoryJournal(dct)
    journal.delete_records(key)

    assert dct == {}
