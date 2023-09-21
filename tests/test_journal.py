from saga.journal import MemoryJournal
from saga.models import JobRecord


def test_get():
    record = JobRecord('1')
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
    record = JobRecord(key)
    dct = {}
    journal = MemoryJournal(dct)

    record.result = b'42'
    journal.update_record(record)
    assert dct[key] is record


def test_delete():
    key = '1'
    record = JobRecord(key)
    dct = {record.idempotent_operation_id: record}
    journal = MemoryJournal(dct)
    journal.delete_records(record)

    assert dct == {}
