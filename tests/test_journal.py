import uuid

import pytest

from saga.journal import MemoryJournal
from saga.models import JobRecord, JobStatus


@pytest.fixture()
def job_record(wk_journal) -> JobRecord:
    return JobRecord(
        uuid=uuid.uuid4(), operation_id=1
    )


def test_get(job_record):
    dct = {job_record.uuid: {job_record.operation_id: job_record}}
    journal = MemoryJournal(dct)
    get_record = journal.get_record(job_record.uuid, job_record.operation_id)
    assert get_record == job_record


def test_create(job_record):
    dct = {}
    journal = MemoryJournal(dct)
    journal.create_record(job_record.uuid, job_record.operation_id)
    assert journal.get_record(job_record.uuid, job_record.operation_id) is not None


def test_update(job_record):
    job_record.status = JobStatus.FAILED
    journal = MemoryJournal({job_record.uuid: {job_record.operation_id: job_record}})
    journal.update_record(job_record.uuid, job_record.operation_id,
                          ['status'], [JobStatus.DONE])
    assert journal.get_record(job_record.uuid, job_record.operation_id).status == JobStatus.DONE


def test_delete(job_record):
    job_record.status = JobStatus.FAILED
    journal = MemoryJournal({job_record.uuid: {job_record.operation_id: job_record}})
    journal.delete_records(job_record.uuid)
    assert journal.get_record(job_record.uuid, job_record.operation_id) is None
