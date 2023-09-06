import pickle

from enum import Enum
from typing import Optional
from datetime import datetime
from dataclasses import dataclass


class JobStatus(str, Enum):
    RUNNING = 'RUNNING'
    """ Method running inside saga. """
    COMPENSATED = 'COMPENSATED'
    """ Compensation method has been executed inside saga. """
    DONE = 'DONE'
    """ Method has been executed inside saga. """
    FAILED = 'FAILED'
    """ Method has encourage an exception during execution. """


@dataclass
class JobRecord:
    idempotent_operation_id: str
    """ A unique key op an operation inside saga. """
    status: JobStatus = JobStatus.RUNNING
    """ Current status of an operation. """
    payload: bytes = pickle.dumps(None)
    """ Return content of an operation. """
    traceback: Optional[str] = None
    """ Traceback of an operation """
    error: Optional[str] = None
    """ Error message of an operation """
    failed_time: Optional[datetime] = None
    """ Error time when exception happened. """
