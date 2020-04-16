# -*- coding: utf-8 -*-

# Assorted constants and functions involving errors goes here.

import logging
import sys
from enum import Enum

LOG = logging.getLogger(__name__)


class Errors(Enum):
    NO_PROFILE = 1
    MISSING_SETTING = 2
    SERVICE_TIMEOUT = 3
    SERVICE_REDIRECTS = 4
    SERVICE_ERROR = 5
    NO_LOCATOR = 6
    FILE_ERROR = 7
    NGAS_ERROR = 8
    SIZE_MISMATCH = 9


TERMINAL_ERRORS = {
    Errors.NO_PROFILE: 'no CAPO profile provided',
    Errors.MISSING_SETTING: 'missing required setting',
    Errors.SERVICE_TIMEOUT: 'request to locator service timed out',
    Errors.SERVICE_REDIRECTS: 'too many redirects on locator service',
    Errors.SERVICE_ERROR: 'catastrophic error on request service',
    Errors.NO_LOCATOR: 'product locator not found',
    Errors.FILE_ERROR: 'not able to open specified location file',
    Errors.NGAS_ERROR: 'error fetching file from NGAS server',
    Errors.SIZE_MISMATCH: 'retrieved file not expected size'
}


def get_error_descriptions():
    result = 'Return Codes:\n'
    for error in Errors:
        result += '\t{}: {}\n'.format(error.value, TERMINAL_ERRORS[error])
    return result


def terminal_error(errno):
    if errno in TERMINAL_ERRORS:
        LOG.error(TERMINAL_ERRORS[errno])
    else:
        LOG.error('unspecified error {}'.format(errno))

    sys.exit(errno.value)
