# -*- coding: utf-8 -*-

# Assorted constants and functions involving errors goes here.

import logging
import sys
import traceback
from enum import Enum

LOG = logging.getLogger(__name__)


class Errors(Enum):
    NO_PROFILE = 1
    MISSING_SETTING = 2
    LOCATION_SERVICE_TIMEOUT = 3
    LOCATION_SERVICE_REDIRECTS = 4
    LOCATION_SERVICE_ERROR = 5
    NO_LOCATOR = 6
    FILE_ERROR = 7
    NGAS_SERVICE_TIMEOUT = 8
    NGAS_SERVICE_REDIRECTS = 9
    NGAS_SERVICE_ERROR = 10
    SIZE_MISMATCH = 11


class NoProfileException(Exception):
    pass


class MissingSettingsException(Exception):
    pass


class LocationServiceTimeoutException(Exception):
    pass


class LocationServiceRedirectsException(Exception):
    pass


class LocationServiceErrorException(Exception):
    pass


class NGASServiceTimeoutException(Exception):
    pass


class NGASServiceRedirectsException(Exception):
    pass


class NGASServiceErrorException(Exception):
    pass


class NoLocatorException(Exception):
    pass


class FileErrorException(Exception):
    pass


class SizeMismatchException(Exception):
    pass


TERMINAL_ERRORS = {
    Errors.NO_PROFILE: 'no CAPO profile provided',
    Errors.MISSING_SETTING: 'missing required setting',
    Errors.LOCATION_SERVICE_TIMEOUT: 'request to locator service timed out',
    Errors.LOCATION_SERVICE_REDIRECTS: 'too many redirects on locator service',
    Errors.LOCATION_SERVICE_ERROR: 'catastrophic error on locator service',
    Errors.NO_LOCATOR: 'product locator not found',
    Errors.FILE_ERROR: 'not able to open specified location file',
    Errors.NGAS_SERVICE_TIMEOUT: 'request to NGAS timed out',
    Errors.NGAS_SERVICE_REDIRECTS: 'too many redirects on NGAS service',
    Errors.NGAS_SERVICE_ERROR: 'catastrophic error on NGAS service',
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


def exception_to_error(exception):
    switcher = {
        'NoProfileException': Errors.NO_PROFILE,
        'MissingSettingsException': Errors.MISSING_SETTING,
        'LocationServiceTimeoutException': Errors.LOCATION_SERVICE_TIMEOUT,
        'LocationServiceRedirectsException': Errors.LOCATION_SERVICE_REDIRECTS,
        'LocationServiceErrorException': Errors.LOCATION_SERVICE_ERROR,
        'NoLocatorException': Errors.NO_LOCATOR,
        'FileErrorException': Errors.FILE_ERROR,
        'NGASServiceTimeoutException': Errors.NGAS_SERVICE_TIMEOUT,
        'NGASServiceRedirectsException': Errors.NGAS_SERVICE_REDIRECTS,
        'NGASServiceErrorException': Errors.NGAS_SERVICE_ERROR,
        'SizeMismatchException': Errors.SIZE_MISMATCH
    }
    return switcher.get(exception.__class__.__name__)


def terminal_exception(exception):
    errorno = exception_to_error(exception)
    LOG.debug(traceback.format_exc())
    LOG.error(str(exception))
    sys.exit(errorno.value)
