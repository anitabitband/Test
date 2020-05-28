# -*- coding: utf-8 -*-

"""This is a home for assorted utilities that didn't seem to belong elsewhere,
    stuff like the CAPO settings retriever, command line parser generator and
    so on.
"""

import argparse
import logging
import os
import pathlib
import time
from enum import Enum

import psycopg2 as pg
from pycapo import CapoConfig

from yoink.errors import get_error_descriptions, NoProfileException, \
    MissingSettingsException, NGASServiceErrorException, SizeMismatchException

LOG_FORMAT = "%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

MAX_TRIES = 10
SLEEP_INTERVAL_SECONDS = 1
FILE_SPEC_KEYS = ['ngas_file_id', 'subdirectory', 'relative_path',
                  'checksum', 'checksum_type', 'version', 'size', 'server']
SERVER_SPEC_KEYS = ['server', 'location', 'cluster', 'retrieve_method']

# Prologue and epilogue for the command line parser.
_PROLOGUE = \
    """Retrieve a product (a science product or an ancillary product) 
from the NRAO archive,
either by specifying the product's locator or by providing the path to a product
locator report."""
_EPILOGUE = get_error_descriptions()

# This is a dictionary of required CAPO settings and the attribute names we'll store them as.
REQUIRED_SETTINGS = {
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.LOCATORSERVICEURLPREFIX':
        'locator_service_url',
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.EXECUTIONSITE':
        'execution_site',
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.DEFAULTTHREADSPERHOST':
        'threads_per_host'
}

def path_is_accessible(path):
    ''' Is this path readable, executable, and writable?
    '''
    can_access = os.access(path, os.F_OK)
    can_access = can_access and os.path.isdir(path)
    can_access = can_access and os.access(path, os.R_OK)
    can_access = can_access and os.access(path, os.W_OK)
    can_access = can_access and os.access(path, os.X_OK)
    return can_access

# TODO: parser should be a class; write unit tests
def get_arg_parser():
    """ Build and return an argument parser with the command line options
        for yoink; this is out here and not in a class because Sphinx needs it
        to build the docs.

    :return: an argparse 'parser' with command line options for yoink.
    """
    cwd = pathlib.Path().absolute()
    parser = argparse.ArgumentParser(description=_PROLOGUE, epilog=_EPILOGUE,
                                     formatter_class=
                                     argparse.RawTextHelpFormatter)
    # Can't find a way of clearing the action groups
    # without hitting an internal attribute.
    parser._action_groups.pop()
    exclusive_group = parser.add_mutually_exclusive_group(required=True)
    exclusive_group.add_argument('--product-locator', action='store',
                                 dest='product_locator',
                                 help='product locator to download')
    exclusive_group.add_argument('--location-file', action='store',
                                 dest='location_file',
                                 help='product locator report (in JSON)')
    optional_group = parser.add_argument_group('Optional Arguments')
    optional_group.add_argument('--dry-run', action='store_true',
                                dest='dry_run', default=False,
                                help='dry run, do not fetch product')
    optional_group.add_argument('--output-dir', action='store',
                                dest='output_dir', default=cwd,
                                help='output directory, default current dir')
    optional_group.add_argument('--sdm-only', action='store_true',
                                dest='sdm_only',
                                help='only get the metadata, not the fringes')
    optional_group.add_argument('--verbose', action='store_true',
                                required=False, dest='verbose', default=False,
                                help='make a lot of noise')
    optional_group.add_argument('--force', action='store_true',
                                required=False, dest='force', default=False,
                                help='overwrite existing file(s) at dest')
    if 'CAPO_PROFILE' in os.environ:
        optional_group.add_argument('--profile', action='store', dest='profile',
                                    help='CAPO profile to use, default \''
                                    + os.environ['CAPO_PROFILE'] + '\'',
                                    default=os.environ['CAPO_PROFILE'])
    else:
        optional_group.add_argument('--profile', action='store', dest='profile',
                                    help='CAPO profile to use')
    return parser


def get_capo_settings(profile):
    """ Get the required CAPO settings for yoink for the provided profile
    (prod, test). Spits out an error message and exits (1) if it can't find
    one of them.

    :param profile: the profile to use
    :return: a bunch of settings
    """
    result = dict()
    if profile is None:
        raise NoProfileException('CAPO_PROFILE required, none provided')
    capo = CapoConfig(profile=profile)
    for setting in REQUIRED_SETTINGS:
        value = None
        setting = setting.upper()
        _LOG.debug('looking for setting {}'.format(setting))
        try:
            value = capo[setting]
        except KeyError:
            raise MissingSettingsException('missing required setting "{}"'
                                           .format(setting))
        result[REQUIRED_SETTINGS[setting]] = value
    _LOG.debug('CAPO settings: {}'.format(str(result)))
    return result

def get_metadata_db_settings(profile):
    """ Get Capo settings needed to connect to archive DB
    :param profile:
    :return:
    """
    result = dict()
    if profile is None:
        raise NoProfileException('CAPO_PROFILE required, none provided')
    config = CapoConfig(profile=profile)
    fields = ['jdbcDriver', 'jdbcUrl', 'jdbcUsername', 'jdbcPassword']
    qualified_fields = ['metadataDatabase.' + field for field in fields]
    for field in qualified_fields:
        _LOG.debug(f'looking for {field}....')
        try:
            value = config[field]
            result[field] = value
        except KeyError:
            raise MissingSettingsException(
                f'missing required setting "{field}"')
    return result

def validate_file_spec(file_spec: dict, retrieve_method_expected: bool):
    '''
    Make sure this nugget of file info contains everything it should.
    :param file_spec:
    :return:
    '''
    for key in FILE_SPEC_KEYS:
        if key not in file_spec:
            raise MissingSettingsException(f'{key} not found in file_spec')

    server = file_spec['server']

    for key in SERVER_SPEC_KEYS:
        if not key in server.keys():
            # if this is before retrieval mode has been set, OK not to have it
            if retrieve_method_expected:
                raise MissingSettingsException(
                    f'{key} not found in server spec: {server}')


class ProductLocatorLookup:

    """
    Look up the product locator for an external name (fileset ID)

    """
    def __init__(self, capo_db_settings):
        self.capo_db_settings = capo_db_settings
        self.credentials = {}
        for key, value in self.capo_db_settings.items():
            self.credentials[key.replace('metadataDatabase.', '')] = value

    def look_up_locator_for_ext_name(self, external_name):
        '''
        Given a fileset ID or analogous identifier, find its product locator

        :param external_name:
        :return:
        '''
        host, dbname = self.credentials['jdbcUrl'].split(':')[2][2:].split('/')

        with pg.connect(dbname=dbname,
                        host=host,
                        user=self.credentials['jdbcUsername'],
                        password=self.credentials['jdbcPassword']) as conn:
            cursor = conn.cursor()
            sql = 'SELECT science_product_locator ' \
                  'FROM science_products ' \
                  'WHERE external_name=%s'
            cursor.execute(sql, (external_name,), )
            product_locator = cursor.fetchone()
        return product_locator[0]


class Retryer:
    """
    Retry executing a function, or die trying
    """

    def __init__(self, func, max_tries, sleep_interval):
        self.func = func
        self.num_tries = 0
        self.max_tries = max_tries
        self.sleep_interval = sleep_interval
        self.complete = False

    def retry(self, *args):
        '''
        Try something a specified number of times.
        Die if it doesn't work after N tries.
        :param args:
        :return:
        '''

        while self.num_tries < self.max_tries and not self.complete:

            _LOG.info(f'trying {self.func.__name__}({args})....')
            self.num_tries += 1
            exc = None
            try:
                success = self.func(args)
                if success:
                    self.complete = True
                else:
                    if self.num_tries < self.max_tries:
                        _LOG.info('iteration #{}: {}; trying again after {} '
                                  'seconds....'
                                  .format(self.num_tries, exc,
                                          self.sleep_interval))
                        time.sleep(self.sleep_interval)
                    else:
                        raise NGASServiceErrorException(
                            'FAILURE after {} attempts'.format(
                                self.num_tries))

            except (NGASServiceErrorException, SizeMismatchException) as exc:
                if self.num_tries < self.max_tries:
                    _LOG.info('{}; trying again after {} seconds....'
                              .format(exc, self.sleep_interval))
                    time.sleep(self.sleep_interval)
                else:
                    _LOG.error(
                        'FAILURE after {} attempts'.format(self.num_tries))
                    raise exc


class Location(Enum):
    """
    Where the files live
    """
    DSOC  = 'DSOC'
    NAASC = 'NAASC'

    def __str__(self):
        return str(self.value)


class Cluster(Enum):
    """
    Which cluster the files are on
    """
    DSOC  = 'DSOC'
    NAASC = 'NAASC'

    def __str__(self):
        return str(self.value)


class ExecutionSite(Enum):
    """
    Where this code is executing
    """
    DSOC  = 'DSOC'
    NAASC = 'NAASC'

    def __str__(self):
        return str(self.value)


class RetrievalMode(Enum):
    """
        How we're retrieving a file: via streaming or via
        direct copy (plugin)
    """
    STREAM = 'stream'
    COPY   = 'copy'
