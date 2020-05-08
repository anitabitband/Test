# -*- coding: utf-8 -*-

# This is a home for assorted utilities that didn't seem to belong elsewhere,
# stuff like the CAPO settings retriever, command line parser generator and
# so on.


import argparse
import copy
import json
import logging
import os
import pathlib
import time
from typing import List

import psycopg2 as pg
import requests
from pycapo import *

from yoink.errors import get_error_descriptions, NoProfileException, MissingSettingsException, \
    FileErrorException, LocationServiceErrorException, LocationServiceRedirectsException, \
    LocationServiceTimeoutException, NoLocatorException

LOG_FORMAT = "%(name)s.%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

# Prologue and epilogue for the command line parser.
_PROLOGUE = \
    """Retrieve a product (a science product or an ancillary product) from the NRAO archive,
either by specifying the product's locator or by providing the path to a product
locator report."""
_EPILOGUE = get_error_descriptions()

# This is a dictionary of required CAPO settings and the attribute names we'll store them as.
REQUIRED_SETTINGS = {
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.LOCATORSERVICEURLPREFIX': 'locator_service_url',
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.EXECUTIONSITE': 'execution_site',
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.DEFAULTTHREADSPERHOST': 'threads_per_host'
}

def configure_logging():
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

def path_is_accessible(path):
    can_access = os.access(path, os.F_OK)
    can_access = can_access and os.path.isdir(path)
    can_access = can_access and os.access(path, os.R_OK)
    can_access = can_access and os.access(path, os.W_OK)
    can_access = can_access and os.access(path, os.X_OK)
    return can_access


def get_arg_parser():
    """ Build and return an argument parser with the command line options for yoink; this is
        out here and not in a class because Sphinx needs it to build the docs.

    :return: an argparse 'parser' with command line options for yoink.
    """
    cwd = pathlib.Path().absolute()
    parser = argparse.ArgumentParser(description=_PROLOGUE, epilog=_EPILOGUE,
                                     formatter_class=argparse.RawTextHelpFormatter)
    # Can't find a way of clearing the action groups without hitting an internal attribute.
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
                                help='output directory, default current directory')
    optional_group.add_argument('--sdm-only', action='store_true', dest='sdm_only',
                                help='only get the metadata, not the fringes')
    optional_group.add_argument('--verbose', action='store_true',
                                required=False, dest='verbose', default=False,
                                help='make a lot of noise')
    optional_group.add_argument('--force', action='store_true',
                                required=False, dest='force', default=False,
                                help='overwrite existing file(s) at destination')
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
    """ Get the required CAPO settings for yoink for the provided profile (prod, test).
    Spits out an error message and exits (1) if it can't find one of them.

    :param profile: the profile to use
    :return: a bunch of settings
    """
    result = dict()
    if profile is None:
        raise NoProfileException('CAPO_PROFILE required, none provided')
    c = CapoConfig(profile=profile)
    for setting in REQUIRED_SETTINGS:
        value = None
        setting = setting.upper()
        _LOG.debug('looking for setting {}'.format(setting))
        try:
            value = c[setting]
        except KeyError:
            raise MissingSettingsException('missing required setting "{}"'
                                           .format(setting))
        result[REQUIRED_SETTINGS[setting]] = value
        _LOG.debug('required setting {} is {}'.format(REQUIRED_SETTINGS[setting], value))
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
    c = CapoConfig(profile=profile)
    fields = ['jdbcDriver', 'jdbcUrl', 'jdbcUsername', 'jdbcPassword']
    qualified_fields = ['metadataDatabase.' + field for field in fields]
    for field in qualified_fields:
        _LOG.debug(f'looking for {field}....')
        # field = 'metadataDatabase.' + field
        try:
            value = c[field]
            result[field] = value
        except KeyError:
            raise MissingSettingsException(f'missing required setting "{field}"')
    return result


class LocationsReport:
    """ A locations report is produced by the archive service, you give
    it a product locator and it returns a dictionary of details on how
    to retrieve the product's files from long term storage (NGAS): this
    class handles fetching the report from the service or reading it from
    a file, and has utilities to manipulate the report. """

    # TODO: it would be wide to build some validation into the class
    #   so if we get passed an improper file (either by service or by
    #   path) we complain loudly and fall over.

    def __init__(self, args, settings):
        self.log = logging.getLogger(self.__class__.__name__)
        self.settings = settings
        self.product_locator = args.product_locator
        self.location_file = args.location_file
        self.sdm_only = args.sdm_only
        self.files_report = self._get_files_report()
        self.servers_report = self._get_servers_report()

    def _add_retrieve_method_field(self, files_report):
        """ This adds a field to the files report about whether we can do
        a direct copy or we have to rely on streaming: this is something
        the location service itself doesn't know because it depends on
        which site yoink is running on, which site has the data and whether
        the NGAS cluster supports direct copy. """
        for f in files_report['files']:
            if f['server']['cluster'] == 'DSOC' and \
                    f['server']['location'] == self.settings['execution_site']:
                f['server']['retrieve_method'] = 'copy'
            else:
                f['server']['retrieve_method'] = 'stream'
        return files_report

    def _filter_sdm_only(self, files_report):
        """ The user wants only the SDM tables, not the BDFs, so filter
        everything else out. Note, if they specify SDM-Only and the product
        is not a EVLA execution block things will go badly for them. """
        if self.sdm_only:
            result = list()
            for f in files_report['files']:
                relative_path = f['relative_path']
                if relative_path.endswith('.bin') or \
                        relative_path.endswith('.xml'):
                    result.append(f)
            files_report['files'] = result
        return files_report

    def _get_servers_report(self):
        """ The location report we get back looks like this, for each file:

        {"ngas_file_id":"17B-197_2018_02_19_T15_59_16.097.tar",
        "subdirectory":"17B-197.sb34812522.eb35115211.58168.58572621528",
        "relative_path":"17B-197_2018_02_19_T15_59_16.097.tar",
        "checksum":"-1848259250",
        "checksum_type":"ngamsGenCrc32",
        "version":1,
        "size":108677120,
        "server":{"server":"nmngas01.aoc.nrao.edu:7777",
            "location":"DSOC",
            "cluster":"DSOC"
        }}

        Re-organize it to group files under servers so it is more useful.
        """
        result = dict()
        for f in self.files_report['files']:
            new_f = copy.deepcopy(f)
            del new_f['server']
            server = f['server']
            server_host = server['server']
            if server_host not in result:
                result[server_host] = dict()
                result[server_host]['location'] = server['location']
                result[server_host]['cluster'] = server['cluster']
                result[server_host]['retrieve_method'] = server['retrieve_method']
                result[server_host]['files'] = list()
            result[server_host]['files'].append(new_f)
        return result

    def _get_files_report(self):
        """ Given a product locator or a path to a location file, return a location report,
        an object describing the files that make up the product and where to get them from.
        If neither argument is provided, throw a ValueError, if both are (for some reason)
        then the location file takes precedence.

        :return: location report (from file, in JSON)
        """
        result = dict()
        if self.product_locator is None and self.location_file is None:
            raise ValueError('product_locator or location_file must be provided, neither were')
        if self.location_file is not None:
            result = self._get_location_report_from_file()
        if self.product_locator is not None:
            result = self._get_location_report_from_service()
        result = self._filter_sdm_only(result)
        return self._add_retrieve_method_field(result)

    def _get_location_report_from_file(self):
        """ Read a file at a user provided path to pull in the location report. """
        try:
            with open(self.location_file) as f:
                result = json.load(f)
                return result
        except EnvironmentError as ex:
            # This broadly catches any exception with opening and reading the
            # file, but it might not catch exceptions converting to JSON.
            #
            # TODO: look into that and add other clauses.
            raise FileErrorException('can not read provided file {}'
                                     .format(self.location_file))

    def _get_location_report_from_service(self):
        """ Use 'requests' to fetch the location report from the locator service.

        :return: location report (from locator service, in JSON)
        """
        response = None
        self.log.debug('fetching report from {} for {}'.format(self.settings['locator_service_url'],
                                                               self.product_locator))

        try:
            response = requests.get(self.settings['locator_service_url'],
                                    params={'locator': self.product_locator})
        except requests.exceptions.Timeout:
            raise LocationServiceTimeoutException()
        except requests.exceptions.TooManyRedirects:
            raise LocationServiceRedirectsException()
        except requests.exceptions.RequestException as ex:
            raise LocationServiceErrorException(ex)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise NoLocatorException('can not find locator {}'.
                                     format(self.product_locator))
        else:
            raise LocationServiceErrorException('locator service returned {}'
                                                .format(response.status_code))

class ProductLocatorLookup:
    """ Look up the product locator for an external name (fileset ID)

    """

    def __init__(self, capo_db_settings):
        self.capo_db_settings = capo_db_settings
        self.credentials = {}
        for key, value in self.capo_db_settings.items():
            self.credentials[key.replace('metadataDatabase.', '')] = value

    def look_up_locator_for_ext_name(self, external_name):
        self.host, self.dbname = self.credentials['jdbcUrl'].split(':')[2][2:].split('/')

        with pg.connect(dbname=self.dbname,
                        host=self.host,
                        user=self.credentials['jdbcUsername'],
                        password=self.credentials['jdbcPassword']) as conn:
            cursor = conn.cursor()
            sql = 'SELECT science_product_locator FROM science_products WHERE external_name=%s'
            cursor.execute(sql, (external_name,), )
            product_locator = cursor.fetchone()
        return product_locator[0]


class Retryer:

    def __init__(self, func, max_tries, sleep_interval):
        self.func = func
        self.max_tries = max_tries
        self.complete = False
        self.sleep_interval = sleep_interval

    def retry(self, args: List):
        num_tries = 0
        while num_tries < self.max_tries and not self.complete:

            num_tries += 1
            _LOG.info(f'trying {self.func.__name__} with argument(s) "{args}"....')

            try:
                success = self.func(args)
                if success:
                    self.complete = True
            except SystemExit as exc:
                if num_tries < self.max_tries:
                    _LOG.info(f'{exc}; trying again after {self.sleep_interval} seconds....')
                    time.sleep(self.sleep_interval)
                    return num_tries
                else:
                    _LOG.error(f'FAILURE after {num_tries} attempts')
                    raise exc

        return num_tries