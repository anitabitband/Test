# -*- coding: utf-8 -*-

# This is a home for assorted utilities that didn't seem to belong elsewhere,
# stuff like the CAPO settings retriever, command line parser generator and
# so on.


import os
import sys
import argparse
import requests
import json
from enum import Enum

from pycapo import *

from yoink import LOG

# Prologue and epilogue for the command line parser.
_PROLOGUE = \
    """Retrieve a product (a science product or an ancillary product) from the NRAO archive,
either by specifying the product's locator or by providing the path to a product
locator report."""
_EPILOGUE = \
    """This is my epilogue"""

# This is a dictionary of required CAPO settings and the attribute names we'll store them as.
REQUIRED_SETTINGS = {
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.LOCATORSERVICEURLPREFIX': 'locator_service_url',
    'EDU.NRAO.ARCHIVE.DATAFETCHER.DATAFETCHERSETTINGS.EXECUTIONSITE': 'execution_site'
}


class Errors(Enum):
    NO_PROFILE = 1
    MISSING_SETTING = 2
    SERVICE_TIMEOUT = 3
    SERVICE_REDIRECTS = 4
    SERVICE_ERROR = 5
    NO_LOCATOR = 6
    FILE_ERROR = 7


TERMINAL_ERRORS = {
    Errors.NO_PROFILE: 'no CAPO profile provided',
    Errors.MISSING_SETTING: 'missing required setting',
    Errors.SERVICE_TIMEOUT: 'request to locator service timed out',
    Errors.SERVICE_REDIRECTS: 'too many redirects on locator service',
    Errors.SERVICE_ERROR: 'catastrophic error on request service',
    Errors.NO_LOCATOR: 'product locator not found',
    Errors.FILE_ERROR: 'not able to open specified location file'
}


def terminal_error(errno):
    if errno in TERMINAL_ERRORS:
        LOG.error(TERMINAL_ERRORS[errno])
    else:
        LOG.error('unspecified error {}'.format(errno))

    sys.exit(errno)


def get_arg_parser():
    """ Build and return an argument parser with the command line options for yoink; this is
        out here and not in a class because Sphinx needs it to build the docs.

    :return: an argparse 'parser' with command line options for yoink.
    """
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
                                help='dry run, do not down the files')
    optional_group.add_argument('--verbose', action='store_true',
                                required=False, dest='verbose',
                                help='make a lot of noise')
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
        terminal_error(Errors.NO_PROFILE)
    c = CapoConfig(profile=profile)
    for setting in REQUIRED_SETTINGS:
        setting = setting.upper()
        LOG.debug('looking for setting {}'.format(setting))
        try:
            value = c[setting]
        except KeyError:
            LOG.error('missing required setting {}'.format(setting))
            terminal_error(Errors.MISSING_SETTING)
        result[REQUIRED_SETTINGS[setting]] = value
        LOG.debug('required setting {} is {}'.format(REQUIRED_SETTINGS[setting], value))
    LOG.error(str(result))
    return result


def get_location_report(settings, product_locator=None, location_file=None):
    """ Given a product locator or a path to a location file, return a location report,
    an object describing the files that make up the product and where to get them from.
    If neither argument is provided, throw a ValueError, if both are (for some reason)
    then the location file takes precedence.

    :param settings: required CAPO settings for yoink
    :param product_locator:
    :param location_file:
    :return:
    """
    if product_locator is None and location_file is None:
        raise ValueError('product_locator or location_file must be provided, neither were')
    if location_file is not None:
        return _get_location_report_from_file(location_file)
    if product_locator is not None:
        return _get_location_report_from_service(settings, product_locator)


def _get_location_report_from_file(location_file):
    try:
        with open(location_file) as f:
            result = json.load(f)
            return result
    except EnvironmentError:
        LOG.error('problem opening file {}'.format(location_file))
        terminal_error(Errors.FILE_ERROR)


def _get_location_report_from_service(settings, product_locator):
    """ Use 'requests' to fetch the location report from the locator service.

    :param product_locator: the product locator to look up
    :return: the location report (from JSON)
    """
    response = None
    LOG.debug('fetching report from {} for {}'.format(settings['locator_service_url'],
                                                      product_locator))

    try:
        response = requests.get(settings['locator_service_url'],
                                params={'locator': product_locator})
    except requests.exceptions.Timeout:
        terminal_error(Errors.SERVICE_TIMEOUT)
    except requests.exceptions.TooManyRedirects:
        terminal_error(Errors.SERVICE_REDIRECTS)
    except requests.exceptions.RequestException:
        terminal_error(Errors.SERVICE_ERROR)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        LOG.error('can not find locator {}'.format(product_locator))
        terminal_error(Errors.NO_LOCATOR)
    else:
        LOG.error('locator service returned {}'.format(response.status_code))
        terminal_error(Errors.SERVICE_ERROR)
