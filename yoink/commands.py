#!/usr/bin/env/python
# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import os, logging
import sys
import traceback
from time import time

from yoink.errors import NoLocatorException, \
    NGASServiceErrorException, exception_to_error, terminal_exception
from yoink.product_fetchers import ParallelProductFetcher
from yoink.utilities import get_arg_parser, get_capo_settings, LOG_FORMAT
from yoink.locations_report import LocationsReport


class Yoink:
    '''
        example command line that should work with the correct local profile:
        yoink --profile local --output-dir ~/Downloads/ --product-locator \
            uid://evla/execblock/93e1c0cd-76de-4e65-a3f2-d5fe55f386d8 \
            --sdm-only --verbose

        local.properties must have:
        - edu.nrao.archive.workflow.config
            .StartupSettings.temporaryDataDirectory pointing to a locally
            writable temp dir, e.g., /var/tmp
        - edu.nrao.archive.workflow.config.DeliverySettings.hostname must point
            to local computer
        - execution_site must NOT be DSOC or NAASC

    '''

    def __init__(self, args, settings):
        self.args = args
        self.settings = settings
        self.configure_logging()
        try:
            self.locations_report = self._get_locations()
            self.servers_report = self.locations_report.servers_report
        except (NoLocatorException, FileNotFoundError, PermissionError) as exc:
            self._terminal_exception(exc)
        except TypeError as exc:
            self._LOG.error(f'TODO: handle TypeError')
            self._terminal_exception(exc)
        except Exception as exc:
            self._LOG.error(
                f'>>> throwing unexpected {type(exc)} during init: {exc}')
            self._terminal_exception(exc)

    def configure_logging(self):
        ''' set up logging
        '''
        self.output_dir = self.args.output_dir
        log_pathname = f'{self.__class__.__name__}_{str(time())}.log'
        try:
            self.logfile = os.path.join(self.output_dir, log_pathname)
            self._LOG = logging.getLogger(self.logfile)
            self.handler = logging.FileHandler(self.logfile)
            formatter = logging.Formatter(LOG_FORMAT)
            self.handler.setFormatter(formatter)
            self._LOG.addHandler(self.handler)
        except (PermissionError, FileNotFoundError) as err:
            self._terminal_exception(err)

        level = logging.DEBUG if self.args.verbose else logging.WARN
        self._LOG.setLevel(level)

    def run(self):
        try:
            return ParallelProductFetcher(
                self.args, self.settings, self.logfile,
                self.servers_report).run()
        except (NGASServiceErrorException, FileExistsError) as exc:
            self._terminal_exception(exc)
        except Exception as exc:
            self._LOG.error(f'>>> throwing unexpected exception during run: {exc}')
            self._terminal_exception(exc)

    def _get_locations(self):
        try:
            return LocationsReport(self.logfile, self.args, self.settings)
        except NoLocatorException as exc:
            self._terminal_exception(exc)

    def _terminal_exception(self, exception: Exception):
        ''' report exception, then throw in the towel
        '''
        errorno = exception_to_error(exception)
        self._LOG.debug(traceback.format_exc())
        self._LOG.error(f'{type(exception)}: {str(exception)}')
        sys.exit(errorno.value)


def main():
    ''' this will be executed when yoink is launched
        from the command line
    '''
    try:
        parser = get_arg_parser()
        args = parser.parse_args()
        settings = get_capo_settings(args.profile)
        yoink = Yoink(args, settings)
        yoink.run()
    except (NGASServiceErrorException, FileExistsError) as ex:
        terminal_exception(ex)
    except Exception as ex:
        logging.error(f'>>> some other kind of exception during main: {ex}')
        terminal_exception(ex)


if __name__ == '__main__':
    main()
