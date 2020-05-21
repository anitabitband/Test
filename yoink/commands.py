#!/usr/bin/env/python
# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import logging

from yoink.errors import terminal_exception, NoLocatorException, \
    NGASServiceErrorException
from yoink.product_fetchers import ParallelProductFetcher
from yoink.utilities import get_arg_parser, get_capo_settings
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

        # TODO Some Fine Day: get verbose-or-not logging to work 4 realz
        logging.basicConfig(level=logging.DEBUG) if args.verbose \
            else logging.basicConfig(level=logging.WARN)
        self._LOG = logging.getLogger(self.__class__.__name__)

        self.args = args
        self.settings = settings
        try:
            self.locations_report = self._get_locations()
            self.servers_report = self.locations_report.servers_report
        except (NoLocatorException, FileNotFoundError, PermissionError) as exc:
            terminal_exception(exc)
        except Exception as exc:
            self._LOG.error(
                f'>>> throwing unexpected exception during init: {exc}')
            terminal_exception(exc)

    def run(self):
        try:
            return ParallelProductFetcher(
                self.args, self.settings, self.servers_report).run()
        except (NGASServiceErrorException, FileExistsError) as exc:
            terminal_exception(exc)
        except Exception as exc:
            self._LOG.error(f'>>> throwing unexpected exception during run: {exc}')
            terminal_exception(exc)

    def _get_locations(self):
        try:
            return LocationsReport(self.args, self.settings)
        except NoLocatorException as exc:
            terminal_exception(exc)

def main():

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
