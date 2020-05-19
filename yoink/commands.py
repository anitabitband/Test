#!/usr/bin/env/python
# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import logging

from yoink.errors import terminal_exception, NoLocatorException, \
    NGASServiceErrorException
from yoink.product_fetchers import ParallelProductFetcher
from yoink.utilities import get_arg_parser, get_capo_settings, LocationsReport

class Yoink:
    '''
        example command line that should work with the correct local profile:
        yoink --profile local --output-dir ~/Downloads/ --product-locator \
            uid://evla/execblock/93e1c0cd-76de-4e65-a3f2-d5fe55f386d8

        local.properties must have:
        - edu.nrao.archive.workflow.config
            .StartupSettings.temporaryDataDirectory pointing to a locally
            writable temp dir, e.g., /var/tmp
        - edu.nrao.archive.workflow.config.DeliverySettings.hostname must point
            to local computer
        - execution_site must NOT be DSOC or NAASC

    '''

    def __init__(self, args, settings):

        self._LOG = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.DEBUG) if args.verbose \
            else logging.basicConfig(level=logging.WARN)

        self.args = args
        self.settings = settings
        self.locations_report = self._get_locations()
        self.servers_report = self.locations_report.servers_report

    def run(self):
        try:
            return ParallelProductFetcher(
                self.args, self.settings, self.servers_report).run()
        except (NGASServiceErrorException, FileExistsError) as exc:
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


if __name__ == '__main__':
    main()
