# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import logging

from yoink.errors import terminal_exception
from yoink.product_fetchers import ParallelProductFetcher
from yoink.utilities import get_arg_parser, get_capo_settings, LocationsReport

class Yoink:
    '''
        example command line that should work with the correct local profile:
        yoink --profile local --output-dir ~/Downloads/ --product-locator uid://evla/execblock/93e1c0cd-76de-4e65-a3f2-d5fe55f386d8

        local.properties must have:
        - edu.nrao.archive.workflow.config.StartupSettings.temporaryDataDirectory pointing to a locally writable temp dir, e.g., /var/tmp
        - edu.nrao.archive.workflow.config.DeliverySettings.hostname must point to local computer
        - execution_site must NOT be DSOC or NAASC

    '''

    def __init__(self, args, settings):

        self._LOG = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.DEBUG) if args.verbose else logging.basicConfig(level=logging.WARN)
        self._LOG.info(f">>> ARGS PASSED IN: {args}")

        self.args = args
        self.settings = settings
        self.locations_report = LocationsReport(args, settings)
        self.servers_report = self.locations_report.servers_report

    def run(self):
        fetcher = ParallelProductFetcher(self.args, self.settings,self.servers_report)
        fetcher.run()


def main():

    try:
        parser = get_arg_parser()
        args = parser.parse_args()
        settings = get_capo_settings(args.profile)
        yoink = Yoink(args, settings)
        yoink.run()
    except Exception as ex:
        terminal_exception(ex)


if __name__ == '__main__':
    main()
