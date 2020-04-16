# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import logging
import pprint

from yoink.utilities import get_arg_parser, get_capo_settings, LocationReport
from yoink.product_fetchers import SerialProductFetcher

LOG = logging.getLogger(__name__)


class Yoink:
    def __init__(self, args, settings):
        self.log = logging.getLogger(self.__class__.__name__)
        self.args = args
        self.settings = settings
        self.location_report = LocationReport(settings=self.settings,
                                              product_locator=self.args.product_locator,
                                              location_file=self.args.location_file)
        self.servers_report = self.location_report.servers_report

    def run(self):
        fetcher = SerialProductFetcher(self.args, self.settings,
                                       self.servers_report)
        fetcher.run()


def main():
    parser = get_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG) if args.verbose else \
        logging.basicConfig(level=logging.WARN)
    settings = get_capo_settings(args.profile)

    yoink = Yoink(args, settings)
    yoink.run()


if __name__ == '__main__':
    main()
