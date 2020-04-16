# -*- coding: utf-8 -*-

""" Module for the command line interface to yoink. """

import logging
import pprint

from yoink import LOG
from yoink.utilities import get_arg_parser, get_capo_settings, LocationReport

_DIRECT_COPY_PLUGIN_DEFAULT = 'ngamsDirectCopyDppi'


def main():
    parser = get_arg_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG) if args.verbose else \
        logging.basicConfig(level=logging.WARN)
    settings = get_capo_settings(args.profile)
    report = LocationReport(settings=settings,
                            product_locator=args.product_locator,
                            location_file=args.location_file)
    pp = pprint.PrettyPrinter(indent=4)
    LOG.error(pp.pformat(report.servers_report))


if __name__ == '__main__':
    main()
