# -*- coding: utf-8 -*-

# Implementations of assorted product fetchers

import logging


class SerialProductFetcher:
    def __init__(self, args, settings, servers_report):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_directory = args.output_directory
        self.servers_report = servers_report

    def run(self):
        pass


class ParallelProductFetcher:
    def __init__(self, args, settings, servers_report):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_directory = args.output_directory
        self.servers_report = servers_report

    def run(self):
        pass
