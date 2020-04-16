# -*- coding: utf-8 -*-

# Implementations of assorted product fetchers

import logging

from yoink.file_retrievers import NGASFileRetriever


class BaseProductFetcher:
    def __init__(self, args, settings, servers_report):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run
        self.servers_report = servers_report
        self.ngas_retriever = NGASFileRetriever(args)

    def run(self):
        pass

    def verify_files(self):
        pass

    def verify_file(self):
        pass


class SerialProductFetcher(BaseProductFetcher):
    """ Pull the files out, one right after another, don't try to be
    clever about it. """
    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)

    def run(self):
        self.log.debug('writing to {}'.format(self.output_dir))
        self.log.debug('dry run: {}'.format(self.dry_run))
        for server in self.servers_report:
            retrieve_method = self.servers_report[server]['retrieve_method']
            for f in self.servers_report[server]['files']:
                self.ngas_retriever.retrieve(server, retrieve_method, f)


class ParallelProductFetcher(BaseProductFetcher):
    """ Pull the files out in parallel, try to be clever about it. Likely
    fail in the attempt, but do try to be clever. """
    def __init__(self, args, settings, servers_report):
        super().__init__(args, settings, servers_report)

    def run(self):
        self.log.debug('writing to {}'.format(self.output_dir))
        self.log.debug('dry run: {}'.format(self.dry_run))
