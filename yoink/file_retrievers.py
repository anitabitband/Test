# -*- coding: utf-8 -*-

# Implementations of assorted file retrievers.

import logging

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'


class NGASFileRetriever:
    def __init__(self, args):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run

    def retrieve(self, server, retrieve_method, file_specification):
        if retrieve_method == 'copy':
            self._copying_fetch(server, file_specification)
        elif retrieve_method == 'stream':
            self._streaming_fetch(server, file_specification)

    def _copying_fetch(self, server, file_specification):
        self.log.debug('COPY: {}, {}'.format(server, file_specification))

    def _streaming_fetch(self, server, file_specification):
        self.log.debug('STREAM: {}, {}'.format(server, file_specification))

