# -*- coding: utf-8 -*-

# Implementations of assorted file retrievers.

import logging
import requests
import os
import sys
from pathlib import Path

from yoink.errors import Errors, terminal_error

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'


class NGASFileRetriever:
    def __init__(self, args):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run

    def retrieve(self, server, retrieve_method, file_spec):
        download_url = 'http://' + server + '/RETRIEVE'
        destination = self._get_destination(file_spec)
        self._make_basedir(destination)
        if retrieve_method == 'copy':
            self._copying_fetch(download_url, destination, file_spec)
        elif retrieve_method == 'stream':
            self._streaming_fetch(download_url, destination, file_spec)
        self._check_result(destination, file_spec)

    def _get_destination(self, file_spec):
        if file_spec['subdirectory'] is None:
            return os.path.join(self.output_dir, file_spec['relative_path'])
        return os.path.join(self.output_dir, file_spec['subdirectory'],
                            file_spec['relative_path'])

    def _make_basedir(self, destination):
        if not self.dry_run:
            umask = os.umask(0o000)
            basedir = os.path.dirname(destination)
            Path(basedir).mkdir(parents=True, exist_ok=True)
            os.umask(umask)

    def _check_result(self, destination, file_spec):
        self.log.debug('verifying fetch of {}'.format(destination))
        if not self.dry_run:
            if not os.path.exists(destination):
                terminal_error(Errors.NGAS_ERROR)
            if file_spec['size'] != os.path.getsize(destination):
                terminal_error(Errors.SIZE_MISMATCH)

    def _copying_fetch(self, download_url, destination, file_spec):
        params = {'file_id': file_spec['ngas_file_id'],
                  'processing': 'ngamsDirectCopyDppi',
                  'processingPars': 'outfile=' + destination,
                  'file_version': file_spec['version']}
        self.log.debug('attempting copying download:\nurl: {}\ndestination: {}'
                       .format(download_url, destination))
        if not self.dry_run:
            with requests.Session() as s:
                r = s.get(download_url, params=params)

                if r.status_code != requests.codes.ok:
                    self.log.error('bad status code {}'.format(r.status_code))
                    terminal_error(Errors.NGAS_ERROR)

    def _streaming_fetch(self, download_url, destination, file_spec):
        params = {'file_id': file_spec['ngas_file_id'],
                  'file_version': file_spec['version']}

        self.log.debug('attempting streaming download:\nurl: {}\ndestination: {}'
                       .format(download_url, destination))
        if not self.dry_run:
            with requests.Session() as s:
                r = s.get(download_url, params=params, stream=True)
                chunk_size = 8192;
                with open(destination, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        f.write(chunk)

                if r.status_code != requests.codes.ok:
                    self.log.error('bad status code {}'.format(r.status_code))
                    terminal_error(Errors.NGAS_ERROR)
