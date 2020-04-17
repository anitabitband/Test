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
    """ Responsible for getting a file out of NGAS and saving it to
    the requested location. """

    def __init__(self, args):
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run

    def retrieve(self, server, retrieve_method, file_spec):
        """ Retrieve a file described in the file_spec from a given
        NGAS server using a streaming pull or a copy request.

        :param server: the URL of the server to retrieve from
        :param retrieve_method: 'copy' or 'stream', how to retrieve
        :param file_spec: location report for the file to retrieve
        """
        download_url = 'http://' + server + '/RETRIEVE'
        destination = self._get_destination(file_spec)
        self._make_basedir(destination)
        if retrieve_method == 'copy':
            self._copying_fetch(download_url, destination, file_spec)
        elif retrieve_method == 'stream':
            self._streaming_fetch(download_url, destination, file_spec)
        self._check_result(destination, file_spec)

    def _get_destination(self, file_spec):
        """ Build a destination for the file based on the output directory
        the user provided and the (optional) subdirectory and relative
        path in the file_spec.

        :param file_spec: location report for the file to retrieve
        :return: full path to the output file
        """
        if file_spec['subdirectory'] is None:
            return os.path.join(self.output_dir, file_spec['relative_path'])
        return os.path.join(self.output_dir, file_spec['subdirectory'],
                            file_spec['relative_path'])

    def _make_basedir(self, destination):
        """ Creates the directory (if it doesn't exist) the product will
        be saved to.

        TODO: if the directory already exists check if writable?

        :param destination:
        :return:
        """
        if not self.dry_run:
            umask = os.umask(0o000)
            basedir = os.path.dirname(destination)
            Path(basedir).mkdir(parents=True, exist_ok=True)
            os.umask(umask)

    def _check_result(self, destination, file_spec):
        """ Confirm that the file was retrieved and its size matches what
        we expect. If not, throw an error and die.

        :param destination: the path to the file to check
        :param file_spec: the file specification of that file
        """
        self.log.debug('verifying fetch of {}'.format(destination))
        if not self.dry_run:
            if not os.path.exists(destination):
                terminal_error(Errors.NGAS_ERROR)
            if file_spec['size'] != os.path.getsize(destination):
                terminal_error(Errors.SIZE_MISMATCH)

    def _copying_fetch(self, download_url, destination, file_spec):
        """ Pull a file out of NGAS via the direct copy plugin.

        :param download_url: the address to hit
        :param destination:  the path to where to store the result
        :param file_spec:  file specification of the requested file
        :return:
        """
        params = {'file_id': file_spec['ngas_file_id'],
                  'processing': _DIRECT_COPY_PLUGIN,
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
        """ Pull a file out of NGAS via streaming.

        :param download_url: the address to hit
        :param destination:  the path to where to store the result
        :param file_spec:  file specification of the requested file
        :return:
        """
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
