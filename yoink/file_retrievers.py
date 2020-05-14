# -*- coding: utf-8 -*-

"""
Implementations of assorted file retrievers.
"""
import logging
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from yoink.errors import SizeMismatchException, NGASServiceErrorException, \
    FileErrorException, Errors, \
    terminal_exception, MissingSettingsException
from yoink.utilities import RetrievalMode

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'


class NGASFileRetriever:
    """ Responsible for getting a file out of NGAS
        and saving it to the requested location.
    """

    def __init__(self, args):
        logging.basicConfig(level=logging.DEBUG) if args.verbose \
            else logging.basicConfig(level=logging.WARN)
        self._LOG = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run
        self.force_overwrite = args.force
        self.fetch_attempted = False

    def retrieve(self, server, retrieve_method, file_spec):
        """ Retrieve a file described in the file_spec from a given
        NGAS server using a streaming pull or a copy request.

        :param server: the URL of the server to retrieve from
        :param retrieve_method: 'copy' or 'stream', how to retrieve
        :param file_spec: location report for the file to retrieve
        """
        download_url = 'http://' + server + '/RETRIEVE'
        destination = self._get_destination(file_spec)

        if os.path.exists(destination):
            if not self.force_overwrite or self.dry_run:
                exception = FileExistsError(f'{destination} exists; aborting')
                terminal_exception(exception)

        try:
            self._make_basedir(destination)
        except FileErrorException as exc:
            terminal_exception(exc)


        if retrieve_method == RetrievalMode.COPY:
            self._copying_fetch(download_url, destination, file_spec)
        elif retrieve_method == RetrievalMode.STREAM:
            self._streaming_fetch(download_url, destination, file_spec)
        self._check_result(destination, file_spec)

        return destination

    def _get_destination(self, file_spec):
        """ Build a destination for the file based on the output directory
        the user provided and the (optional) subdirectory and relative
        path in the file_spec.

        :param file_spec: location report for the file to retrieve
        :return: full path to the output file
        """

        try:
            if file_spec['subdirectory'] is None:
                return os.path.join(self.output_dir, file_spec['relative_path'])
            return os.path.join(self.output_dir, file_spec['subdirectory'],
                                file_spec['relative_path'])
        except KeyError as k_err:
            exc = MissingSettingsException(k_err)
            terminal_exception(exc)


    def _make_basedir(self, destination):
        """ Creates the directory (if it doesn't exist) the product will
        be saved to.

        :param destination:
        :return:
        """
        if not self.dry_run:
            basedir = os.path.dirname(destination)
            if os.path.isdir(basedir):
                if not os.access(basedir, os.W_OK):
                    terminal_exception(FileErrorException(
                        f'output directory {basedir} is not writable'), )
            try:
                umask = os.umask(0o000)
                Path(basedir).mkdir(parents=True, exist_ok=True)
                os.umask(umask)
            except OSError:
                raise FileErrorException(
                    f'failure trying to create output directory {basedir}')

    def _check_result(self, destination, file_spec):
        """ Confirm that the file was retrieved and its size matches what
        we expect. If not, throw an error and die.

        :param destination: the path to the file to check
        :param file_spec: the file specification of that file
        """
        self._LOG.debug(f'verifying fetch of {destination}')
        if not self.dry_run:
            if not os.path.exists(destination):
                terminal_exception(NGASServiceErrorException(
                    f'file not delivered to {destination}'))
            if file_spec['size'] != os.path.getsize(destination):
                terminal_exception(SizeMismatchException(
                    f"file retrieval size mismatch on {destination}: "
                    f"expected {file_spec['size']}, "
                    f"got {os.path.getsize(destination)}"))
            self._LOG.debug('\tlooks good; sizes match')

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
        self._LOG.debug('attempting copying download:\nurl: {}\ndestination: {}'
                        .format(download_url, destination))
        if not self.dry_run:
            with requests.Session() as session:
                response = session.get(download_url, params=params)

                if response.status_code != requests.codes.ok:
                    self._LOG.error(
                        'NGAS does not like this request:\n{}'
                        .format(response.url))
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    ngams_status = soup.NgamsStatus.Status
                    message = ngams_status.get("Message")

                    raise NGASServiceErrorException(
                        {'status_code': response.status_code,
                         'url': response.url,
                         'reason': response.reason,
                         'message': message})

    def _streaming_fetch(self, download_url, destination, file_spec):
        """ Pull a file out of NGAS via streaming.

        :param download_url: the address to hit
        :param destination:  the path to where to store the result
        :param file_spec:  file specification of the requested file
        :return:
        """
        params = {'file_id': file_spec['ngas_file_id'],
                  'file_version': file_spec['version']}

        self._LOG.debug(
            'attempting streaming download:\nurl: {}\ndestination: {}'
            .format(download_url, destination))
        self.fetch_attempted = True
        if not self.dry_run:
            with requests.Session() as session:
                try:
                    response = session.get(
                        download_url, params=params, stream=True)
                    chunk_size = 8192 # TODO constant
                    with open(destination, 'wb') as file_to_write:
                        for chunk in response.iter_content(
                                chunk_size=chunk_size):
                            file_to_write.write(chunk)
                except requests.exceptions.ConnectionError:
                    terminal_exception(NGASServiceErrorException(
                        f'problem connecting with {download_url}'))

                if response.status_code != requests.codes.ok:
                    self._LOG.error('NGAS does not like this request:\n{}'
                                    .format(response.url))
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    ngams_status = soup.NgamsStatus.Status
                    message = ngams_status.get("Message")

                    n_exc = NGASServiceErrorException(
                        {'status_code': response.status_code,
                         'url': response.url,
                         'reason': response.reason,
                         'message': message})
                    SystemExit(n_exc, Errors.NGAS_SERVICE_ERROR)
                else:
                    self._LOG.info('retrieved {} from {}'.
                                   format(destination, response.url))
