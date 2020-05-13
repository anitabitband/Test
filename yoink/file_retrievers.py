# -*- coding: utf-8 -*-

# Implementations of assorted file retrievers.

import logging
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from yoink.errors import SizeMismatchException, NGASServiceErrorException, FileErrorException, Errors, \
    terminal_exception, MissingSettingsException
from yoink.utilities import RetrievalMode

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'


class NGASFileRetriever:
    """ Responsible for getting a file out of NGAS and saving it to
    the requested location. """

    def __init__(self, args):
        self._LOG = logging.getLogger(self.__class__.__name__)
        self.output_dir = args.output_dir
        self.dry_run = args.dry_run
        self.force_overwrite = args.force

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
        except Exception as exc:
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
                    terminal_exception(FileErrorException(f'output directory {basedir} is not writable'), )
            try:
                umask = os.umask(0o000)
                Path(basedir).mkdir(parents=True, exist_ok=True)
                os.umask(umask)
            except OSError:
                raise FileErrorException(f'failure trying to create output directory {basedir}')

    def _check_result(self, destination, file_spec):
        """ Confirm that the file was retrieved and its size matches what
        we expect. If not, throw an error and die.

        :param destination: the path to the file to check
        :param file_spec: the file specification of that file
        """
        self._LOG.debug(f'verifying fetch of {destination}')
        if not self.dry_run:
            if not os.path.exists(destination):
                terminal_exception(NGASServiceErrorException(f'file not delivered to {destination}'))
            if file_spec['size'] != os.path.getsize(destination):
                terminal_exception(SizeMismatchException(
                    f"file retrieval size mismatch on {destination}: "
                    f"expected {file_spec['size']}, got {os.path.getsize(destination)}"))
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
            with requests.Session() as s:
                r = s.get(download_url, params=params)

                if r.status_code != requests.codes.ok:
                    self._LOG.error(f'NGAS does not like this request:\n{r.url}')
                    soup = BeautifulSoup(r.text, 'lxml-xml')
                    ngams_status = soup.NgamsStatus.Status
                    message = ngams_status.get("Message")

                    raise NGASServiceErrorException(
                        {'status_code': r.status_code, 'url': r.url, 'reason': r.reason, 'message': message})

                else:
                    self._LOG.info(f'retrieved {destination} from {r.url}')

    def _streaming_fetch(self, download_url, destination, file_spec):
        """ Pull a file out of NGAS via streaming.

        :param download_url: the address to hit
        :param destination:  the path to where to store the result
        :param file_spec:  file specification of the requested file
        :return:
        """
        params = {'file_id': file_spec['ngas_file_id'],
                  'file_version': file_spec['version']}

        self._LOG.debug(f'attempting streaming download:\nurl: {download_url}\ndestination: {destination}')
        self.fetch_attempted = True
        if not self.dry_run:
            with requests.Session() as s:
                try:
                    r = s.get(download_url, params=params, stream=True)
                    chunk_size = 8192
                    with open(destination, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                except requests.exceptions.ConnectionError:
                    terminal_exception(NGASServiceErrorException(f'problem connecting with {download_url}'))

                if r.status_code != requests.codes.ok:
                    self._LOG.error(f'NGAS does not like this request:\n{r.url}')
                    soup = BeautifulSoup(r.text, 'lxml-xml')
                    ngams_status = soup.NgamsStatus.Status
                    message = ngams_status.get("Message")

                    n_exc = NGASServiceErrorException(
                        {'status_code': r.status_code, 'url': r.url, 'reason': r.reason, 'message': message})
                    SystemExit(n_exc, Errors.NGAS_SERVICE_ERROR)
                else:
                    self._LOG.info(f'retrieved {destination} from {r.url}')

