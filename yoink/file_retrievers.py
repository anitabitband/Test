# -*- coding: utf-8 -*-

"""
Implementations of assorted file retrievers.
"""
import http
import os
from argparse import Namespace
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from yoink.errors import SizeMismatchException, NGASServiceErrorException, \
    FileErrorException, MissingSettingsException
from yoink.utilities import RetrievalMode, Retryer, MAX_TRIES, \
    SLEEP_INTERVAL_SECONDS, FlexLogger

_DIRECT_COPY_PLUGIN = 'ngamsDirectCopyDppi'
_STREAMING_CHUNK_SIZE = 8192

class NGASFileRetriever:
    """ Responsible for getting a file out of NGAS
        and saving it to the requested location.
    """

    def __init__(self, args: Namespace, logger: FlexLogger):
        self.output_dir = args.output_dir
        self._LOG = logger
        self.logfile = self._LOG.logfile
        self.dry_run = args.dry_run
        self.force_overwrite = args.force
        self.fetch_attempted = False
        self.num_tries = 0

    # def __init__(self, logfile, args):
    #     self.logfile = logfile
    #     self.output_dir = args.output_dir
    #     self._LOG = FlexLogger(
    #         self.__class__.__name__, self.output_dir, args.verbose)
    #     self.dry_run = args.dry_run
    #     self.force_overwrite = args.force
    #     self.fetch_attempted = False
    #     self.num_tries = 0

    def retrieve(self, server, retrieve_method, file_spec):
        """ Retrieve a file described in the file_spec from a given
        NGAS server using a streaming pull or a copy request.

        :param server: the URL of the server to retrieve from
        :param retrieve_method: 'copy' or 'stream', how to retrieve
        :param file_spec: location report for the file to retrieve
        @:returns Path
        """
        download_url = 'http://' + server + '/RETRIEVE'
        destination = self._get_destination(file_spec)

        if os.path.exists(destination):
            if not self.force_overwrite and not self.dry_run:
                raise FileExistsError(f'{destination} exists; aborting')

        self._make_basedir(destination)


        func = self._copying_fetch if retrieve_method == RetrievalMode.COPY \
            else self._streaming_fetch
        retryer = Retryer(func, MAX_TRIES, SLEEP_INTERVAL_SECONDS, self._LOG)
        try:
            retryer.retry(download_url, destination, file_spec)
        finally:
            self.num_tries = retryer.num_tries

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
            raise MissingSettingsException(k_err)

        except TypeError as t_err:
            raise MissingSettingsException(t_err)

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
                    raise FileErrorException(
                        f'output directory {basedir} is not writable')
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
        if not self.dry_run:
            self._LOG.debug(f'verifying fetch of {destination}')
            if not os.path.exists(destination):
                raise NGASServiceErrorException(
                    f'file not delivered to {destination}')
            if file_spec['size'] != os.path.getsize(destination):
                raise SizeMismatchException(
                    f"file retrieval size mismatch on {destination}: "
                    f"expected {file_spec['size']}, "
                    f"got {os.path.getsize(destination)}")
            self._LOG.debug('\tlooks good; sizes match')
        else:
            self._LOG.debug(
                '(This was a dry run; no files should have been fetched)')

    def _copying_fetch(self, args: list):
        """ Pull a file out of NGAS via the direct copy plugin.
            :param args: List
        """
        download_url, destination, file_spec = args

        params = {'file_id': file_spec['ngas_file_id'],
                  'processing': _DIRECT_COPY_PLUGIN,
                  'processingPars': 'outfile=' + destination,
                  'file_version': file_spec['version']}
        self._LOG.debug('attempting copying download:\nurl: {}\ndestination: {}'
                        .format(download_url, destination))
        if not self.dry_run:
            with requests.Session() as session:
                response = session.get(download_url, params=params)

                if response.status_code != http.HTTPStatus.OK:
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

        else:
            self._LOG.debug(
                f'if this were not a dry run, we would have been copying '
                f'{file_spec["relative_path"]}')

        return True

    def _streaming_fetch(self, args: list):
        """ Pull a file out of NGAS via streaming.

        :param args: list
        :return:
        """

        download_url, destination, file_spec = args

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
                    with open(destination, 'wb') as file_to_write:
                        # a word to the wise: DO NOT try to assign chunk size
                        # to a variable or to make it a constant. This will
                        # result in an incomplete download.
                        for chunk in response.iter_content(
                                chunk_size=_STREAMING_CHUNK_SIZE):
                            file_to_write.write(chunk)
                    expected_size = file_spec['size']
                    actual_size   = os.path.getsize(destination)
                    if actual_size == 0:
                        raise FileErrorException(
                            f'{os.path.basename(destination)} '
                            f'was not retrieved')
                    if actual_size != expected_size:
                        raise SizeMismatchException(
                            f'expected {os.path.basename(destination)} '
                            f'to be {expected_size} bytes; '
                            f'was {actual_size} bytes'
                        )

                except requests.exceptions.ConnectionError:
                    raise NGASServiceErrorException(
                        f'problem connecting with {download_url}')
                except AttributeError as a_err:
                    self._LOG.warning(f'possible problem streaming: {a_err}')

                if response.status_code !=  http.HTTPStatus.OK:
                    self._LOG.error('NGAS does not like this request:\n{}'
                                    .format(response.url))
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    ngams_status = soup.NgamsStatus.Status
                    message = ngams_status.get("Message")

                    raise NGASServiceErrorException(
                        {'status_code': response.status_code,
                         'url': response.url,
                         'reason': response.reason,
                         'message': message})

                self._LOG.debug('retrieved {} from {}'.
                               format(destination, response.url))

        else:
            self._LOG.debug(
                f'if this were not a dry run, we would have been streaming '
                f'{file_spec["relative_path"]}')

        return True
