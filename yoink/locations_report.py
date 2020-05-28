""" A locations report is produced by the archive service; you give
    it a product locator and it returns a dictionary of details on how
    to retrieve the product's files from long term storage (NGAS): this
    class handles fetching the report from the service or reading it from
    a file, and has utilities to manipulate the report.

"""

import copy
import http
import json
import logging

import requests

from yoink.errors import LocationServiceTimeoutException, \
    LocationServiceRedirectsException, LocationServiceErrorException, \
    NoLocatorException, MissingSettingsException
from yoink.utilities import Cluster, RetrievalMode, validate_file_spec, \
    LOG_FORMAT


class LocationsReport:
    ''' the location report class

    '''

    def __init__(self, logfile, args, settings):
        self._logfile = logfile
        self._verbose = args and args.verbose
        self.configure_logging()
        self._capture_and_validate_input(args, settings)
        self._run()

    # TODO: duplicate code; consolidate
    #  HERE AND ELSEWHERE: easier just to pass the logger?
    def configure_logging(self):
        ''' set up logging
        '''
        self._LOG = logging.getLogger(self._logfile)
        self.handler = logging.FileHandler(self._logfile)
        formatter = logging.Formatter(LOG_FORMAT)
        self.handler.setFormatter(formatter)
        self._LOG.addHandler(self.handler)

        level = logging.DEBUG if self._verbose else logging.WARN
        self._LOG.setLevel(level)

    def _capture_and_validate_input(self, args, settings):
        if args is None:
            raise MissingSettingsException(
                'arguments (locator and/or report file, destination) '
                'are required')
        self.args = args
        if settings is None:
            raise MissingSettingsException('CAPO settings are required')
        self.settings = settings

        if not self.settings['execution_site']:
            raise MissingSettingsException('execution_site is required')

        self.product_locator = args.product_locator
        self.location_file = args.location_file
        if not self.product_locator and not self.location_file:
            raise NoLocatorException(
                'either product locator or report file must be specified')

        self.sdm_only = args.sdm_only

    def _run(self):
        self.files_report   = self._get_files_report()
        self.servers_report = self._get_servers_report()

    def _add_retrieve_method_field(self, files_report):
        """ This adds a field to the files report about whether we can do
        a direct copy or we have to rely on streaming: this is something
        the location service itself doesn't know because it depends on
        which site yoink is running on, which site has the data and whether
        the NGAS cluster supports direct copy. """
        dsoc_cluster = Cluster.DSOC
        exec_site = self.settings['execution_site']

        for file_spec in files_report['files']:
            validate_file_spec(file_spec, False)

            server = file_spec['server']
            location = server['location']
            if server['cluster'] == dsoc_cluster.value \
                    and (location == exec_site or location == str(exec_site)):
                server['retrieve_method'] = RetrievalMode.COPY
            else:
                server['retrieve_method'] = RetrievalMode.STREAM

        return files_report

    def _filter_sdm_only(self, files_report):
        """ The user wants only the SDM tables, not the BDFs, so filter
        everything else out. Note, if they specify SDM-Only and the product
        is not a EVLA execution block things will go badly for them. """
        if self.sdm_only:
            result = list()
            for file_spec in files_report['files']:
                relative_path = file_spec['relative_path']
                if relative_path.endswith('.bin') or \
                        relative_path.endswith('.xml'):
                    result.append(file_spec)
            files_report['files'] = result
        return files_report

    def _get_servers_report(self):
        """ The location report we get back looks like this, for each file:

        {"ngas_file_id":"17B-197_2018_02_19_T15_59_16.097.tar",
        "subdirectory":"17B-197.sb34812522.eb35115211.58168.58572621528",
        "relative_path":"17B-197_2018_02_19_T15_59_16.097.tar",
        "checksum":"-1848259250",
        "checksum_type":"ngamsGenCrc32",
        "version":1,
        "size":108677120,
        "server":{"server":"nmngas01.aoc.nrao.edu:7777",
            "location":"DSOC",
            "cluster":"DSOC"
        }}

        Re-organize it to group files under servers so it is more useful.
        """

        result = dict()
        for file_spec in self.files_report['files']:
            validate_file_spec(file_spec, True)
            new_f = copy.deepcopy(file_spec)
            del new_f['server']
            server = file_spec['server']
            server_host = server['server']
            if server_host not in result:
                result[server_host] = dict()
                result[server_host]['location'] = server['location']
                result[server_host]['cluster'] = server['cluster']
                result[server_host]['retrieve_method'] \
                    = server['retrieve_method']
                result[server_host]['files'] = list()
            result[server_host]['files'].append(new_f)
        return result

    def _get_files_report(self):
        """ Given a product locator or a path to a location file, return a
        location report: an object describing the files that make up the product
        and where to get them from.
        If neither argument is provided, throw a ValueError; if both are
        (for some reason), then the location file takes precedence.

        :return: location report (from file, in JSON)
        """
        result = dict()
        if self.product_locator is None and self.location_file is None:
            raise ValueError(
                'product_locator or location_file must be provided; '
                'neither were')
        if self.location_file is not None:
            result = self._get_location_report_from_file()
        if self.product_locator is not None:
            result = self._get_location_report_from_service()
        if self.sdm_only:
            result = self._filter_sdm_only(result)
        return self._add_retrieve_method_field(result)

    def _get_location_report_from_file(self):
        """ Read a file at a user provided path
            to pull in the location report.
        """
        # try:
        with open(self.location_file) as to_read:
            result = json.load(to_read)
            return result
        # except (JSONDecodeError, FileNotFoundError):
        #     raise
        # except Exception as ex:
        #     self._LOG.error(f'>>> unexpected exception thrown: {ex}')
        #     raise

    def _get_location_report_from_service(self):
        """ Use 'requests' to fetch the location report from the locator service.

        :return: location report (from locator service, in JSON)
        """
        self._LOG.debug('fetching report from {} for {}'.format(
            self.settings['locator_service_url'], self.product_locator))

        try:
            response = requests.get(self.settings['locator_service_url'],
                                    params={'locator': self.product_locator})
        except requests.exceptions.Timeout:
            raise LocationServiceTimeoutException()
        except requests.exceptions.TooManyRedirects:
            raise LocationServiceRedirectsException()
        except requests.exceptions.RequestException as ex:
            raise LocationServiceErrorException(ex)

        if response.status_code == http.HTTPStatus.OK:
            return response.json()
        elif response.status_code == http.HTTPStatus.NOT_FOUND:
            raise NoLocatorException('cannot find locator "{}"'.
                                     format(self.product_locator))
        else:
            raise LocationServiceErrorException('locator service returned {}'
                                                .format(response.status_code))
