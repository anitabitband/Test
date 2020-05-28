""" Unit tests for locations report """

import logging
import os
import re
import tempfile
import unittest
from time import time
from json import JSONDecodeError

import pytest

from tests.testing_utils import LOCATION_REPORTS, DATA_DIR
from yoink.errors import Errors, NoLocatorException, MissingSettingsException
from yoink.locations_report import LocationsReport
from yoink.utilities import get_capo_settings, get_metadata_db_settings, \
    ProductLocatorLookup, get_arg_parser, RetrievalMode, LOG_FORMAT


class LocationsReportTestCase(unittest.TestCase):
    ''' locations report test case'''
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = 'nmtest'
        cls.settings = get_capo_settings(cls.profile)
        cls.db_settings = get_metadata_db_settings(cls.profile)
        cls._13b_locator = ProductLocatorLookup(cls.db_settings) \
            .look_up_locator_for_ext_name(
                '13B-014.sb28862036.eb29155786.56782.5720116088')

    @classmethod
    def setUp(cls) -> None:
        umask = os.umask(0o000)
        cls.top_level = tempfile.mkdtemp()
        cls.configure_logging(cls)
        os.umask(umask)

    def configure_logging(self):
        ''' set up logging
        '''
        log_pathname = f'LocationsReport_{str(time())}.log'
        self._logfile = os.path.join(self.top_level, log_pathname)
        self._LOG = logging.getLogger(self._logfile)
        self.handler = logging.FileHandler(self._logfile)
        formatter = logging.Formatter(LOG_FORMAT)
        self.handler.setFormatter(formatter)
        self._LOG.addHandler(self.handler)

    def test_init_failure(self):

        with pytest.raises(TypeError):
            LocationsReport(None, None, None)

        with pytest.raises(TypeError):
            LocationsReport(None, None, self.settings)

        args = ['--product-locator', self._13b_locator,
                '--output-dir', '/nope', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        with pytest.raises(MissingSettingsException):
            LocationsReport(self._logfile, namespace, None)

        args = ['--product-locator', self._13b_locator]
        namespace = get_arg_parser().parse_args(args)
        with pytest.raises(MissingSettingsException):
            LocationsReport(self._logfile, namespace, None)

        args = ['--output-dir', None, '--profile', None]
        with pytest.raises(SystemExit) as s_ex:
            get_arg_parser().parse_args(args)
        self.assertEqual(Errors.MISSING_SETTING.value, s_ex.value.code,
                         'should throw MISSING_SETTING error')

        # empty destination, profile shouldn't matter
        args = ['--product-locator', self._13b_locator,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        LocationsReport(self._logfile, namespace, self.settings)

    def test_filters_sdms(self):
        args = ['--product-locator', self._13b_locator,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        all_files = report.files_report['files']

        args.append('--sdm-only')
        namespace = get_arg_parser().parse_args(args)
        filtered_report = LocationsReport(self._logfile, namespace, self.settings)
        filtered_files = filtered_report.files_report['files']
        sdms = [file for file in all_files
                if re.match(r'.*\.(xml|bin)$', file['relative_path'])]
        self.assertEqual(sdms, filtered_files,
                         'CALs and BDFs should have been filtered out')

    def test_throws_missing_setting_with_missing_locator(self):
        args = ['--product-locator', None,
                '--output-dir', None, '--profile', None]
        with pytest.raises(SystemExit) as s_ex:
            get_arg_parser().parse_args(args)
        self.assertEqual(Errors.MISSING_SETTING.value, s_ex.value.code,
                         'should throw MISSING_SETTING error')

    def test_throws_no_locator_with_bad_locator(self):
        args = ['--product-locator', 'Fred',
                '--output-dir', None, '--profile', None]
        with pytest.raises(NoLocatorException):
            namespace = get_arg_parser().parse_args(args)
            LocationsReport(self._logfile, namespace, self.settings)

    def test_throws_file_error_if_cant_find_report_file(self):
        args = ['--location-file', 'Mildred',
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        with pytest.raises(FileNotFoundError):
            LocationsReport(self._logfile, namespace, self.settings)

    def test_gets_expected_eb_from_locator(self):
        args = ['--product-locator', self._13b_locator,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        files = report.files_report['files']
        self.assertEqual(91, len(files), 'expecting 91 files in report')

    def test_gets_expected_servers_info_from_locator(self):
        args = ['--product-locator', self._13b_locator,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        server_info = report.servers_report
        self.assertEqual(3, len(server_info), 'files should be on 3 NGAS hosts')
        for server in ('1', '3', '4'):
            server_url = 'nmngas0' + server + '.aoc.nrao.edu:7777'
            self.assertTrue(server_url in server_info.keys())
            num_files = len(server_info[server_url]['files'])
            if server == '1':
                self.assertEqual(1, num_files,
                                 f'{server_url} should have 1 file')
            elif server == '3':
                self.assertEqual(30, num_files,
                                 f'{server_url} should have 30 files')
            elif server == '4':
                self.assertEqual(60, num_files,
                                 f'{server_url} should have 60 files')

    def test_gets_expected_images_from_file(self):
        report_metadata = LOCATION_REPORTS['IMG']
        report_file = os.path.join(DATA_DIR, report_metadata['filename'])

        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        files = report.files_report['files']
        self.assertEqual(report_metadata['file_count'], len(files),
                         f"expecting {report_metadata['file_count']} files in report")

        server_info = report.servers_report
        self.assertEqual(report_metadata['server_count'], len(server_info),
                         f"expecting files to be on "
                         f"{report_metadata['server_count']} NGAS hosts")
        for item in server_info.items():
            server_url = item[0]
            file = item[1]['files'][0]

            if 'nmngas01' in server_url:
                self.assertEqual(
                    file['checksum'], '-1675665022',
                    f"{server_url} file checksum")
                self.assertEqual(
                    file['ngas_file_id'],
                    'uid____evla_image_56a10be7-f1c2-4788-8651-6ecc5bfbc2f1.fits',
                    f"{server_url} file ngas_file_id")
            elif 'nmngas02' in server_url:
                self.assertEqual(
                    file['checksum'], '1271435719',
                    f"{server_url} file checksum")
                self.assertEqual(
                    file['ngas_file_id'],
                    'uid____evla_image_b10137d8-d2ef-4286-a5c9-a3b8cd74f276.fits',
                    f"{server_url} file ngas_file_id")
            else:
                self.fail(f"didn't expect to find {server_url}")

    def test_gets_vla_large_from_file_correctly(self):
        report_metadata = LOCATION_REPORTS['VLA_LARGE_EB']
        report_file = os.path.join(DATA_DIR, report_metadata['filename'])
        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        files = report.files_report['files']
        self.assertEqual(report_metadata['file_count'], len(files),
                         f"expecting {report_metadata['file_count']} files in report")
        server_info = report.servers_report
        self.assertEqual(report_metadata['server_count'], len(server_info),
                         f"expecting files to be on "
                         f"{report_metadata['server_count']} NGAS hosts")

        for item in server_info.items():
            files = item[1]['files']
            server_url = item[0]
            if 'nmngas01' in server_url:
                self.assertEqual(6, len(files),
                                 f'expecting 6 files on {server_url}')
            elif 'nmngas02' in server_url:
                self.assertEqual(40, len(files), f'expecting 40 files on '
                                                 f'{server_url}')
            else:
                self.fail(
                    f"not expecting {server_url} in {report_metadata['filename']}")

    def test_gets_vla_small_from_file_correctly(self):
        report_metadata = LOCATION_REPORTS['VLA_SMALL_EB']
        report_file = os.path.join(DATA_DIR, report_metadata['filename'])
        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        files = report.files_report['files']
        self.assertEqual(report_metadata['file_count'], len(files),
                         f"expecting {report_metadata['file_count']} files in report")
        server_info = report.servers_report
        self.assertEqual(report_metadata['server_count'], len(server_info),
                         f"expecting files to be on "
                         f"{report_metadata['server_count']} NGAS hosts")

        for item in server_info.items():
            files       = item[1]['files']
            server_url  = item[0]
            if 'nmngas03' in server_url:
                self.assertEqual(3, len(files), f'expecting 3 files on '
                                                f'{server_url}')
            elif 'nmngas04' in server_url:
                self.assertEqual(41, len(files), f'expecting 41 files on '
                                                 f'{server_url}')
            else:
                self.fail(f"not expecting {server_url} in {report_metadata['filename']}")

    def test_gets_expected_vlbas_from_file(self):
        report_metadata = LOCATION_REPORTS['VLBA_EB']
        report_file = os.path.join(DATA_DIR, report_metadata['filename'])

        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        files = report.files_report['files']
        self.assertEqual(report_metadata['file_count'], len(files),
                         f"expecting {report_metadata['file_count']} files in report")

        server_info = report.servers_report
        self.assertEqual(report_metadata['server_count'], len(server_info),
                         f"expecting files to be on "
                         f"{report_metadata['server_count']} NGAS host")
        for item in server_info.items():
            file = item[1]['files'][0]
            ngas_id = file['ngas_file_id']
            self.assertEqual(ngas_id, file['relative_path'],
                             'ngas_file_id = relative_path for VLBA files')
            self.assertTrue(str(ngas_id).endswith('.uvfits'),
                            'these should all be VLBA_VSN0011..UVFITS files')
            self.assertTrue(str(ngas_id)
                            .startswith('VLBA_VSN0011'),
                            'these should all be VLBA_VSN0011..UVFITS files')

    def test_throws_json_error_if_nothing_in_report_file(self):
        report_file = os.path.join(DATA_DIR, 'EMPTY.json')
        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        with pytest.raises(JSONDecodeError):
            LocationsReport(self._logfile, namespace, self.settings)

    def test_throws_json_error_if_report_file_is_not_json(self):
        report_file = os.path.join(DATA_DIR, 'NOT_JSON.json')
        args = ['--location-file', report_file,
                '--output-dir', None, '--profile', None]
        namespace = get_arg_parser().parse_args(args)
        with pytest.raises(JSONDecodeError):
            LocationsReport(self._logfile, namespace, self.settings)

    def test_local_profile_is_streaming_else_copy(self):
        old_exec_site = self.settings['execution_site']
        self.settings['execution_site'] = 'somewhere else'
        try:
            args = ['--product-locator', self._13b_locator,
                    '--output-dir', None, '--profile', self.profile]
            namespace = get_arg_parser().parse_args(args)
            report = LocationsReport(self._logfile, namespace, self.settings)
            server_info = report.servers_report
            for item in server_info.items():
                self.assertEqual(RetrievalMode.STREAM,
                                 item[1]['retrieve_method'],
                                 'files should be streamed')
        finally:
            self.settings['execution_site'] = old_exec_site

        args = ['--product-locator', self._13b_locator,
                '--output-dir', None, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        report = LocationsReport(self._logfile, namespace, self.settings)
        server_info = report.servers_report
        for item in server_info.items():
            self.assertEqual(RetrievalMode.COPY, item[1]['retrieve_method'],
                             'files should be direct-copied')


if __name__ == '__main__':
    unittest.main()
