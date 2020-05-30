""" Unit tests for yoink as a whole """

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest

from tests.testing_utils import get_locations_report, LOCATION_REPORTS, \
    get_mini_locations_file, write_locations_file, DATA_DIR, get_locations_file, \
    find_yoink_log_file
from yoink.commands import Yoink
from yoink.errors import Errors
from yoink.locations_report import LocationsReport
from yoink.utilities import get_capo_settings, get_arg_parser, \
    ProductLocatorLookup, get_metadata_db_settings, ExecutionSite, \
    RetrievalMode, FlexLogger

VLA_SMALL_KEY = 'VLA_SMALL_EB'


class YoinkTestCase(unittest.TestCase):
    """ IMPORTANT NOTE: we CANNOT retrieve by copy if we don't have access to a
        location to which NGAS can write, e.g, lustre. Therefore, any test
        that involves -actual- retrieval of files must be by streaming, to
        ensure which we must use a Capo profile in which the execution site is
        -not- DSOC or NAASC.
        The reason is this algorithm used in LocationsReport:

         for f in files_report['files']:
             if f['server']['cluster'] == Cluster.DSOC and \
                     f['server']['location'] == self.settings['execution_site']:
                 f['server']['retrieve_method'] = RetrievalMode.COPY
             else:
                 f['server']['retrieve_method'] = RetrievalMode.STREAM


        Be sure to have on the test system a local profile (local.properties)
        that meets these criteria:

      - edu.nrao.archive.workflow.config.StartupSettings.temporaryDataDirectory
          pointing to a locally writable temp dir, e.g., /var/tmp
      - edu.nrao.archive.workflow.config.DeliverySettings.hostname
        must point to local computer
      - execution_site must NOT be DSOC or NAASC


    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = 'local'
        cls.settings = get_capo_settings(cls.profile)
        cls.db_settings = get_metadata_db_settings(cls.profile)
        cls.test_data = cls._initialize_test_data(cls)

    @classmethod
    def setUp(cls) -> None:
        umask = os.umask(0o000)
        cls.top_level = tempfile.mkdtemp()
        os.umask(umask)
        cls._LOG = FlexLogger(cls.__class__.__name__, cls.top_level)

    def test_bad_command_line(self):
        command = 'yoink'
        args = [command,
                '--product-locator', 'not even',
                '--profile', 'local', '--output-dir', self.top_level]
        yoinker = Yoinker(args, self._LOG)
        yoinker.run()
        exception_found = False
        terminal_exception_thrown = False

        logfile = find_yoink_log_file(self.top_level)
        self.assertNotEqual(0, os.path.getsize(logfile),
                            f'expecting a non-empty log file in {self.top_level}')
        with open(logfile, 'r') as log:
            log_contents = log.readlines()

        for line in log_contents:
            if 'NoLocatorException' in line:
                exception_found = True
            if 'terminal_exception' in line:
                terminal_exception_thrown = True
            if exception_found and terminal_exception_thrown:
                break
        self.assertTrue(exception_found, 'expecting NoLocatorException')
        self.assertTrue(terminal_exception_thrown, 'terminal_exception should be thrown')

        os.rename(logfile, str(logfile).replace('Yoink_', 'bad_locator__'))
        args = [command, '--location-file', 'aint_got_one', '--output-dir',
                self.top_level]

        yoinker = Yoinker(args, self._LOG)
        yoinker.run()
        logfile = find_yoink_log_file(self.top_level)
        with open(logfile, 'r') as log:
            log_contents = log.readlines()

        exception_found = False
        terminal_exception_thrown = False
        for line in log_contents:
            if 'FileNotFoundError' in line:
                exception_found = True
            if 'terminal_exception' in line:
                terminal_exception_thrown = True
            if exception_found and terminal_exception_thrown:
                break
        self.assertTrue(exception_found, 'expecting FileNotFoundError')
        self.assertTrue(terminal_exception_thrown, 'terminal_exception should be thrown')

    def test_nothing_retrieved_if_dry_on_cmd_line(self):
        command = 'yoink'
        location_file = get_mini_locations_file(os.path.join(self.top_level,
                                                             'locations.json'))
        args = [command,
                '--location-file', location_file,
                '--profile', 'local', '--output-dir', self.top_level,
                '--dry', '--verbose']
        yoinker = Yoinker(args, self._LOG)
        output = yoinker.run()
        logfile = find_yoink_log_file(self.top_level)
        self.assertEqual([], output, 'expecting no files for dry run')
        self.assertNotEqual(0, os.path.getsize(logfile),
                            'log file should be non-empty because verbose')

        # make sure none of these files written
        os.remove(location_file)
        file_count = 0
        for anything in os.walk(location_file):
            file_count += 1
        self.assertEqual(0, file_count, 'no files should have been retrieved')

    def test_force_overwrite_from_cmd_line(self):
        command = 'yoink'
        location_file = get_mini_locations_file(os.path.join(self.top_level,
                                                             'locations.json'))
        dest_dir = os.path.join(self.top_level, 'sysstartS.58955.83384832176')
        Path(dest_dir).mkdir(parents=True, exist_ok=True)

        # make a fake file to be overwritten
        fake_file = os.path.join(dest_dir, 'ASDM.xml')
        with open(fake_file, 'w') as to_write:
            to_write.write('alas, my days are numbered')
        args = [command,
                '--location-file', location_file,
                '--profile', 'local',
                '--output-dir', self.top_level,
                '--force']
        Yoinker(args, self._LOG).run()

        sizes = dict()
        for dirname, dirnames, fnames in os.walk(dest_dir):
            for fname in fnames:
                path = os.path.join(dest_dir, fname)
                sizes[path] = os.path.getsize(path)
        self.assertEqual(37, len(sizes), 'expecting 37 files to be fetched')
        fake_size = os.path.getsize(fake_file)
        self.assertEqual(9339, fake_size, f'expecting '
                                          f'{os.path.basename(fake_file)} to '
                                          f'be 9339 bytes')

    def test_no_overwrite_from_cmd_line(self):
        command = 'yoink'
        location_file = get_mini_locations_file(os.path.join(self.top_level,
                                                             'locations.json'))
        dest_dir = os.path.join(self.top_level, 'sysstartS.58955.83384832176')
        Path(dest_dir).mkdir(parents=True, exist_ok=True)

        # make a fake file that shouldn't be overwritten
        fake_file = os.path.join(dest_dir, 'ASDM.xml')
        with open(fake_file, 'w') as to_write:
            to_write.write("I'm not going anywhere!")
        args = [command,
                '--location-file', location_file,
                '--profile', 'local', '--output-dir', self.top_level]
        yoinker = Yoinker(args, self._LOG)
        yoinker.run()

        term_except_found = False
        file_exists_found = False
        logfile = find_yoink_log_file(self.top_level)
        with open(logfile, 'r') as log:
            log_contents = log.readlines()
        for line in log_contents:
            if 'terminal_exception' in line:
                term_except_found = True
            if 'FileExistsError' in line:
                file_exists_found = True
            if term_except_found and file_exists_found:
                break

        self.assertTrue(term_except_found and file_exists_found,
                        'expecting terminal_exception for FileExistsError')

    def test_cmd_line_more_output_when_verbose(self):
        command = 'yoink'
        args = [command,
                '--location-file',
                get_mini_locations_file(
                    os.path.join(self.top_level, 'locations_verbose.json')),
                '--profile', 'local', '--output-dir', self.top_level,
                '--verbose']
        yoinker = Yoinker(args, self._LOG)
        retrieved = yoinker.run()
        num_files_expected = 37
        self.assertEqual(num_files_expected, len(retrieved),
                         f'expecting {num_files_expected} files')

        logfile = find_yoink_log_file(self.top_level)
        self.assertNotEqual(0,  os.path.getsize(logfile),
                            'log should contain debug messages')

        [os.remove(file) for file in retrieved]
        logname = str(logfile).replace('Yoink_', 'yoink_verbose')
        os.rename(logfile, logname)

        # same thing, but without verbose
        args = [command,
                '--location-file',
                get_mini_locations_file(
                    os.path.join(self.top_level, 'locations.json')),
                '--profile', 'local', '--output-dir', self.top_level]
        yoinker = Yoinker(args, self._LOG)
        retrieved = yoinker.run()
        self.assertEqual(num_files_expected, len(retrieved),
                         f'expecting {num_files_expected} files')
        logfile = find_yoink_log_file(self.top_level)
        self.assertEqual(0,  os.path.getsize(logfile), 'log should be empty')

    def test_can_stream_from_mini_locations_file(self):
        """ gin up a location report with just a few small files in it
            and confirm that we can actually stream them
        """
        path = os.path.join(self.top_level, 'locations.json')
        report_file = get_mini_locations_file(path)
        args = ['--location-file', report_file, '--output-dir', self.top_level,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        retrieved = yoink.run()
        file_count = len(retrieved)
        print(
            f'{file_count} files should have been delivered to {self.top_level}')
        self.assertEqual(37, file_count)

    def test_verbose_writes_stuff_to_log(self):
        path = os.path.join(self.top_level, 'locations.json')
        report_file = get_mini_locations_file(path)
        args = ['--location-file', report_file, '--output-dir', self.top_level,
                '--sdm-only', '--profile', self.profile, '--verbose']
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        yoink.run()

        logfile = yoink.logfile
        self.assertTrue(os.path.isfile(logfile),
                        f'expecting log file {logfile}')
        self.assertNotEqual(0, os.path.getsize(logfile),
                            'there should be entries in the log file')

    def test_empty_log_if_not_verbose(self):
        path = os.path.join(self.top_level, 'locations.json')
        report_file = get_mini_locations_file(path)
        args = ['--location-file', report_file, '--output-dir', self.top_level,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        yoink.run()

        logfile = yoink.logfile
        self.assertTrue(os.path.isfile(logfile),
                        f'expecting log file {logfile}')
        self.assertEqual(0, os.path.getsize(logfile),
                         'log file should be empty')

    def test_copy_attempt_throws_sys_exit_service_error(self):
        product_locator = self.test_data['13B-014']['product_locator']

        # use site from non-local profile to guarantee copy attempt
        local_exec_site = self.settings['execution_site']
        self.settings['execution_site'] = ExecutionSite.DSOC

        args = ['--product-locator', product_locator,
                '--output-dir', self.top_level, '--sdm-only',
                '--profile', self.settings['execution_site'].value, '--verbose']
        parser = get_arg_parser()
        namespace = parser.parse_args(args)
        yoink = Yoink(namespace, self.settings)
        servers_report = yoink.servers_report
        for server in servers_report:
            entry = servers_report[server]
            self.assertTrue(entry['retrieve_method'] == RetrievalMode.COPY)

        # let's try just one file so we're not sitting here all day
        for server in servers_report:
            entry = servers_report[server]
            servers_report = {server: entry}
            break
        yoink.servers_report = servers_report
        files = yoink.servers_report[server]['files']
        yoink.servers_report[server]['files'] = [files[0]]

        try:
            with pytest.raises(SystemExit) as s_ex:
                yoink.run()
            self.assertEqual(Errors.NGAS_SERVICE_ERROR.value, s_ex.value.code)
        finally:
            self.settings['execution_site'] = local_exec_site

    def test_dies_with_bad_server_info(self):
        report_file = get_locations_file('VLA_BAD_SERVER')
        args = ['--location-file', report_file,
                '--output-dir', self.top_level,
                '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        with pytest.raises(SystemExit) as s_ex:
            yoink.run()
        exc_code = s_ex.value.code
        expected = Errors.NGAS_SERVICE_ERROR.value
        self.assertEqual(expected, exc_code)

    def test_throws_sys_exit_file_exists_if_overwrite_not_forced(self):
        file_spec = self.test_data['13B-014']
        destination = os.path.join(
            self.top_level, file_spec['external_name'])
        product_locator = file_spec['product_locator']

        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        # stick a fake SDM in there so it will fall over
        fake_file = os.path.join(destination, 'SysPower.bin')
        with open(fake_file, 'w') as to_write:
            to_write.write('lalalalalala')
        self.assertTrue(os.path.exists(fake_file))
        self.assertFalse(os.path.getsize(fake_file) == 0)

        parser = get_arg_parser()

        args = ['--product-locator', product_locator,
                '--output-dir', self.top_level, '--sdm-only', '--profile',
                'local']
        namespace = parser.parse_args(args)

        # exception should be thrown because one of the files to be retrieved
        # is in the destination dir and we're not forcing overwrite here
        with pytest.raises(SystemExit) as exc:
            yoink = Yoink(namespace, self.settings)
            yoink.run()
        exc_code = exc.value.code
        expected = Errors.FILE_EXISTS_ERROR.value
        self.assertEqual(expected, exc_code)

    def test_overwrites_when_forced(self):
        location_report = get_locations_report(VLA_SMALL_KEY)
        location_report = self._remove_large_files_from_location_report(
            location_report)
        report_file = write_locations_file(
            os.path.join(self.top_level, 'locations.json'), location_report)
        external_name = LOCATION_REPORTS['VLA_SMALL_EB']['external_name']
        destination = os.path.join(self.top_level, external_name)
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        to_overwrite = 'ASDM.xml'
        # stick a fake SDM in there to see if overwrite really happens
        fake_file = os.path.join(destination, to_overwrite)
        text = '"Bother!" said Pooh. "Lock phasers on that heffalump!"'
        with open(fake_file, 'w') as to_write:
            to_write.write(text)
        self.assertTrue(os.path.exists(fake_file), f'{to_overwrite} should '
                                                   f'have been created')
        self.assertEqual(len(text), os.path.getsize(fake_file),
                         f'before overwrite, {to_overwrite} should be'
                         f' {len(text)} bytes')

        args = ['--location-file', report_file,
                '--output-dir', self.top_level, '--force', '--profile', 'local']
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        retrieved = yoink.run()

        real_size = -1
        location_report = get_locations_report(VLA_SMALL_KEY)
        for file in location_report['files']:
            if to_overwrite == file['relative_path']:
                real_size = file['size']
                break
        for file in retrieved:
            if to_overwrite == os.path.basename(file):
                self.assertEqual(real_size, os.path.getsize(file),
                                 f'{to_overwrite} should be {real_size} bytes')

    def test_no_bdfs_with_sdm_only(self):

        report_key = VLA_SMALL_KEY
        report_spec = LOCATION_REPORTS[report_key]
        external_name = report_spec['external_name']
        expected_file_count = report_spec['file_count']
        locations_report = get_locations_report(report_key)
        self.assertEqual(expected_file_count, len(locations_report['files']),
                         f'expecting {expected_file_count} files in all')
        umask = os.umask(0o000)
        top_level = tempfile.mkdtemp()
        os.umask(umask)
        product_locator = ProductLocatorLookup(self.db_settings) \
            .look_up_locator_for_ext_name(external_name)

        args = ['--product-locator', product_locator,
                '--output-dir', top_level, '--dry',
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        yoink = Yoink(namespace, self.settings)
        expected_files = self._get_vla_small_sdms(locations_report)
        yoink.run = MagicMock(return_value=expected_files)

        retrieved = yoink.run()
        num_files_expected = len(expected_files)
        self.assertEqual(num_files_expected, len(retrieved),
                         f"expecting {len(expected_files)} SDMs "
                         f"from {os.path.basename(report_spec['filename'])}")

        match_count = 0
        for exp_file in expected_files:
            for act_file in retrieved:
                if os.path.basename(act_file) == os.path.basename(exp_file):
                    match_count += 1
                    break

        self.assertEqual(num_files_expected, match_count,
                         f'{num_files_expected} SDMs expected')

    def test_sys_exit_file_error_on_bad_destination(self):
        file_spec = self.test_data['13B-014']
        bad_path = os.path.abspath('/foo')
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', bad_path,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        # yoink = Yoink(namespace, self.settings)
        with pytest.raises(SystemExit) as s_ex:
            Yoink(namespace, self.settings)
        self.assertEqual(Errors.FILE_NOT_FOUND_ERROR.value, s_ex.value.code,
                         'should throw FILE_NOT_FOUND_ERROR')

    def test_sys_exit_no_locator_for_bad_product_locator(self):
        args = ['--product-locator', 'foo',
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        with pytest.raises(SystemExit) as s_ex:
            yoink = Yoink(namespace, self.settings)
            yoink.run()
        self.assertEqual(Errors.NO_LOCATOR.value, s_ex.value.code,
                         'should throw NO_LOCATOR')

    def test_gets_expected_test_data(self):
        self.assertIsNotNone(self.test_data['13B-014'])
        file_spec = self.test_data['13B-014']
        self.assertEqual('13B-014.sb28862036.eb29155786.56782.5720116088',
                         file_spec['external_name'])
        locator = file_spec['product_locator']
        self.assertTrue(locator.startswith('uid://evla/execblock/'))

    def test_gets_vlbas_from_report_file(self):
        report_file = get_locations_file('VLBA_EB')
        args = ['--location-file', report_file,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        report_files = yoink.locations_report.files_report['files']
        self.assertEqual(16, len(report_files),
                         f'{os.path.basename(report_file)} should have 16 '
                         f'files')
        expected_files = [os.path.join(self.top_level, item['relative_path'])
                          for item in report_files]
        yoink.run = MagicMock(return_value=expected_files)
        actual_files = yoink.run()
        num_expected = len(expected_files)
        self.assertEqual(num_expected, len(actual_files), f'expecting '
                                                          f'{num_expected} '
                                                          f'VLBA files')

        match_count = 0
        for exp_file in expected_files:
            for act_file in actual_files:
                if os.path.basename(act_file) == os.path.basename(exp_file):
                    match_count += 1
                    break
        self.assertEqual(num_expected, match_count,
                         f'{num_expected - match_count} file(s) are '
                         f'unaccounted for')

    def test_gets_large_vla_ebs_from_report_file(self):
        report_file = get_locations_file('VLA_LARGE_EB')
        args = ['--location-file', report_file,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        report_files = yoink.locations_report.files_report['files']
        self.assertEqual(46, len(report_files),
                         f'{os.path.basename(report_file)} should have 46 '
                         f'files')
        expected_files = [os.path.join(self.top_level, item['relative_path'])
                          for item in report_files]
        yoink.run = MagicMock(return_value=expected_files)
        actual_files = yoink.run()
        num_expected = len(expected_files)
        self.assertEqual(num_expected, len(actual_files), f'expecting '
                                                          f'{num_expected} '
                                                          f'VLBA files')

    def test_gets_images_from_report_file(self):
        report_file = get_locations_file('IMG')
        args = ['--location-file', report_file,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        report_files = yoink.locations_report.files_report['files']
        self.assertEqual(2, len(report_files),
                         f'{os.path.basename(report_file)} should have 2 files')
        expected_files = [os.path.join(self.top_level, item['relative_path'])
                          for item in report_files]
        yoink.run = MagicMock(return_value=expected_files)
        actual_files = yoink.run()
        num_expected = len(expected_files)
        self.assertEqual(num_expected, len(actual_files), f'expecting '
                                                          f'{num_expected} '
                                                          f'image files')

    def test_gets_calibration_from_report_file(self):
        report_file = get_locations_file('CALIBRATION')
        args = ['--location-file', report_file,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        report_files = yoink.locations_report.files_report['files']
        self.assertEqual(1, len(report_files),
                         f'{os.path.basename(report_file)} should have 1 file')
        file_spec = report_files[0]

        # calibration will have external name = relative path = subdirectory
        relative_path = file_spec['relative_path']
        self.assertEqual(relative_path, file_spec['subdirectory'],
                         'expecting relative_path same as subdirectory')

        expected_files = [os.path.join(self.top_level, relative_path)]
        yoink.run = MagicMock(return_value=expected_files)
        actual_files = yoink.run()
        num_expected = len(expected_files)
        self.assertEqual(num_expected, len(actual_files), f'expecting '
                                                          f'{num_expected} '
                                                          f'calibration')

    def test_gets_calibration_from_locator(self):
        external_name = LOCATION_REPORTS['CALIBRATION']['external_name']
        product_locator = ProductLocatorLookup(
            self.db_settings).look_up_locator_for_ext_name(external_name)
        args = ['--product-locator', product_locator,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        report_files = yoink.locations_report.files_report['files']
        self.assertEqual(1, len(report_files),
                         f'{external_name} should be 1 file')

        file_spec = report_files[0]

        # calibration will have external name = relative path = subdirectory
        relative_path = file_spec['relative_path']
        self.assertEqual(external_name, relative_path,
                         'expecting external_name same as relative path')
        self.assertEqual(relative_path, file_spec['subdirectory'],
                         'expecting relative_path same as subdirectory')

        expected_files = [os.path.join(self.top_level, relative_path)]
        yoink.run = MagicMock(return_value=expected_files)
        actual_files = yoink.run()
        num_expected = len(expected_files)
        self.assertEqual(num_expected, len(actual_files), f'expecting '
                                                          f'{num_expected} '
                                                          f'calibration')
    def test_retrieval_finds_size_mismatch(self):
        report_spec = LOCATION_REPORTS[VLA_SMALL_KEY]
        external_name = report_spec['external_name']

        locations_file = os.path.join(DATA_DIR, 'VLA_SMALL_EB_BUSTED.json')
        args = ['--location-file', locations_file,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink1 = Yoink(namespace, self.settings)
        report_files = yoink1.locations_report.files_report['files']
        self.assertEqual(44, len(report_files),
                         f'{os.path.basename(locations_file)} should have 44 '
                         f'files')

        filename = 'Weather.xml'
        for file in report_files:
            if filename == file['relative_path']:
                self.assertEqual(165100, file['size'])
                break

        product_locator = ProductLocatorLookup(self.db_settings) \
            .look_up_locator_for_ext_name(external_name)
        args = ['--product-locator', product_locator,
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink2 = Yoink(namespace, self.settings)

        locations_report = get_locations_report(VLA_SMALL_KEY)
        yoink2.run = MagicMock(return_value=locations_report['files'])
        locator_files = yoink2.run()
        self.assertEqual(len(report_files), len(locator_files),
                         'should get same no. files from locator as from '
                         'report file')
        for file1 in report_files:
            for file2 in locator_files:
                if file2['relative_path'] == file1['relative_path']:
                    if filename != file1['relative_path']:
                        self.assertEqual(file2['size'], file1['size'],
                                         'sizes should match')
                    else:
                        self.assertNotEqual(file2['size'], file1['size'],
                                            'sizes should match')
                    break

    def test_throws_sys_exit_missing_setting_if_no_args(self):
        args = []
        with pytest.raises(SystemExit) as s_ex:
            get_arg_parser().parse_args(args)
        self.assertEqual(Errors.MISSING_SETTING.value, s_ex.value.code,
                         'should throw MISSING_SETTING error')

    def test_throws_sys_exit_no_locator_if_no_product_locator(self):
        args = ['--product-locator', '',
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        with pytest.raises(SystemExit) as s_ex:
            Yoink(namespace, self.settings)
        self.assertEqual(Errors.NO_LOCATOR.value, s_ex.value.code,
                         'should throw NO_LOCATOR error')

    # --------------------------------------------------------------------------
    #
    #        U T I L I T I E S
    #
    # --------------------------------------------------------------------------

    @staticmethod
    def _remove_large_files_from_location_report(locations_in: LocationsReport):
        ''' strip files > 100000 bytes from location report, so we can try
            an actual stream without it taking forever

            :returns: LocationsReport
        '''

        files = locations_in['files']
        locations_out = locations_in.copy()
        locations_out['files'] = \
            [file for file in files if file['size'] <= 100000]
        return locations_out

    @staticmethod
    def _count_sdms(location_report: LocationsReport):
        sdms_found = [f for f in location_report['files']
                      if re.match(r'.*\.(xml|bin)$', f['relative_path'])]
        return len(sdms_found)

    def _get_vla_small_sdms(self, location_report: LocationsReport):
        sdms_found = []
        for file in location_report['files']:
            if re.match(r'.*\.(xml|bin)$', file['relative_path']):
                path = os.path.join(self.top_level, file['relative_path'])
                sdms_found.append(path)
        return sdms_found

    def _initialize_test_data(self):
        ext_name = '13B-014.sb28862036.eb29155786.56782.5720116088'

        product_locator = ProductLocatorLookup(self.db_settings) \
            .look_up_locator_for_ext_name(ext_name)
        dict13b = {'external_name': ext_name,
                   'product_locator': product_locator}

        to_return = {'13B-014': dict13b}
        return to_return


# logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
# _LOG = logging.getLogger(__name__)
class Yoinker:
    """ Launches Yoink from command line, with logging
    """

    def __init__(self, args: List, logger: FlexLogger):
        args_to_parse = args if args[0] != 'yoink' else args[1:]
        self._LOG = logger
        namespace = get_arg_parser().parse_args(args_to_parse)
        self.args = args
        self.output_dir = namespace.output_dir
        self.verbose = namespace.verbose

    def run(self):
        ''' launch yoink from command line
            @:returns directory listing
        '''
        with subprocess.Popen(self.args,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              bufsize=1,
                              universal_newlines=True) as proc:
            if proc.stderr:
                for err in proc.stderr:
                    self._LOG.error(err.strip())

            output = proc.stdout
            error_found = output.errors
            if error_found:
                if isinstance(error_found, list):
                    [self._LOG.error(line) for line in error_found]
                else:
                    if error_found != 'strict':
                        self._LOG.error(error_found)

            lines = list()
            for line in output:
                lines.append(line.strip())

            for i in range(0, len(lines) - 1):
                line = lines[i]
                self._LOG.debug(f'{line}')
                if 'error' in line.lower():
                    # log everything after the error
                    for j in range(i, len(lines) - 1):
                        self._LOG.error(lines[j])
                if 'debug' in line.lower() and self.verbose:
                    self._LOG.debug(line)
                if 'warn' in line.lower():
                    self._LOG.warning(line)

        files_retrieved = list()
        for root, dirnames, filenames in os.walk(self.output_dir):
            if dirnames:
                subdir = os.path.join(root, dirnames[0])
            else:
                subdir = root
            for filename in filenames:
                if not filename.endswith('.log') and not filename.endswith('.json'):
                    files_retrieved.append(os.path.join(subdir, filename))

        return files_retrieved


if __name__ == '__main__':
    unittest.main()
