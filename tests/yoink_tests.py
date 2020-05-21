import logging
import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tests.testing_utils import get_locations_report, LOCATION_REPORTS, \
    get_mini_locations_file, write_locations_file, DATA_DIR, get_locations_file
from yoink.commands import Yoink
from yoink.errors import Errors
from yoink.locations_report import LocationsReport
from yoink.utilities import get_capo_settings, get_arg_parser, \
    ProductLocatorLookup, get_metadata_db_settings, ExecutionSite, RetrievalMode

LOG_FORMAT = "%(name)s.%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

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

    def test_bad_command_line(self):
        command = 'yoink'
        args = [command,
                '--product-locator', 'not even',
                '--profile', 'local', '--output-dir', self.top_level]

        output = Yoinker().run(args)
        exception_found = False
        terminal_exception_thrown = False
        for line in output:
            if 'NoLocatorException' in line:
                exception_found = True
            if 'terminal_exception' in line:
                terminal_exception_thrown = True
            if exception_found and terminal_exception_thrown:
                break
        self.assertTrue(exception_found, 'expecting NoLocatorException')
        self.assertTrue(terminal_exception_thrown, 'terminal_exception should be thrown')

        args = [command, '--location-file', 'aint_got_one']
        output = Yoinker().run(args)
        exception_found = False
        terminal_exception_thrown = False
        for line in output:
            if 'FileNotFoundError' in line:
                exception_found = True
            if 'terminal_exception' in line:
                terminal_exception_thrown = True
            if exception_found and terminal_exception_thrown:
                break
        self.assertTrue(exception_found, 'expecting FileNotFoundError')
        self.assertTrue(terminal_exception_thrown, 'terminal_exception should be thrown')

        args = [command,
                '--location-file',
                get_mini_locations_file(os.path.join(self.top_level,
                                                     'locations.json')),
                '--profile', 'local', '--output-dir', '/']
        output = Yoinker().run(args)
        exception_found = False
        terminal_exception_thrown = False
        for line in output:
            if 'PermissionError' in line:
                exception_found = True
            if 'terminal_exception' in line:
                terminal_exception_thrown = True
            if exception_found and terminal_exception_thrown:
                break
        self.assertTrue(exception_found, 'expecting PermissionError')
        self.assertTrue(terminal_exception_thrown, 'terminal_exception should be thrown')

    def test_nothing_retrieved_if_dry_on_cmd_line(self):
        command = 'yoink'
        location_file = get_mini_locations_file(os.path.join(self.top_level,
                                                             'locations.json'))
        args = [command,
                '--location-file', location_file,
                '--profile', 'local', '--output-dir', self.top_level,
                '--dry', '--verbose']
        output = Yoinker().run(args)
        filenames_found = []
        for line in output:
            found = re.match(
                r'.*/RETRIEVE\?file_id=(.+\.(xml|bin|sdm)).*', line)
            if found:
                filenames_found.append(found.groups()[0])

        filenames_found = set(filenames_found)
        self.assertEqual(0, len(filenames_found), 'expecting no files')

        # make sure none of these files was actually written
        os.remove(location_file)
        file_count = 0
        for anything in os.walk(location_file):
            file_count += 1
        self.assertEqual(0, file_count, 'expecting no files')

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
                '--profile', 'local', '--output-dir', self.top_level,
                '--force']
        Yoinker().run(args)
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
        output = Yoinker().run(args)

        term_except_found = False
        file_exists_found = False
        for line in output:
            if 'terminal_exception' in line:
                term_except_found = True
            if 'FileExistsError' in line:
                file_exists_found = True
            if term_except_found and file_exists_found:
                break

        self.assertTrue(term_except_found and file_exists_found,
                        'expecting terminal_exception for FileExistsError')

    def test_cmd_line_verbose_sdm_output_from_file(self):
        command = 'yoink'
        args = [command,
                '--location-file',
                get_mini_locations_file(os.path.join(self.top_level,
                                                     'locations.json')),
                '--profile', 'local', '--output-dir', self.top_level,
                '--sdm-only', '--verbose']
        output = Yoinker().run(args)
        filenames_found = []
        for line in output:
            found = re.match(
                r'.*/RETRIEVE\?file_id=(.+\.(xml|bin|sdm)).*', line)
            if found:
                filenames_found.append(found.groups()[0])

        filenames_found = set(filenames_found)
        self.assertEqual(37, len(filenames_found), 'expecting 37 SDMs')

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

    def test_copy_attempt_throws_sys_exit_service_error(self):
        test_data_13B_014 = self.test_data['13B-014']

        product_locator = test_data_13B_014['product_locator']

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
        test_data_13B_014 = self.test_data['13B-014']
        destination = os.path.join(
            self.top_level, test_data_13B_014['external_name'])
        product_locator = test_data_13B_014['product_locator']

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
        test_data_13B_014 = self.test_data['13B-014']
        bad_path = '/foo'
        args = ['--product-locator', test_data_13B_014['product_locator'],
                '--output-dir', bad_path,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        with pytest.raises(SystemExit) as s_ex:
            yoink.run()
        self.assertEqual(Errors.FILE_ERROR.value, s_ex.value.code,
                         'should throw FILE_ERROR')

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
        data_13B = self.test_data['13B-014']
        self.assertEqual('13B-014.sb28862036.eb29155786.56782.5720116088',
                         data_13B['external_name'])
        locator = data_13B['product_locator']
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

    def test_throws_sys_exist_no_locator_if_no_product_locator(self):
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


class Yoinker:
    """ Wraps subprocess to run or mock command line
    """
    @classmethod
    def run(self, args):
        ''' executes command line represented by args

            :returns command output
        '''
        if not args:
            raise ValueError('arguments are required')

        verbose = '--verbose' in args
        logging.basicConfig(level=logging.DEBUG) if verbose \
            else logging.basicConfig(level=logging.WARN)
        lines = list()
        with subprocess.Popen(args,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT,
                              bufsize=1,
                              universal_newlines=True) as proc:
            if proc.stderr:
                for err in proc.stderr:
                    lines.append(err.strip())

            output = proc.stdout
            error_found = output.errors
            for line in output:
                line = line.strip()
                _LOG.info(f'{line}')
                if error_found or verbose:
                    lines.append(line)

            return lines


if __name__ == '__main__':
    unittest.main()
