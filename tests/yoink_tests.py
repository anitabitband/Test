import http
import logging
import os
import re
import tempfile
import unittest
from pathlib import Path
from typing import List

import pytest

from tests.testing_utils import get_locations_report, LOCATION_REPORTS, \
    get_mini_locations_file, write_locations_file
from yoink.commands import Yoink
from yoink.errors import Errors, NGASServiceErrorException
from yoink.utilities import get_capo_settings, get_arg_parser, \
    ProductLocatorLookup, get_metadata_db_settings, LocationsReport, \
    ExecutionSite, RetrievalMode

LOG_FORMAT = "%(name)s.%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

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

    def test_can_stream_from_mini_locations_file(self):
        ''' gin up a location report with just a few small files in it
            and confirm that we can actually stream them
        '''
        umask = os.umask(0o000)
        top_level = tempfile.mkdtemp()
        os.umask(umask)
        path = os.path.join(top_level, 'locations.json')
        report_file = get_mini_locations_file(path)
        args = ['--location-file', report_file, '--output-dir', top_level,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        retrieved = yoink.run()
        file_count = len(retrieved)
        print(f'{file_count} files should have been delivered to {top_level}')
        self.assertEqual(37, file_count)

    def test_expected_copy_failure(self):
        test_data_13B_014 = self.test_data['13B-014']

        umask = os.umask(0o000)
        top_level = tempfile.mkdtemp()
        os.umask(umask)

        product_locator = test_data_13B_014['product_locator']

        # use site from non-local profile to guarantee copy attempt
        local_exec_site = self.settings['execution_site']
        self.settings['execution_site'] = ExecutionSite.DSOC

        args = ['--product-locator', product_locator,
                '--output-dir', top_level, '--sdm-only',
                '--profile', self.settings['execution_site'].value, '--verbose']
        parser = get_arg_parser()
        namespace = parser.parse_args(args)
        yoink = Yoink(namespace, self.settings)
        servers_report = yoink.servers_report
        for server in servers_report:
            entry = servers_report[server]
            self.assertTrue(entry['retrieve_method'] == RetrievalMode.COPY)

        try:
            with pytest.raises(NGASServiceErrorException) as s_ex:
                yoink.run()
            details = s_ex.value.args[0]
            self.assertEqual(
                http.HTTPStatus.BAD_REQUEST, details['status_code'])
        except Exception as exc:
            raise exc
        finally:
            self.settings['execution_site'] = local_exec_site

    def test_no_overwrite_unless_forced(self):
        top_level = tempfile.mkdtemp()
        test_data_13B_014 = self.test_data['13B-014']
        destination = os.path.join(
            top_level, test_data_13B_014['external_name'])
        product_locator = test_data_13B_014['product_locator']

        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        # stick a fake SDM in there so it will fall over
        fake_file = os.path.join(destination, 'ASDM.xml')
        with open(fake_file, 'w') as f:
            f.write('lalalalalala')
        self.assertTrue(os.path.exists(fake_file))
        self.assertFalse(0 == os.path.getsize(fake_file))

        parser = get_arg_parser()

        args = ['--product-locator', product_locator,
                '--output-dir', top_level, '--sdm-only', '--profile', 'local']
        namespace = parser.parse_args(args)

        # exception should be thrown because one of the files to be retrieved
        # is in the destination dir and we're not forcing overwrite here
        with pytest.raises(SystemExit) as exc:
            yoink = Yoink(namespace, self.settings)
            yoink.run()
        _LOG.info(f'exception thrown:\n\t{exc}')
        exc_code = exc.value.code
        expected = Errors.FILE_EXISTS_ERROR.value
        self.assertEqual(expected, exc_code)
        # TODO: same thing, but overwrite this time

    def test_no_bdfs_with_sdm_only(self):

        report_key = 'VLA_SMALL'
        report_spec = LOCATION_REPORTS[report_key]
        external_name = report_spec['external_name']
        expected_file_count = report_spec['file_count']
        locations_report = get_locations_report(report_key)
        self.assertEqual(expected_file_count, len(locations_report['files']),
                         f'expecting {expected_file_count} files in all')
        umask = os.umask(0o000)
        top_level = tempfile.mkdtemp()
        os.umask(umask)
        product_locator = ProductLocatorLookup(self.db_settings)\
            .look_up_locator_for_ext_name(external_name)

        args = ['--product-locator', product_locator,
                '--output-dir', top_level, '--dry',
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        expected_file_count = self._count_sdms(locations_report)
        yoink = Yoink(namespace, self.settings)
        retrieved = yoink.run()
        self.assertEqual(expected_file_count, len(retrieved),
                         f"expecting {expected_file_count} SDMs "
                         f"from {os.path.basename(report_spec['filename'])}")

        self._confirm_fetch(args, locations_report, retrieved)

    def test_retrieve_from_report_file(self):
        report_key = 'VLA_SMALL'
        report_spec = LOCATION_REPORTS[report_key]
        locations_report = get_locations_report(report_key)
        expected_file_count = self._count_sdms(locations_report)
        umask = os.umask(0o000)
        top_level = tempfile.mkdtemp()
        os.umask(umask)
        target = os.path.join(top_level, 'locations.json')
        location_file = write_locations_file(target, locations_report)
        args = ['--location-file', location_file,
                '--output-dir', top_level,
                '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        yoink = Yoink(namespace, self.settings)
        retrieved = yoink.run()
        self.assertEqual(expected_file_count, len(retrieved),
                         f"expecting {expected_file_count} SDMs from "
                         f"{os.path.basename(report_spec['filename'])}")

    def test_gets_needed_test_data(self):
        self.assertIsNotNone(self.test_data['13B-014'])
        data_13B = self.test_data['13B-014']
        self.assertEqual('13B-014.sb28862036.eb29155786.56782.5720116088',
                         data_13B['external_name'])
        locator = data_13B['product_locator']
        self.assertTrue(locator.startswith('uid://evla/execblock/'))


    # TODO:
    #  def test_retries_failed_retrieval(self):
    #     bad_product_locator = 'foo'
    #     .
    #     .
    #     .

    def _initialize_test_data(self):
        ext_name = '13B-014.sb28862036.eb29155786.56782.5720116088'

        product_locator = ProductLocatorLookup(self.db_settings)\
            .look_up_locator_for_ext_name(ext_name)
        dict13b = {'external_name': ext_name,
                   'product_locator': product_locator}

        to_return = {'13B-014': dict13b}
        return to_return


    def _confirm_fetch(self, args: List,
                       location_report: LocationsReport, retrieved: List):
        match_count = 0
        for file_spec in location_report['files']:
            for file in retrieved:
                if file_spec['relative_path'] == os.path.basename(file):
                    if not '--dry' in args:
                        if file_spec['size'] == os.path.getsize(file):
                            match_count += 1
                    else:
                        match_count += 1
        self.assertEqual(len(retrieved), match_count)

    def _remove_large_files_from_location_report(
            self, locations_in: LocationsReport):
        ''' strip files > 100000 bytes from location report, so we can try
            an actual stream without it taking forever

            :returns: LocationsReport
        '''

        files = locations_in['files']
        files_to_keep = [file for file in files if file['size'] <= 100000]
        locations_out = locations_in.copy()
        locations_out['files'] = [file for file in files_to_keep]
        return locations_out

    def _count_sdms(self, location_report: LocationsReport):
        sdms_found = [f for f in location_report['files']
                      if re.match('.*\.(xml|bin)$', f['relative_path'])]
        return len(sdms_found)

if __name__ == '__main__':
    unittest.main()
