import logging
import os
import tempfile
import unittest
from pathlib import Path

import pytest
from yoink.commands import Yoink
from yoink.errors import FileErrorException, Errors
from yoink.utilities import get_capo_settings, get_arg_parser, ProductLocatorLookup, get_metadata_db_settings

LOG_FORMAT = "%(name)s.%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

class YoinkTestCase(unittest.TestCase):
    """ IMPORTANT NOTE: we CANNOT retrieve by copy if we don't have access to a location to which NGAS can write,
        e.g, lustre. Therefore, any test that involves -actual- retrieval of files must be by streaming,
        to ensure which we must use a Capo profile in which the execution site is -not- DSOC or NAASC.
        The reason is this algorithm used in LocationsReport:

            for f in files_report['files']:
                if f['server']['cluster'] == 'DSOC' and \
                        f['server']['location'] == self.settings['execution_site']:
                    f['server']['retrieve_method'] = 'copy'
                else:
                    f['server']['retrieve_method'] = 'stream'


        Be sure to have on the test system a local profile (local.properties) that meets these criteria:

        - edu.nrao.archive.workflow.config.StartupSettings.temporaryDataDirectory
            pointing to a locally writable temp dir, e.g., /var/tmp
        - edu.nrao.archive.workflow.config.DeliverySettings.hostname must point to local computer
        - execution_site must NOT be DSOC or NAASC


    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = 'local'
        cls.settings = get_capo_settings(cls.profile)
        cls.db_settings = get_metadata_db_settings(cls.profile)
        cls.test_data = cls._initialize_test_data(cls)

    @unittest.skip('test_can_stream_exec_block')
    def test_can_stream_exec_block(self):
        parser = get_arg_parser()

        product_locator = self.test_data['13B-014']['product_locator']
        args = ['--product-locator', product_locator, '--output-dir','~/Downloads/', '--sdm-only', '--profile', self.profile]
        namespace = parser.parse_args(args)
        yoink = Yoink(namespace, self.settings)
        # TODO: too slow b/c of honkin' big SysPower.bin; mock or sth
        yoink.run()
        # TODO: does location exist and have 31 files?


    def test_expected_copy_failure(self):
        test_data_13B_014 = self.test_data['13B-014']

        top_level = tempfile.mkdtemp()
        destination = os.path.join(top_level, test_data_13B_014['external_name'])

        umask = os.umask(0o000)
        Path(destination).mkdir(parents=True, exist_ok=True)
        os.umask(umask)

        product_locator = test_data_13B_014['product_locator']

        # use site from non-local profile to guarantee copy attempt
        self.settings['execution_site'] = 'DSOC'
        args = ['--product-locator', product_locator,
                '--output-dir', top_level, '--sdm-only', '--profile', self.profile, '--verbose']
        parser = get_arg_parser()
        namespace = parser.parse_args(args)
        yoink = Yoink(namespace, self.settings)
        servers_report = yoink.servers_report
        for server in servers_report:
            entry = servers_report[server]
            self.assertTrue(entry['retrieve_method'] == 'copy')
        with pytest.raises(SystemExit) as s_ex:
            yoink.run()
        exc_code = s_ex.value.code
        expected = Errors.NGAS_SERVICE_ERROR.value
        self.assertEqual(expected, exc_code)

    def test_no_overwrite_unless_forced(self):
        top_level = tempfile.mkdtemp()
        # external_name = self.test_data['13B-014'].
        test_data_13B_014 = self.test_data['13B-014']
        destination = os.path.join(top_level, test_data_13B_014['external_name'])
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

        # exception should be thrown because one of the files to be retrieved is in the destination dir
        # and we're not forcing overwrite here
        with pytest.raises(SystemExit) as exc:
            yoink = Yoink(namespace, self.settings)
            yoink.run()
        _LOG.info(f'exception thrown:\n\t{exc}')
        exc_code = exc.value.code
        expected = Errors.FILE_EXISTS_ERROR.value
        self.assertEqual(expected, exc_code)

    def _initialize_test_data(self):
        ext_name = '13B-014.sb28862036.eb29155786.56782.5720116088'

        product_locator = ProductLocatorLookup(self.db_settings).look_up_locator_for_ext_name(ext_name)
        dict13b = {'external_name': ext_name, 'product_locator': product_locator}

        to_return = {'13B-014': dict13b}

        return to_return

    def test_gets_needed_test_data(self):
        self.assertIsNotNone(self.test_data['13B-014'])
        data_13B = self.test_data['13B-014']
        self.assertEqual('13B-014.sb28862036.eb29155786.56782.5720116088', data_13B['external_name'])
        locator = data_13B['product_locator']
        self.assertTrue(locator.startswith('uid://evla/execblock/'))

    # TODO:
    #  def test_retries_failed_retrieval(self):
    #     bad_product_locator = 'foo'
    #     .
    #     .
    #     .


if __name__ == '__main__':
    unittest.main()
