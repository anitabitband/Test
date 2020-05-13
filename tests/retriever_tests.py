import http
import json
import logging
import logging
import os
import tempfile
import unittest
from pathlib import Path
from typing import List

import pytest

from yoink.errors import Errors, NGASServiceErrorException
from yoink.file_retrievers import NGASFileRetriever
from yoink.utilities import Retryer, get_capo_settings, get_metadata_db_settings, ProductLocatorLookup, get_arg_parser, \
    path_is_accessible, Cluster, RetrievalMode

LOG_FORMAT = "%(name)s.%(module)s.%(funcName)s, %(lineno)d: %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)
_LOG = logging.getLogger(__name__)

_MAX_TRIES = 5
_A_FEW_TRIES = 3
_SLEEP_INTERVAL_SECONDS = 1

# TODO after implementing no-BDFs test: add retry

class RetrieverTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # local profile is required to force streaming
        cls.profile = 'local'
        cls.settings = get_capo_settings(cls.profile)
        cls.db_settings = get_metadata_db_settings(cls.profile)
        cls.test_data = cls._initialize_13B_014_test_data(cls)

    # TODO: same test with multiple files, mock copy
    def test_retriever_accepts_valid_partial_args(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]

        parser = get_arg_parser()
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--sdm-only', '--profile', self.profile]
        namespace = parser.parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)
        retrieved = retriever.retrieve(server, retrieve_method, file_spec)
        self.assertTrue(os.path.exists(retrieved), 'retrieved file must exist')
        self.assertTrue(os.path.isfile(retrieved), 'retrieved file must be a regular file')
        self.assertEqual(file_spec['size'], os.path.getsize(retrieved),
                         f"expecting {os.path.basename(retrieved)} to be {file_spec['size']} bytes")

    # TODO: find a way to test that overwrite DOES occur with --forced, but WITHOUT actually fetching
    # TODO: same test with multiple files, mock copy
    def test_no_overwrite_if_not_forced(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]
        destination = os.path.join(top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        # stick a fake SDM in there so it will fall over
        fake_file = os.path.join(destination, file_spec['relative_path'])
        with open(fake_file, 'w') as f:
            f.write('as if!')
        self.assertTrue(os.path.exists(fake_file))
        self.assertFalse(0 == os.path.getsize(fake_file))

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', destination, '--sdm-only', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)
        # to_be_retrieved = os.path.join(destination, file_spec['relative_path'])

        # exception should be thrown because one of the files to be retrieved is in the destination dir
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, file_spec)
        exc_code = exc.value.code
        expected = Errors.FILE_EXISTS_ERROR.value
        self.assertEqual(expected, exc_code)

    @unittest.skip('test_overwrite: not yet implemented')
    def test_overwrite(self):
        # TODO:
        file_spec = self._get_test_filespec('SysPower.bin')

    # TODO: same test with mock copy
    def test_nothing_retrieved_in_dry_run(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]
        destination = os.path.join(top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--dry', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        self.assertTrue(retriever.fetch_attempted, 'streaming_fetch() should have been entered')

    def test_stream_inaccessible_destination(self):
        top_level = tempfile.mkdtemp()

        file_spec = self.test_data['files'][0]
        # make directory read-only
        os.chmod(top_level, 0o444)
        self.assertFalse(path_is_accessible(top_level), 'output directory should not be accessible')

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        exc_code = exc.value.code
        expected = Errors.FILE_ERROR.value
        self.assertEqual(expected, exc_code)

        # make directory writeable again so it'll get deleted
        os.chmod(top_level, 0o555)

    def test_stream_bad_destination(self):
        top_level = 'foo'
        file_spec = self.test_data['files'][0]
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.COPY
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])

        with pytest.raises(NGASServiceErrorException) as s_ex:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        details = s_ex.value.args[0]
        self.assertEqual(http.HTTPStatus.BAD_REQUEST, details['status_code'])

    def test_stream_no_data(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, {})
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        exc_code = exc.value.code
        expected = Errors.MISSING_SETTING.value
        self.assertEqual(expected, exc_code)

    def test_size_mismatch(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]
        # give it the wrong size to cause a SizeMismatchException
        file_spec['size'] = 42

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        exc_code = exc.value.code
        expected = Errors.SIZE_MISMATCH.value
        self.assertEqual(expected, exc_code)

    def test_stream_fetch_failure(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, 'not_the_droids_youre_looking_for')
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, {})
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        exc_code = exc.value.code
        expected = Errors.MISSING_SETTING.value
        self.assertEqual(expected, exc_code)

    def test_stream_cannot_connect(self):
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = 'foo'
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(SystemExit) as exc:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        exc_code = exc.value.code
        expected = Errors.NGAS_SERVICE_ERROR.value
        self.assertEqual(expected, exc_code)

    def test_copy_attempt(self):
        ''' we can expect a copy ALWAYS to fail, because NGAS can't write to a local destination
        '''
        top_level = tempfile.mkdtemp()
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.COPY
        retriever = NGASFileRetriever(namespace)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(NGASServiceErrorException) as s_ex:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved), 'nothing should have been retrieved')
        details = s_ex.value.args[0]
        self.assertEqual(http.HTTPStatus.BAD_REQUEST, details['status_code'])

    def test_retryer_does_retry(self):
        retryer = Retryer(self.do_something_once, _MAX_TRIES, _SLEEP_INTERVAL_SECONDS)
        self.assertEqual(1, retryer.retry('I am doing something once'), 'expecting 1 retry')

        num_tries_expected = _MAX_TRIES
        num_tries_actual   = 0
        retryer = Retryer(self.do_something_wrong, _MAX_TRIES, _SLEEP_INTERVAL_SECONDS)
        while num_tries_actual < num_tries_expected:
            try:
                num_tries_actual += retryer.retry('This will never work')
            except AssertionError as a_exc:
                _LOG.info(f'{a_exc} after {num_tries_actual} tries; going to give it another whirl')

        self.assertEqual(num_tries_expected, num_tries_actual, 'expecting maximum number of retries')

    def do_something_once(self, args: List):
        _LOG.info(f'doing something once with args: {args}')
        return 1

    def do_something_wrong(self, args: List):
        _LOG.info('failing at something')
        raise SystemExit(Exception(args))

    def do_something_a_few_times(self, num_tries: int):
        _LOG.info(f'doing something a few times; this is iteration #{num_tries}')
        if _A_FEW_TRIES > num_tries:
            raise Exception(num_tries)
        return num_tries

    def _initialize_13B_014_test_data(self):
        ext_name = '13B-014.sb29151475.eb29223944.56810.442529050924'
        product_locator = ProductLocatorLookup(self.db_settings).look_up_locator_for_ext_name(ext_name)
        server = {'server': 'nmngas03.aoc.nrao.edu:7777', 'location': 'somewhere_else', 'cluster': Cluster.DSOC}

        # IRL there will be a -list- of files
        files = [
            {
                'ngas_file_id': 'uid___evla_sdm_X1401705435287.sdm',
                'external_name': ext_name,
                'subdirectory' : None,
                'product_locator': product_locator,
                'relative_path': 'ASDM.xml',
                'checksum': '-2040810571',
                'version': 1,
                'size': 7566,
                'server': server,
            }
        ]
        return {'files': files}

    def _get_test_files(self):
        ''' for the retriever interface: return each location report's information -minus- server
        '''
        files = []
        for location_report in self.test_data[0].files:
            file = location_report.deepcopy()
            del file['server']
            files.append(file)
        return files

    def _get_test_filespec(self, target_filename):
        test_data_dir = os.path.join(os.curdir, 'data')
        self.assertTrue(os.path.isdir(test_data_dir))

        report_file = os.path.join(test_data_dir, 'VLA_SMALL_EB.json')
        self.assertTrue(os.path.isfile(report_file))
        with open(report_file, 'r') as content:
            locations_report = json.loads(content.read())
        for file_spec in locations_report['files']:
            if target_filename == file_spec['relative_path']:
                return file_spec


if __name__ == '__main__':
    unittest.main()
