""" File retriever unit tests """

import http
import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import List

import pytest

from yoink.errors import NGASServiceErrorException, \
    SizeMismatchException, MissingSettingsException, FileErrorException
from yoink.file_retrievers import NGASFileRetriever
from yoink.utilities import get_capo_settings, \
    get_metadata_db_settings, ProductLocatorLookup, get_arg_parser, \
    path_is_accessible, Cluster, RetrievalMode, MAX_TRIES, \
    FlexLogger

_A_FEW_TRIES = 3

class RetrieverTestCase(unittest.TestCase):
    """
    Tests for product retrieval
    """

    @classmethod
    def setUpClass(cls) -> None:
        ''' do this before running tests '''

        # local profile is required to force streaming
        cls.profile = 'local'

        cls.settings = get_capo_settings(cls.profile)
        cls.db_settings = get_metadata_db_settings(cls.profile)
        cls.test_data = cls._initialize_13b_014_file_spec(cls)

    @classmethod
    def setUp(cls) -> None:
        umask = os.umask(0o000)
        cls.top_level = tempfile.mkdtemp()
        cls._LOG = FlexLogger(cls.__class__.__name__, cls.top_level)
        os.umask(umask)

    def test_retriever_accepts_valid_partial_args(self):
        file_spec = self.test_data['files'][1]

        parser = get_arg_parser()
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level,
                '--sdm-only', '--profile', self.profile]
        namespace = parser.parse_args(args)

        server = file_spec['server']['server']

        retriever = NGASFileRetriever(namespace, self._LOG)
        retrieved = retriever.retrieve(server, RetrievalMode.STREAM, file_spec)
        self.assertTrue(os.path.exists(retrieved), 'retrieved file must exist')
        self.assertTrue(os.path.isfile(retrieved),
                        'retrieved file must be a regular file')
        self.assertEqual(file_spec['size'], os.path.getsize(retrieved),
                         f"expecting {os.path.basename(retrieved)} to be "
                         f"{file_spec['size']} bytes")

    def test_throws_file_exists_error_if_overwrite_not_forced(self):
        ''' if the --force flag is supplied, any file that exists at the
            destination should NOT be retrieved; throw error instead
        '''
        file_spec = self.test_data['files'][0]
        destination = os.path.join(self.top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        # stick a fake SDM in there so it will fall over
        fake_file = os.path.join(destination, file_spec['relative_path'])
        with open(fake_file, 'w') as to_write:
            to_write.write('as if!')
        self.assertTrue(os.path.exists(fake_file))
        self.assertFalse(os.path.getsize(fake_file) == 0)

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', destination, '--sdm-only',
                '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)
        server = file_spec['server']['server']
        retriever = NGASFileRetriever(namespace, self._LOG)

    # exception should be thrown because one of the files to be retrieved
        # is in the destination dir
        with pytest.raises(FileExistsError):
            retriever.retrieve(server, RetrievalMode.STREAM, file_spec)

    def test_nothing_retrieved_in_dry_run(self):
        file_spec = self.test_data['files'][0]
        destination = os.path.join(self.top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--dry', '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retriever = NGASFileRetriever(namespace, self._LOG)
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        retriever.retrieve(server, RetrievalMode.STREAM, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')
        self.assertTrue(retriever.fetch_attempted,
                        'streaming_fetch() should have been entered')

    def test_verbose_log_has_debug_messages(self):
        file_spec = self.test_data['files'][0]
        destination = os.path.join(self.top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', destination,
                '--profile', self.profile, '--verbose']
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        logger = FlexLogger(self.__class__.__name__, self.top_level, True)
        retriever = NGASFileRetriever(namespace, logger)
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        retriever.retrieve(
            server, RetrievalMode.STREAM, file_spec)

        files_retrieved = list()
        for root, dirnames, filenames in os.walk(destination):
            if dirnames:
                subdir = os.path.join(root, dirnames[0])
            else:
                subdir = root
            to_add = [file for file in filenames
                      if not str(file).endswith('.log')]
            for filename in to_add:
                files_retrieved.append(os.path.join(subdir, filename))

        self.assertEqual(1, len(files_retrieved),
                         'one file should have been retrieved')
        self.assertEqual(7566, os.path.getsize(to_be_retrieved),
                         f'expecting {to_be_retrieved} to be 7566 bytes')

        self.assertTrue(os.path.isfile(retriever.logfile),
                        f'expecting log file {os.path.basename(retriever.logfile)}')
        self.assertNotEqual(0, os.path.getsize(retriever.logfile),
                            'log file should not be empty')

    def test_non_verbose_log_empty(self):
        file_spec = self.test_data['files'][0]
        destination = os.path.join(self.top_level, file_spec['external_name'])
        Path(destination).mkdir(parents=True, exist_ok=True)
        self.assertTrue(os.path.isdir(destination))

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', destination,
                '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retriever = NGASFileRetriever(namespace, self._LOG)
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        retriever.retrieve(
            server, RetrievalMode.STREAM, file_spec)

        files_retrieved = list()
        for root, dirnames, filenames in os.walk(destination):
            if dirnames:
                subdir = os.path.join(root, dirnames[0])
            else:
                subdir = root
            to_add = [file for file in filenames if not str(
                file).endswith('.log')]
            for filename in to_add:
                files_retrieved.append(os.path.join(subdir, filename))
        self.assertEqual(1, len(files_retrieved),
                         'one file should have been retrieved')
        self.assertEqual(7566, os.path.getsize(to_be_retrieved),
                         f'expecting {to_be_retrieved} to be 7566 bytes')

        logfile = self._LOG.logfile
        self.assertTrue(os.path.isfile(logfile),
                        f'expecting log file {os.path.basename(logfile)}')
        self.assertEqual(0, os.path.getsize(logfile),
                         'log file should be empty')

    def test_stream_inaccessible_destination_throws_file_error(self):
        file_spec = self.test_data['files'][0]

        # make directory read-only
        os.chmod(self.top_level, 0o444)
        self.assertFalse(path_is_accessible(self.top_level),
                         'output directory should not be accessible')

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        with pytest.raises(FileErrorException):
            NGASFileRetriever(namespace, self._LOG).retrieve(
                file_spec['server']['server'], RetrievalMode.STREAM, file_spec)

        # make directory writeable again so it'll get deleted
        os.chmod(self.top_level, 0o555)

    def test_stream_bad_destination_throws_service_error(self):
        top_level = 'foo'
        file_spec = self.test_data['files'][0]
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.COPY
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])

        with pytest.raises(NGASServiceErrorException) as s_ex:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')
        details = s_ex.value.args[0]
        self.assertEqual(http.HTTPStatus.BAD_REQUEST, details['status_code'])

    def test_stream_no_data_throws_missing_setting(self):
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(self.top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(MissingSettingsException):
            retriever.retrieve(server, retrieve_method, {})
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')

    def test_wrong_size_throws_size_mismatch(self):
        file_spec = self.test_data['files'][0]
        # give it the wrong size to cause a SizeMismatchException
        file_spec['size'] = 42

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(self.top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(SizeMismatchException):
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved))

    def test_stream_fetch_failure_throws_missing_setting(self):
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(self.top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination,
                                       'not_the_droids_youre_looking_for')
        with pytest.raises(MissingSettingsException):
            retriever.retrieve(server, retrieve_method, {})
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')

    def test_stream_cannot_connect_throws_service_error(self):
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = 'foo'
        retrieve_method = RetrievalMode.STREAM
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(self.top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(NGASServiceErrorException):
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')

    def test_local_copy_attempt_raises_service_error(self):
        ''' we can expect a copy ALWAYS to fail,
            because NGAS can't write to a local destination
        '''
        file_spec = self.test_data['files'][0]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retrieve_method = RetrievalMode.COPY
        retriever = NGASFileRetriever(namespace, self._LOG)

        destination = os.path.join(self.top_level, file_spec['external_name'])
        to_be_retrieved = os.path.join(destination, file_spec['relative_path'])
        with pytest.raises(NGASServiceErrorException) as s_ex:
            retriever.retrieve(server, retrieve_method, file_spec)
        self.assertFalse(os.path.exists(to_be_retrieved),
                         'nothing should have been retrieved')
        details = s_ex.value.args[0]
        self.assertEqual(http.HTTPStatus.BAD_REQUEST, details['status_code'])

    def test_no_retries_on_success(self):
        self.assertTrue(path_is_accessible(self.top_level))
        file_spec = self.test_data['files'][1]

        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]
        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retriever = NGASFileRetriever(namespace, self._LOG)
        destination = os.path.join(self.top_level, file_spec['relative_path'])
        retriever.retrieve(server, RetrievalMode.STREAM, file_spec)
        self.assertTrue(os.path.exists(destination))
        self.assertEqual(1, retriever.num_tries)

    def test_max_retries_on_failure(self):
        file_spec = self.test_data['files'][0].copy()

        # give it an invalid version
        file_spec['version'] = 126
        args = ['--product-locator', file_spec['product_locator'],
                '--output-dir', self.top_level, '--profile', self.profile]

        namespace = get_arg_parser().parse_args(args)

        server = file_spec['server']['server']
        retriever = NGASFileRetriever(namespace, self._LOG)

        with pytest.raises(Exception):
            retriever.retrieve(server, RetrievalMode.STREAM, file_spec)
        self.assertEqual(MAX_TRIES, retriever.num_tries)


    # --------------------------------------------------------------------------
    #
    #        U T I L I T I E S
    #
    # --------------------------------------------------------------------------

    @staticmethod
    def do_something_wrong(args: List):
        raise NGASServiceErrorException(args)

    @staticmethod
    def do_something_a_few_times(args: List):
        return int(args[0])

    def _initialize_13b_014_file_spec(self):
        ext_name = '13B-014.sb29151475.eb29223944.56810.442529050924'
        product_locator = ProductLocatorLookup(self.db_settings)\
            .look_up_locator_for_ext_name(ext_name)
        server = {'server': 'nmngas03.aoc.nrao.edu:7777',
                  'location': 'somewhere_else',
                  'cluster': Cluster.DSOC}

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
            },
            {
                'ngas_file_id': 'uid___evla_sdm_X1401705435288.sdm',
                'external_name': ext_name,
                'subdirectory' : None,
                'product_locator': product_locator,
                'relative_path': 'Antenna.xml',
                'checksum': '1014682026',
                'version': 1,
                'size': 10505,
                'server': server,
            }

        ]
        return {'files': files}

    def _get_test_files(self):
        ''' for the retriever interface: return each location report's
            information -minus- server
        '''
        files = []
        for location_report in self.test_data[0].files:
            file = location_report.deepcopy()
            del file['server']
            files.append(file)
        return files

    def _get_test_filespec(self, target_filename):
        ''' grab location report data for just the specified file '''
        test_data_dir = os.path.join(os.curdir, 'data')
        self.assertTrue(os.path.isdir(test_data_dir))

        report_file = os.path.join(test_data_dir, 'VLA_SMALL_EB.json')
        self.assertTrue(os.path.isfile(report_file))
        with open(report_file, 'r') as content:
            locations_report = json.loads(content.read())
        for file_spec in locations_report['files']:
            if target_filename == file_spec['relative_path']:
                return file_spec

        return None

if __name__ == '__main__':
    unittest.main()
