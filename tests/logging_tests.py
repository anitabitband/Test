''' Tests for FlexLogger '''
import os
import tempfile
import unittest
from pathlib import Path

import pytest

from yoink.utilities import FlexLogger


class FlexLoggerTestCase(unittest.TestCase):
    ''' FlexLogger regression tests '''

    @classmethod
    def setUp(cls) -> None:
        umask = os.umask(0o000)
        cls.top_level = tempfile.mkdtemp()
        os.umask(umask)

    def test_errors_are_written_to_file(self):
        logger = FlexLogger(self.__class__.__name__, self.top_level)
        logger.error('Help! Help! The sky is falling!')
        logfile = logger.logfile
        self.assertTrue(os.path.exists(logfile),
                        f'there should be a log file: {logfile}')
        self.assertNotEqual(0, os.path.getsize(logfile),
                            'there should be an error in the log')

    def test_debugs_are_written_to_file_only_if_verbose(self):
        logger = FlexLogger(self.__class__.__name__, self.top_level)
        logger.debug('I am the walrus')
        logfile = logger.logfile
        self.assertTrue(os.path.exists(logfile),
                        f'there should be a log file: {logfile}')
        self.assertEqual(0, os.path.getsize(logfile),
                         'log should be empty')
        os.rename(logfile, os.path.join(self.top_level,
                                        'non_verbose_debug.log'))

        logger = FlexLogger(self.__class__.__name__, self.top_level, True)
        logger.debug('Is it time for lunch yet?')
        logfile = logger.logfile
        self.assertNotEqual(0, os.path.getsize(logfile),
                            'there should be a message in the log now')

    def test_warnings_written_to_file_even_not_verbose(self):
        logger = FlexLogger(self.__class__.__name__, self.top_level)
        logger.warning('For the last time....')
        logfile = logger.logfile
        self.assertTrue(os.path.exists(logfile),
                        f'there should be a log file: {logfile}')
        self.assertNotEqual(0, os.path.getsize(logfile),
                            'there should be a warning in the log')

    def test_init_attempt_throws_fnf_if_dir_not_found(self):
        with pytest.raises(FileNotFoundError):
            FlexLogger(self.__class__.__name__, Path('/foo'))

    def test_init_attempt_throws_type_err_if_dir_not_found(self):
        with pytest.raises(TypeError):
            FlexLogger(self.__class__.__name__, None)

    def test_init_attempt_fails_if_dir_inaccessible(self):
        test_dir = tempfile.mkdtemp()
        # make directory non-writable
        os.chmod(test_dir, 0o444)

        with pytest.raises(PermissionError):
            FlexLogger(self.__class__.__name__, test_dir)

        # make directory writeable again so it'll get deleted
        os.chmod(self.top_level, 0o555)


if __name__ == '__main__':
    unittest.main()
