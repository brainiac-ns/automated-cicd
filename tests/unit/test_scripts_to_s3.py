import datetime
import unittest
from unittest import mock
from scripts_to_s3 import S3FileManager


class TestScriptsToS3(unittest.TestCase):
    @mock.patch("importlib.util.spec_from_file_location")
    def test_check_function_runnable(self, mock_spec_from_file_location):
        script_path = "tests/unit/test_data/script.py"

        mock_spec = mock_spec_from_file_location.return_value
        mock_module = mock_spec.loader.create_module.return_value
        mock_spec.loader.exec_module.return_value = mock_module
        mock_module.run = lambda x: None

        s3_file_manager = S3FileManager("bucket", "folder")
        result = s3_file_manager._check_if_function_is_runnable(script_path)

        self.assertTrue(result)

    @mock.patch("importlib.util.spec_from_file_location")
    def test_check_if_function_is_runnable_exception(
        self, mock_spec_from_file_location
    ):
        script_path = "tests/unit/test_data/script.py"

        mock_spec = mock_spec_from_file_location.return_value
        mock_spec.loader.exec_module.side_effect = Exception("Module execution error")

        s3_file_manager = S3FileManager("bucket", "folder")
        result = s3_file_manager._check_if_function_is_runnable(script_path)

        self.assertFalse(result)

    @mock.patch("os.path.getmtime")
    def test_get_local_files(self, mock_getmtime):
        mock_getmtime.return_value = 1625256000  # Unix timestamp for a specific date

        s3_file_manager = S3FileManager(bucket_name="", folder="tests/unit/test_data")
        result = s3_file_manager._get_local_files()

        expected_files = {
            "tests/unit/test_data/script.py": datetime.datetime.fromtimestamp(
                1625256000
            ),
        }

        self.assertEqual(result, expected_files)

    @mock.patch("scripts_to_s3.S3FileManager._check_if_function_is_runnable")
    def test_get_files_to_upload(self, mock_check_if_function_is_runnable):
        local_files = {
            "file1.py": datetime.datetime(2022, 1, 1),
            "file2.py": datetime.datetime(2022, 1, 2),
            "file3.py": datetime.datetime(2022, 1, 3),
        }

        s3_files = {
            "file1.py": datetime.datetime(2022, 1, 1),
            "file2.py": datetime.datetime(2022, 1, 1),
        }

        s3_file_manager = S3FileManager(bucket_name="", folder="tests/unit/test_data")
        mock_check_if_function_is_runnable.return_value = True

        result = s3_file_manager._get_files_to_upload(local_files, s3_files)

        expected_files = {"file2.py", "file3.py"}
        self.assertEqual(result, expected_files)
