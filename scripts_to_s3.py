import datetime
import logging
import os
from typing import Dict, Set

import boto3
import pytz

logging.basicConfig(level=logging.INFO)


class S3FileManager:
    def __init__(self, bucket_name: str, folder: str) -> None:
        """
        Initialize the S3FileManager.

        Args:
            bucket_name: The name of the S3 bucket.
            folder: The local folder path.
        """
        self.bucket_name = bucket_name
        self.folder = folder
        self.timezone = pytz.timezone("America/New_York")
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def manage_files(self) -> None:
        """
        Manage files between local folder and S3 bucket.
        Uploads new/modified files to S3 and deletes files that are not present locally.
        """
        s3_files = self._get_s3_files()

        local_files = self._get_local_files()

        files_to_upload = self._get_files_to_upload(
            local_files=local_files, s3_files=s3_files
        )
        logging.info(f"Files to upload: {files_to_upload}")

        files_to_delete = s3_files.keys() - local_files.keys()
        logging.info(f"Files to delete: {files_to_delete}")

        self._upload_files(local_files)
        self._delete_files(files_to_delete)

    def _get_files_to_upload(
        self,
        local_files: Dict[str, datetime.datetime],
        s3_files: Dict[str, datetime.datetime],
    ) -> Set[str]:
        """
        Find files that need to be uploaded to S3.

        Args:
            local_files: A set of tuples containing file paths and last modified time.
            s3_files: A set of tuples containing file keys and last modified time.

        Returns:
            A set of file paths that need to be uploaded to S3.
        """
        files_to_upload = set()

        for local_file, local_modified_time in local_files.items():
            if local_file not in s3_files.keys():
                files_to_upload.add(local_file)
            else:
                s3_modified_time = s3_files[local_file]
                if local_modified_time.astimezone(
                    self.timezone
                ) > s3_modified_time.astimezone(self.timezone):
                    print(
                        local_modified_time,
                        s3_modified_time,
                        local_modified_time.astimezone(self.timezone),
                        s3_modified_time.astimezone(self.timezone),
                    )
                    files_to_upload.add(local_file)

        return files_to_upload

    def _get_s3_files(self) -> Dict[str, datetime.datetime]:
        """
        Retrieve the list of .py files present in the S3 bucket along with their last modified time.

        Returns:
            A set of tuples containing file keys and last modified time.
        """
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=self.folder
        )
        s3_files = {}
        if "Contents" in response:
            s3_files = {
                obj["Key"]: obj["LastModified"]
                for obj in response["Contents"]
                if obj["Key"].endswith(".py")
            }

        return s3_files

    def _get_local_files(self) -> Dict[str, datetime.datetime]:
        """
        Get the list of .py files from the local folder along with their last modified time.

        Returns:
            A set of tuples containing file paths and last modified time.
        """
        local_files = {}

        for file_name in os.listdir(self.folder):
            if file_name.endswith(".py"):
                file_path = os.path.join(self.folder, file_name)
                last_modified_time = datetime.datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                )
                local_files[file_path] = last_modified_time

        return local_files

    def _upload_files(self, files: Set[str]) -> None:
        """
        Upload specified files to the S3 bucket.

        Args:
            files: Set of file paths to upload.
        """
        for file in files:
            self.s3_client.upload_file(file, self.bucket_name, file)
            logging.info(f"Uploaded file to S3: {file}")

    def _delete_files(self, files: Set[str]) -> None:
        """
        Delete specified files from the S3 bucket.

        Args:
            files: Set of file keys to delete.
        """
        for file in files:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=file)
            logging.info(f"Deleted file from S3: {file}")


# TODO: Napisati testove unit i integracione
# TODO: Dodati automatski pytest u github actions
if __name__ == "__main__":
    bucket_name = "mlops-task"
    folder = "scripts"
    s3_file_manager = S3FileManager(bucket_name, folder)
    s3_file_manager.manage_files()
