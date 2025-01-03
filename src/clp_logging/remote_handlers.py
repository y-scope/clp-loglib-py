import base64
import boto3
from botocore.exceptions import NoCredentialsError
from clp_logging.handlers import *
import datetime
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

class CLPRemoteHandler(CLPFileHandler):
    """
    Handles CLP file upload and comparison to AWS S3 bucket.
    Configuration of AWS access key is required. Run command `aws configure`
    """

    def __init__(
            self,
            s3_bucket: str,
    ) -> None:
        self.s3_resource: boto3.resources.factory.s3.ServiceResource = boto3.resource("s3")
        self.s3_client: boto3.client = boto3.client("s3")
        self.bucket: str = s3_bucket

        self.log_name: Optional[str] = None
        self.log_path: Optional[Path] = None
        self.remote_folder_path: Optional[str] = None
        self.obj_key: Optional[str] = None

        self.multipart_upload_config: Dict[str, int] = {
            'size': 1024 * 1024 * 5,
            'index': 1,
            'pos': 0,
        }
        self.uploaded_parts: List[Dict[str, int | str]] = []
        self.upload_id: Optional[int] = None

    def _calculate_part_sha256(self, data: bytes) -> str:
        sha256_hash: hashlib.Hash = hashlib.sha256()
        sha256_hash.update(data)
        return base64.b64encode(sha256_hash.digest()).decode('utf-8')

    def _remote_log_naming(self, timestamp: datetime.datetime) -> str:
        new_filename: str
        ext: int = self.log_name.find(".")
        upload_time: str = timestamp.strftime("%Y-%m-%d-%H%M%S")

        if ext != -1:
            new_filename = f'log_{upload_time}{self.log_name[ext:]}'
        else:
            new_filename = f'{upload_time}_{self.log_name}'
        new_filename = f"{self.remote_folder_path}/{new_filename}"
        return new_filename

    def _upload_part(self, upload_id) -> Dict[str, int | str]:
        upload_data: bytes
        # Read the latest file
        try:
            with open(self.log_path, 'rb') as file:
                file.seek(self.multipart_upload_config['pos'])
                upload_data = file.read(self.multipart_upload_config['size'])
        except FileNotFoundError as e:
            raise FileNotFoundError(f'The log file {self.log_path} cannot be found: {e}') from e
        except IOError as e:
            raise IOError(f'IO Error occurred while reading file {self.log_path}: {e}') from e
        except Exception as e:
            raise e

        try:
            sha256_checksum: str = self._calculate_part_sha256(upload_data)
            response: Dict[str, Any] = self.s3_client.upload_part(
                Bucket=self.bucket,
                Key=self.obj_key,
                Body=upload_data,
                PartNumber=self.multipart_upload_config['index'],
                UploadId=upload_id,
                ChecksumSHA256=sha256_checksum
            )
            print(f'Uploaded Part {self.multipart_upload_config["index"]}')
            print(response)

            # Store both ETag and SHA256 for validation
            return {
                'PartNumber': self.multipart_upload_config['index'],
                'ETag': response['ETag'],
                'ChecksumSHA256': response['ChecksumSHA256'],
            }
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=upload_id
            )
            raise Exception(f'Error occurred during multipart upload on part {self.multipart_upload_config["index"]}: {e}') from e

    def get_obj_key(self) -> str:
        return self.obj_key

    def set_obj_key(self, obj_key) -> None:
        self.obj_key = obj_key

    def initiate_upload(self, log_path: Path) -> None:
        self.log_path: Path = log_path
        self.log_name: str = log_path.name
        timestamp: datetime.datetime = datetime.datetime.now()
        self.remote_folder_path: str = f'logs/{timestamp.year}/{timestamp.month}/{timestamp.day}'

        self.obj_key: str = self._remote_log_naming(timestamp)
        create_ret: Dict[str, Any] = self.s3_client.create_multipart_upload(Bucket=self.bucket, Key=self.obj_key,
                                                                            ChecksumAlgorithm='SHA256')
        self.upload_id = create_ret['UploadId']

    def multipart_upload(self) -> None:
        # Upload initiation is required before upload
        if not self.upload_id:
            raise Exception('No upload process.')

        file_size: int = self.log_path.stat().st_size
        print(file_size)
        try:
            while (
                file_size - self.multipart_upload_config['pos']
                >= self.multipart_upload_config['size']
            ):
                upload_status: Dict[str, int | str] = self._upload_part(self.upload_id)
                print(upload_status)
                self.multipart_upload_config['index'] += 1
                self.multipart_upload_config['pos'] += self.multipart_upload_config['size']
                self.uploaded_parts.append(upload_status)

                # AWS S3 limits object part count to 10000
                if self.multipart_upload_config['index'] > 10000:
                    break

        except NoCredentialsError as e:
            raise e
        except Exception as e:
            self.s3_client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.obj_key, UploadId=self.upload_id
            )
            raise e
