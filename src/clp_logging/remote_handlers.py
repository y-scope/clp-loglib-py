import boto3
from botocore.exceptions import NoCredentialsError
from clp_logging.handlers import *
import datetime
from pathlib import Path
from typing import Optional, Any, List

class CLPRemoteHandler():
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
