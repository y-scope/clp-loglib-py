import boto3
from botocore.exceptions import NoCredentialsError
from clp_logging.handlers import *
from pathlib import Path
from typing import Optional

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

    def get_obj_key(self) -> str:
        return self.obj_key

    def set_obj_key(self, obj_key) -> None:
        self.obj_key = obj_key
