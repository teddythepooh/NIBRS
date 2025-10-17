import os
import boto3
from botocore.client import BaseClient
from botocore.exceptions import UnknownServiceError
import pandas as pd
import polars as pl
from io import BytesIO, StringIO

# https://stackoverflow.com/questions/53416226/how-to-write-parquet-file-from-pandas-dataframe-in-s3-in-python
# https://stackoverflow.com/questions/75115246/with-python-is-there-a-way-to-load-a-polars-dataframe-directly-into-an-s3-bucke

import warnings
warnings.filterwarnings(
    "ignore", 
    message = "Polars found a filename" #https://stackoverflow.com/questions/75690784/polars-for-python-how-to-get-rid-of-ensure-you-pass-a-path-to-the-file-instead
    )

class AWSBase:
    '''
    Instantiates boto3 client. If no credentials are passed, region_name; aws_access_key_id; 
    and aws_secret_access_key are pulled from environment variables of the same name.
    '''
    def __init__(self, 
                 region_name: str = os.environ["region_name"], 
                 aws_access_key_id: str = os.environ["aws_access_key_id"], 
                 aws_secret_access_key: str = os.environ["aws_secret_access_key"]):
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def _create_credentials_dict(self) -> dict:
        credentials = {
            "region_name": self.region_name,
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key
        }
        
        return credentials
        
    def _create_client(self, service: str) -> BaseClient:
        try:
            client = boto3.client(service, **self._create_credentials_dict())
            return client
        except UnknownServiceError:
            raise ValueError(f"Service '{service}' is invalid. AWS client could not be created.")

class AmazonS3(AWSBase):
    def view_objects_in_s3_bucket(self, bucket_name: str, view_only: bool = False) -> list:
        '''
        view_only: Returns all objects from an S3 bucket as a list if True, otherwise the object 
        names and file sizes are simply printed.
        '''
        full_dict = self._create_client("s3").list_objects_v2(Bucket = bucket_name)
        
        try:
            if view_only:
                for object in full_dict["Contents"]:
                    file_name = object["Key"]
                    file_size_as_mb = round(object["Size"] / (1000 * 1000), 2)
                    print(f"File Name: {file_name}, Size: {file_size_as_mb} MB")
            else:
                return [object["Key"] for object in full_dict["Contents"]]
        except KeyError:
            raise KeyError(f"No objects found in {bucket_name}.")

    def upload_table_to_s3_bucket(self, 
                                  table: pd.DataFrame, 
                                  how: str, 
                                  bucket_name: str,
                                  object_name: str) -> None:
        '''
        table: Desired table to upload to S3 bucket.
        how: File format, either 'csv' or 'parquet.'
        bucket_name: Name of S3 bucket.
        object_name: File name to use in S3 bucket.

        Uploads in-memory table to an S3 bucket.
        '''
        if how == "parquet":
            out_buffer = BytesIO()
            table.to_parquet(out_buffer, index = False, engine = "pyarrow")
        elif how == "csv":
            out_buffer = StringIO()
            table.to_csv(out_buffer, index = False)
        else:
            raise ValueError("Invalid 'how' value: only 'csv' and 'parquet' are allowed.")
        
        out_buffer.seek(0)
        
        self._create_client("s3").upload_fileobj(out_buffer, Bucket = bucket_name, Key = object_name)
        
    def upload_file_to_s3_bucket(self, file: str, bucket_name: str, object_name: str) -> None:
        '''
        file: Path to file.
        bucket_name: Name of S3 bucket.
        object_name: File name to use in S3 bucket.
        
        Uploads a file to an S3 bucket as object_name.
        '''
        self._create_client("s3").upload_file(Filename = file, Bucket = bucket_name, Key = object_name)
        
    def get_object_attributes_from_s3_bucket(self, bucket_name: str, object_name: str) -> dict:
        '''
        bucket_name: Name of S3 bucket.
        object_name: File name to use in S3 bucket.
        
        Retrieves metadata of object in an S3 bucket.
        '''
        response = self._create_client("s3").get_object(Bucket = bucket_name, Key = object_name)
        
        return response
    
    def read_parquet_file_from_s3_bucket(self, bucket_name: str, object_name: str, n_rows: int = None) -> pl.DataFrame:
        '''
        n_rows: If specified, only the first n_rows of the parquet file are read.
        
        Loads a parquet file from an S3 bucket into a Polars dataframe.
        '''
        if object_name.endswith("parquet"):
            response = self.get_object_attributes_from_s3_bucket(bucket_name = bucket_name, object_name = object_name)
            
            out_table = pl.read_parquet(BytesIO(response["Body"].read()), n_rows = n_rows)
            
            return out_table
        else:
            raise Exception("{object_name} does not end with .parquet.")
