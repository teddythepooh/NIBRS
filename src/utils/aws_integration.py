import os
import boto3
from botocore.client import BaseClient
from botocore.exceptions import UnknownServiceError
import s3fs
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
    Instantiates the boto3 client to interact with AWS. If no credentials are explicitly passed, then
    region_name; aws_access_key_id; and aws_secret_access_key are all pulled from environment variables
    of the same name.
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

class AmazonS3asFS(AWSBase):
    '''
    DEPRECATION NOTICE
    
    Treats S3 as a file system (FS) using the s3fs package. I originally used this to read my parquet tables in-memory
    from my S3 bucket, before ingesting them into my postgres db. However, there are apparently some issues with this mechanism.
    
    Instead, I am currently using the read_parquet_file_from_s3_bucket() method in my AmazonS3 class. It is more
    elegant and concise.
    '''
    @staticmethod
    def _build_s3_uri(bucket_name: str, object_name: str) -> str:
        return f"s3://{bucket_name}/{object_name}"

    @staticmethod
    def _read_parquet(file, polars_or_pandas: str) -> pl.DataFrame | pd.DataFrame:
        '''
        polars_or_pandas: 'polars' or 'pandas'
        '''
        if polars_or_pandas == "polars":
            out_table = pl.read_parquet(file)
        elif polars_or_pandas == "pandas":
            out_table = pd.read_parquet(file)
        else:
            raise ValueError("polars_or_pandas arg must be either 'polars' or 'pandas.'")
        
        return out_table
    
    def _build_s3fs_file_system(self) -> s3fs.S3FileSystem:
        '''
        allows me to ingest files from my s3 bucket in-memory
        '''
        return s3fs.S3FileSystem(key = self.aws_access_key_id, secret = self.aws_secret_access_key)

    def read_table_from_s3_bucket(self, 
                                  bucket_name: str, 
                                  object_name: str,
                                  as_polars_or_pandas: str = "polars") -> pl.DataFrame | pd.DataFrame:
        '''
        as_polars_or_pandas: 'polars' or 'pandas'
        
        ingests a parquet file from s3 in-memory as a polars or pandas dataframe
        '''
        try:
            if object_name.endswith("parquet"):
                s3_uri = AmazonS3asFS._build_s3_uri(bucket_name = bucket_name, object_name = object_name)
                fs = self._build_s3fs_file_system()

                with fs.open(s3_uri, "rb") as file:
                    out_table = AmazonS3asFS._read_parquet(file = file, polars_or_pandas = as_polars_or_pandas)

                return out_table
            else:
                raise ValueError("read_table_from_s3_bucket() only supports reading parquet tables at this time.")
        except FileNotFoundError:
            raise FileNotFoundError(f"{object_name} does not exist in {bucket_name} bucket.")

class AmazonS3(AWSBase):
    '''
    For all methods, assume that 'bucket_name' is the name of the S3 bucket and 'object_name' is the name of the file(s) in said bucket.
    '''
    def view_objects_in_s3_bucket(self, bucket_name: str, view_only: bool = False) -> list:
        '''
        view_only: returns all objects from an s3 bucket as a list if True, otherwise the object names and file sizes
        are simply printed
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
        table: a pandas table
        how: 'csv' or 'parquet'

        uploads a pandas table in-memory onto an S3 bucket as a .parquet or .csv file
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
        file: path to file to be uploaded onto an s3 bucket
        
        uploads a file onto an S3 bucket as a file called object_name
        '''
        self._create_client("s3").upload_file(Filename = file, Bucket = bucket_name, Key = object_name)
        
    def get_object_attributes_from_s3_bucket(self, bucket_name: str, object_name: str) -> dict:
        response = self._create_client("s3").get_object(Bucket = bucket_name, Key = object_name)
        
        return response
    
    def read_parquet_file_from_s3_bucket(self, bucket_name: str, object_name: str, n_rows: int = None) -> pl.DataFrame:
        '''
        n_rows: if specified, only the first n_rows of the parquet file is read
        
        loads a parquet file in-memory from an s3 bucket
        '''
        if object_name.endswith("parquet"):
            response = self.get_object_attributes_from_s3_bucket(bucket_name = bucket_name, object_name = object_name)
            
            out_table = pl.read_parquet(BytesIO(response["Body"].read()), n_rows = n_rows)
            
            return out_table
        else:
            raise Exception("{object_name} does not end with .parquet.")
