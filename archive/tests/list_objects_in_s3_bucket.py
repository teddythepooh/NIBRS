import os

try:
    from src.utils import general_utils, AmazonS3
except ImportError:
    raise Exception("Run this as a module, like python -m tests.list_objects_in_s3_bucket")

def main():
    config = general_utils.load_yaml("configuration/col_specs.yaml")

    aws_s3_tool = AmazonS3(region_name = os.environ["region_name"],
                           aws_access_key_id = os.environ["aws_access_key_id"],
                           aws_secret_access_key = os.environ["aws_secret_access_key"])
    
    aws_s3_tool.print_objects_in_s3_bucket(bucket_name = config["s3_bucket"])
    aws_s3_tool.print_objects_in_s3_bucket(bucket_name = config["s3_bucket"], print_full_dict = True)
    
    print(aws_s3_tool.__doc__)
    
if __name__ == "__main__":
    main()