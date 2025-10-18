import argparse
from core import general, AmazonS3
from db_design import Postgres, raw_tables

def main(args: argparse.Namespace):
    config = general.load_yaml(args.config_file)
    postgres_config = general.load_yaml(args.postgres_config)
    
    S3 = AmazonS3()
    postgres = Postgres(credentials = postgres_config.get("postgresql")["credentials"],
                        schemas = postgres_config.get("postgresql")["schemas"])
    
    bucket_name = config["s3_bucket"]
    parquet_files = S3.view_objects_in_s3_bucket(bucket_name = bucket_name, view_only = False)
    
    print(f"Found {len(parquet_files)} parquet files in {bucket_name} bucket...")
    
    for file_name in parquet_files:
        table_name = file_name.replace(".parquet", "").rsplit("_", 1)[0]
        
        print(f"\nProcessing {file_name}...")
        
        table = S3.read_parquet_file_from_s3_bucket(
            bucket_name=bucket_name,
            object_name=file_name
        )
        
        postgres.ingest_table_into_db(
            table_to_ingest = table,
            db_table = f"raw.{table_name}",
            table_base = raw_tables.Base,
            source_file = file_name
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", help=".yml file with s3_bucket key")
    parser.add_argument("--postgres_config", help=".yml file with postgresql key, under which exists credentials and schemas keys")
    
    args = parser.parse_args()
    
    main(args)
