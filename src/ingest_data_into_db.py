import argparse
from utils import general_utils, AmazonS3
from db_design import Postgres, raw_tables

def main(args: argparse.Namespace):
    config = general_utils.load_yaml(args.config_file)
    postgres_config = general_utils.load_yaml(args.postgres_config)
    
    AmazonS3_tool = AmazonS3()
    postgres_tool = Postgres(credentials = postgres_config.get("postgresql")["credentials"],
                             schemas = postgres_config.get("postgresql")["schemas"])

    
    table_from_s3 = AmazonS3_tool.read_parquet_file_from_s3_bucket(bucket_name = config["s3_bucket"],
                                                                   object_name = f"{args.segment_name}_2022.parquet")
    
    
    postgres_tool.ingest_raw_table_into_sql(table_to_ingest = table_from_s3,
                                            sql_table = f"raw.{args.segment_name}",
                                            raw_table_metadata = raw_tables.Base)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_file", help = ".yaml file with s3_bucket key")
    parser.add_argument("--postgres_config", help = ".yaml file with postgresql key, under which exists credentials and schemas keys")
    parser.add_argument("--segment_name")
    
    args = parser.parse_args()
    
    main(args)
    