import argparse
import re

from pathlib import Path
from time import perf_counter

from core import NIBRSDecoder, AmazonS3, general

def get_year(file_name: str) -> int:
    '''
    Returns the year from file_name of form nibrs-${year}.txt.
    '''
    match = re.search(r"nibrs-([0-9]{4})\.txt", Path(file_name).name)
    if match:
        data_year = match.group(1)
        return data_year
    else:
        raise ValueError(f"Expected file name as nibrs-${{year}}.txt, not {file_name}.")

def main(args: argparse.Namespace) -> None:
    config = general.load_yaml(args.config_file)
    s3_bucket = config["s3_bucket"]
    
    start = perf_counter()
    output_dir, logger = general.create_output_dir(args.output_dir, f"{Path(__file__).stem}.log")
    
    # Step 1: Extract segment.
    reporting_year = get_year(args.nibrs_master_file)
        
    logger.info(f"Decoding {args.segment_name}...")
    
    decoder = NIBRSDecoder(args.nibrs_master_file, config)
    
    out_table = decoder.decode_segment(args.segment_name)
    out_table["db_id"] = f"{reporting_year}_" + (out_table.index + 1).astype(str)
    
    
    # Step 2: Export.
    out_name = f"{args.segment_name}_{reporting_year}.parquet"
    
    logger.info("Exporting...")
    if args.to_s3:
        logger.info("Sending segment to S3 bucket...")
        S3 = AmazonS3()
        S3.upload_table_to_s3_bucket(
            table = out_table, how = "parquet",
            bucket_name = s3_bucket, object_name = out_name
            )
    else:
        out_table.to_parquet(output_dir.joinpath(out_name))
    
    end = perf_counter()
    
    logger.info(f"Done. Total run time: {round((end - start) / 60, 2)} minutes.")
    
if __name__ == "__main__":
    supported_segments = ("administrative", "offense", "arrestee", "victim")
    
    parser = argparse.ArgumentParser(description = "Decodes desired segment from a NIBRS master file into its own .parquet file.")
    
    parser.add_argument("--output_dir", "-o", 
                        default = "output")
    parser.add_argument("--config_file", "-c", 
                        help = ".yaml file with segment_level_codes and s3_bucket keys",
                        default = "configuration/col_specs.yml")
    parser.add_argument("--to_s3",
                        help = "if toggled, the segment will be uploaded to an S3 bucket",
                        action = "store_true")
    
    parser.add_argument("--nibrs_master_file", "-f", 
                        help = "path to NIBRS master file (.txt)")
    parser.add_argument("--segment_name", "-s", choices = [f"{name}_segment" for name in supported_segments],
                        help = "segment of interest that is present as a distinct key in config_file")

    args = parser.parse_args()
    
    main(args)
