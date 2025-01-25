import argparse
import logging
from pathlib import Path

from time import perf_counter

from utils import general_utils, NIBRSDecoder, AmazonS3

def main(args: argparse.Namespace) -> None:
    output_dir: Path
    logger: logging.Logger
    
    start = perf_counter()
    output_dir, logger = general_utils.create_output_dir_and_logger(
        output_dir_str = args.output_dir, 
        log_file = f"{Path(__file__).stem}.log"
        )
        
    data_year = Path(args.nibrs_master_file).name[0:4]
    
    if not data_year.isdigit():
        logger.warning("Double check args.nibrs_master_file. This text file's file name must be "
                       "prefixed with the year (e.g., 2022).")
        
    logger.info(f"Decoding {args.segment_name}...")
    
    config = general_utils.load_yaml(args.config_file)
    
    nibrs_processor_tool = NIBRSDecoder(args.nibrs_master_file, config)
    
    out_table = nibrs_processor_tool.decode_segment(args.segment_name)
    out_table["db_id"] = [f"{data_year}_{i}" for i in out_table.index]
    
    out_name = f"{args.segment_name}_{data_year}.parquet"
    
    logger.info("Exporting...")
    if args.to_s3:
        logger.info("Sending decoded segment to s3 bucket...")
        
        AmazonS3_tool = AmazonS3()
        
        AmazonS3_tool.upload_table_to_s3_bucket(table = out_table, 
                                                how = "parquet",
                                                bucket_name = config["s3_bucket"],
                                                object_name = out_name)
    else:
        out_table.to_parquet(output_dir.joinpath(out_name))
    
    end = perf_counter()
    
    logger.info(f"Done. Total run time: {round((end - start) / 60, 2)} minutes")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", "-o")
    parser.add_argument("--nibrs_master_file", "-f", help = "path to the NIBRS fixed-length, ASCII text file")
    parser.add_argument("--config_file", "-c", help = ".yaml file with 'segment_level_codes' and 's3_bucket' keys, plus any segments of interests as keys")
    parser.add_argument("--segment_name", "-s", help = "segment of interest to decode; it must be present as key in config_file")
    
    parser.add_argument("--to_s3",
                        help = ("if toggled, the decoded segment won't be exported to output_dir and instead be "
                                "uploaded to an s3 bucket using secrets configured as environment variables"),
                        action = "store_true")

    args = parser.parse_args()
    
    main(args)
