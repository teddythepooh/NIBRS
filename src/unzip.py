import argparse
from utils import NIBRSUnzip
from pathlib import Path

def main(nibrs_master_file: str):
    nibrs = NIBRSUnzip(Path(nibrs_master_file))
    nibrs.unzip()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help = "path to NIBRS master file (.zip)")
    args = parser.parse_args()
    
    main(nibrs_master_file = args.f)
