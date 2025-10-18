import argparse

from pathlib import Path

from core import NIBRSUnzip

def main(nibrs_master_file: str):
    '''
    Takes nibrs_master_file, then unzips it in the same directory.
    '''
    nibrs = NIBRSUnzip(Path(nibrs_master_file))
    nibrs.unzip()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help = "path to NIBRS master file (.zip)")
    args = parser.parse_args()
    
    main(nibrs_master_file = args.f)
