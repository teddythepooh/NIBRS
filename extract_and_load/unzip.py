import argparse
from core import NIBRSUnzip
from pathlib import Path

def main(nibrs_master_file: str):
    '''
    nibrs_master_file: Path to NIBRS master file (.zip). The FBI makes one master file available per year.
    
    Takes nibrs_master_file, then unzips it in the same directory.
    '''
    nibrs = NIBRSUnzip(Path(nibrs_master_file))
    nibrs.unzip()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", help = "path to NIBRS master file (.zip)")
    args = parser.parse_args()
    
    main(nibrs_master_file = args.f)
