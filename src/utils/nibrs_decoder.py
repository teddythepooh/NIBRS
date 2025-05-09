import pandas as pd
from io import StringIO
# from zipfile import ZipFile
from zipfile_deflate64 import ZipFile
from pathlib import Path

class NIBRSUnzip:
    def __init__(self, zip_file: Path):
        '''
        zip_file: path to the NIBRS master file (.zip)
        '''
        try:
            self.zip_file = zip_file
        except FileNotFoundError:
            raise FileNotFoundError(f"No {zip_file} found.")
        
    def _parse_zip_file(self) -> str:
        '''
        Returns the file name of the ASCII file in zip_file. No other file should be present in zip_file.
        '''
        with ZipFile(self.zip_file, "r") as z:
            contents = z.namelist()
            
            if len(contents) > 1:
                raise ValueError(
                    (f"Expected one file in {self.zip_file}, "
                     f"but found {len(contents)} instead.")
                    )
            else:
                return contents[0]
    
    def _standardize(self) -> None:
        '''
        Once the ASCII file has been unzipped, it is renamed to the zip file. If the zip file is called "nibrs-2022.zip," 
        the ASCII file would be renamed to "nibrs-2022.txt." The motivation behind this is to standardize the ASCII files' 
        naming conventions. For some reason, the FBI does not follow the same file name scheme across years.
        
        For example, the 2022 and 2023 ASCII files are called "2022_NIBRS_NATIONAL_MASTER_FILE_ENC.txt" and 
        "2023_NIBRS_NATIONAL_MASTER_FILE.txt" respectively.
        '''
        try:
            ascii_file = ((self.zip_file).parent).joinpath(self._parse_zip_file())
            new_name = (self.zip_file).with_suffix(".txt")
            
            ascii_file.rename(new_name)
            print(f"{ascii_file} has been renamed to {new_name}")
        except FileNotFoundError:
            raise FileNotFoundError(f"No {ascii_file} found.")

    def unzip(self, standardize: bool = True) -> None:
        with ZipFile(self.zip_file, "r") as z:
            z.extractall(path = (self.zip_file).parent)
            print(f"{self.zip_file} has been unzipped.")
        
        if standardize:
            self._standardize()

class NIBRSDecoder:
    def __init__(self, nibrs_master_file: str, col_specs: dict):
        '''
        nibrs_master_file: path to the NIBRS master file (.txt)
        
        col_specs: a dictionary that defines the segment names' levels, along with their
        column widths and column names
        
        ------------------- an example of col_specs (as a .yml file)
            segment_level_codes:
                 administrative_segment: '01'

            administrative_segment:
                col1: [start1, end1]
                col2: [start2, end2]
        -------------------
        
        In the NIBRS data, the segment "level" (a 2-character alphanumeric sequence) is how we can 
        delineate which lines belong to which segment in the NIBRS master file. For example, 
        all lines that start with "01" are the so-called Administrative Segment.
        '''
        self.nibrs_master_file = nibrs_master_file
        self.col_specs = col_specs
        
        if "segment_level_codes" not in self.col_specs.keys():
            raise KeyError("Invalid col_specs. It must have a segment_level_codes key.")
        
    def _view_all_segment_level_codes(self) -> None:
        '''
        prints all available segment codes defined in self.col_specs["segment_level_codes"]
        '''
        for segment, code in self.col_specs["segment_level_codes"].items():
            print(f"{segment} : {code}")
    
    def _get_code_for_segment(self, segment_name: str) -> str:
        try:
            return self.col_specs["segment_level_codes"][segment_name]
        except KeyError:
            raise KeyError("no code for {segment_name} found in col_specs")
        
    def get_col_specs_for_segment(self, segment_name: str) -> tuple:
        col_specs_config = self.col_specs[segment_name]
        
        return tuple(tuple(i) for i in col_specs_config.values())
    
    def get_col_names_for_segment(self, segment_name: str) -> list:
        return list(self.col_specs[segment_name].keys())
    
    def decode_segment(self, segment_name: str) -> pd.DataFrame:
        segment_code = self._get_code_for_segment(segment_name)
        col_specs = self.get_col_specs_for_segment(segment_name)
        col_names = self.get_col_names_for_segment(segment_name)
        
        with open(self.nibrs_master_file, "r") as file:
            filtered_lines = (line for line in file if line.startswith(segment_code))
            
            segment_as_text = StringIO()
            for line in filtered_lines:
                segment_as_text.write(line)
            
            segment_as_text.seek(0) # to reset the pointer to the very beginning
            
            out_table = pd.read_fwf(segment_as_text, colspecs = col_specs, names = col_names)

        return out_table
