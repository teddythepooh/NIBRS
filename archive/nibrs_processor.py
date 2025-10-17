import polars as pl
import pandas as pd

class Diagnostics:
    def __init__(self, table: pl.DataFrame):
        if not isinstance(table, pd.DataFrame):
            self.table = table
        else:
            raise TypeError("Invalid table: it must be a Polars dataframe.")
    
    def return_duplicate_rows(self) -> pl.DataFrame:
        out_table = self.table.filter(self.table.is_duplicated())
        
        if out_table.height == 0:
            return out_table
        else:
            raise Exception("No duplicate rows found.")
        
    def pct_unique_in_col(self, col_name: str) -> float:
        return round(self.table.n_unique(subset = col_name) / self.table.shape[0], 2)
    
    def pct_missingness_table(self, in_descending_order: bool = True) -> pd.DataFrame:
        '''
        returns a pandas table with two columns: column and pct_missing
        
        Note that the polars table is cast to pandas because there is not an elegant way
        to rename the table after computing null counts and tranposing the result. Without casting
        it to pandas, the missingness table retains the original columns as column names despite 
        not appearing as such.
        '''
        pct_missing = ((self.table.null_count() / self.table.height)
                       .transpose(include_header = True)
                       .sort(by = pl.col("column_0"), descending = in_descending_order))
        
        out_table = pct_missing.to_pandas()
        out_table.columns = ["column", "pct_missing"]
        
        return out_table