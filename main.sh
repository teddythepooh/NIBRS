raw_data_dir=raw_data
output_dir=output

data_years=(
    2022
    2023
)

for year in "${data_years[@]}"; do
    nibrs_zipped_file=$raw_data_dir/nibrs-$year.zip
    
    if [[ $year -eq 2022 ]]; then
        nibrs_mf=$raw_data_dir/${year}_NIBRS_NATIONAL_MASTER_FILE_ENC.txt
    else
        nibrs_mf=$raw_data_dir/${year}_NIBRS_NATIONAL_MASTER_FILE.txt
    fi
    
    if [[ -f $nibrs_zipped_file ]]; then
        if [[ ! -f $nibrs_mf ]]; then
            echo Unzipping $nibrs_zipped_file into $raw_data_dir...
            unzip -o $nibrs_zipped_file -d $raw_data_dir
        else
            echo $nibrs_zipped_file has already been unzipped...
        fi
    fi
done

make