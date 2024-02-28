from extract import extract_data, get_taxon_key, get_occurrences
from transform import read_csv_file, start_spark, transform_data
from load import load_data, move_csv_file, clear_folder_contents, update_occurrences
from config import load_config
from logger import setup_logger
import sys


def main():
    config = load_config()
    path = config['configuration']['path_file']
    s_name = 'Panthera leo'
    taxon_key = get_taxon_key(s_name)
    occ_gbif = get_occurrences(taxon_key)
    occ_config = config['configuration']['occurrences']

    if occ_gbif > occ_config:
        setup_logger()
        path_to_save = path
        # Extract
        csv_file = extract_data(path_to_save, taxon_key)
        spark = start_spark()
        df = read_csv_file(path_to_save, csv_file, spark)
        # Transform
        columns_to_keep = config['columns_to_keep']
        df_cleaned = transform_data(df, columns_to_keep)
        # Load
        load_data(df_cleaned, path_to_save)
        spark.stop()
        update_occurrences(config, occ_gbif)
        move_csv_file(path_to_save)
        # clear data_temp
        data_temp_path = path_to_save + '/data_temp'
        clear_folder_contents(data_temp_path)
    else:
        sys.exit('No data updates, the program will terminate shortly')


if __name__ == "__main__":
    main()
