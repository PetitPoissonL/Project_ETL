import logging
import yaml
from logger import setup_logger
import shutil
import os
from datetime import datetime

logger = setup_logger()


def load_data(dataframe, path):
    logging.info('Loading data')
    output_path = path + '/temp'
    dataframe.coalesce(1).write.csv(output_path, header=True, mode="overwrite")


def update_occurrences(config, new_occ):
    config['configuration']['occurrences'] = new_occ
    path = config['configuration']['path_file']
    with open(path + '/config/config.yaml', 'w') as file:
        yaml.dump(config, file)


def move_csv_file(path):
    temp_folder = path + '/temp'
    data_folder = path + '/data'

    success_file = os.path.join(temp_folder, '_SUCCESS')
    if os.path.exists(success_file):
        logging.info('Moves CSV files and deletes the temp directory')
        for filename in os.listdir(temp_folder):
            if filename.endswith('.csv'):
                csv_file = os.path.join(temp_folder, filename)

                now = datetime.now()
                formatted_now = now.strftime('%Y-%m-%d %H_%M_%S')
                new_filename = 'dataset_cleaned ' + formatted_now + '.csv'
                new_csv_file = os.path.join(data_folder, new_filename)

                shutil.move(csv_file, new_csv_file)
                logging.info(f'{filename} has been renamed and moved to {new_csv_file}')

        # deletes the temp directory
        shutil.rmtree(temp_folder)
        logging.info(f'The directory {temp_folder} has been deleted')
    else:
        logging.info('The file _SUCCESS does not exist, no action is taken')


def clear_folder_contents(folder_path):
    for entry in os.scandir(folder_path):
        try:
            if entry.is_file():
                os.remove(entry.path)
            elif entry.is_dir():
                shutil.rmtree(entry.path)
        except Exception as e:
            print(f'Failed to delete {entry.path}: {e}')
