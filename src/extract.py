from logger import setup_logger
import logging
import os
import zipfile
from pygbif import occurrences as occ
from pygbif import species
from pathlib import Path
import time

logger = setup_logger()


def get_occurrences(taxon_key):
    return occ.count(taxon_key)


def get_taxon_key(scientific_name):
    name_info = species.name_backbone(name=scientific_name)
    taxon_key = name_info.get('usageKey')
    return taxon_key


def get_download_key(taxon_key):
    # Gets GBIF account information
    user = os.environ.get('GBIF_USER')
    password = os.environ.get('GBIF_PASSWORD')
    email = os.environ.get('GBIF_EMAIL')
    # Gets download_key with taxonKey
    res = occ.download(f'taxonKey = {taxon_key}',
                       format='SIMPLE_CSV',
                       user=user,
                       pwd=password,
                       email=email)
    return res[0]


def extract_data(path, taxon_key):
    # Downloads zip file
    logging.info('Extracting data')
    download_key = get_download_key(taxon_key)
    zip_file = download_key + '.zip'
    path_zip_file = Path(path + '/data_temp/' + zip_file)
    if not path_zip_file.exists():
        # Check the download status
        while True:
            meta = occ.download_meta(download_key)
            if meta['status'] == 'SUCCEEDED':
                logging.info("Download succeeded!")
                break
            elif meta['status'] == 'KILLED' or meta['status'] == 'FAILED':
                logging.info("Download failed or was killed!")
                break
            logging.info('Waiting for the download file to be ready')
            time.sleep(60)

        occ.download_get(download_key, path + '/data_temp')
    else:
        logging.info(f'{zip_file} already exists')

    # Unzips the ZIP file
    csv_file = download_key + '.csv'
    path_csv_file = Path(path + '/data_temp/' + csv_file)
    if not path_csv_file.exists():
        with zipfile.ZipFile(path_zip_file, 'r') as zip_ref:
            zip_ref.extractall(path + '/data_temp')
        logging.info('File decompression complete')
    else:
        logging.info(f'{csv_file} already exists')

    return csv_file


def main():


if __name__ == '__main__':





