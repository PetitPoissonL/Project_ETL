import logging
from config import load_config

config = load_config()
path = config['configuration']['path_file']


def setup_logger():
    logger = logging.getLogger('ETL_lion')
    logger.setLevel(logging.DEBUG)

    # writes to logger
    file_handler = logging.FileHandler(path + '/logs/etl.log')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Outputs to the console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    return logger
