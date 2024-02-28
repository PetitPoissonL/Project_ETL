import logging
from logger import setup_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

logger = setup_logger()


def start_spark():
    logging.info('Starting Spark')
    spark_session = SparkSession \
        .builder \
        .appName('ETL_lion') \
        .getOrCreate()
    return spark_session


def read_csv_file(path, file, spark_session):
    logging.info('Reading CSV file')
    dataframe = spark_session.read \
        .format('csv') \
        .option("header", "true") \
        .option("mode", "FAILFAST") \
        .option("inferSchema", "true") \
        .option("sep", "\t") \
        .load(path + '/data_temp/' + file)
    return dataframe


def transform_data(dataframe, columns_to_keep):
    logging.info('Transforming data')
    df_clean_columns = dataframe.select(columns_to_keep)

    # Processing null data
    df_cleaned = df_clean_columns.na.drop(subset=['occurrenceID', 'eventDate', 'decimalLatitude', 'decimalLongitude'])
    df_filled = df_cleaned.fillna({'individualCount': 1})

    # filtering data
    df_filtered = df_filled.filter(col('scientificName') == 'Panthera leo (Linnaeus, 1758)')
    df_unique = df_filtered.dropDuplicates(["occurrenceID"])

    return df_unique
