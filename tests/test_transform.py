import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from src.transform import transform_data
from pyspark.testing import assertDataFrameEqual


class TestTransform(unittest.TestCase):

    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('unit_test').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data(self):
        # Creates a test DataFrame
        data1 = [('1', '1990-10-10', 23.2, 25.0, None, 'Panthera leo (Linnaeus, 1758)'),
                 ('1', '1990-10-11', 23.2, 25.0, 1, 'Panthera leo (Linnaeus, 1758)'),
                 ('2', '1990-10-10', 23.2, 25.0, 2, 'Panthera leo (Linnaeus, 1758)'),
                 ('3', None, 23.2, 25.0, None, 'Panthera leo (Linnaeus, 1758)'),
                 (None, None, 23.2, 25.0, None, 'Panthera leo (Linnaeus, 1758)'),
                 ('3', None, 23.2, 25.0, 5, 'Panthera leo (Linnaeus, 1758)'),
                 ('10', '1990-10-10', 23.2, 25.0, 2, 'Panthera leo'),
                 ('11', '1990-10-10', 23.2, None, 2, 'Panthera leo'),
                 ('12', '1990-10-10', None, 25.0, 2, 'Panthera leo'),
                 ('50', '1990-10-10', 23.2, 25.0, 2, 'Panthera leo')]

        data2 = [('1', '1990-10-10', 23.2, 25.0, 1, 'Panthera leo (Linnaeus, 1758)'),
                 ('2', '1990-10-10', 23.2, 25.0, 2, 'Panthera leo (Linnaeus, 1758)')]

        schema = StructType([
            StructField("occurrenceID", StringType(), True),
            StructField("eventDate", StringType(), True),
            StructField("decimalLatitude", FloatType(), True),
            StructField("decimalLongitude", FloatType(), True),
            StructField("individualCount", IntegerType(), True),
            StructField("scientificName", StringType(), True)
        ])

        df_test1 = self.spark.createDataFrame(data1, schema)
        df_test2 = self.spark.createDataFrame(data2, schema)

        cols = ["occurrenceID", "eventDate", "decimalLatitude", "decimalLongitude", "individualCount", "scientificName"]

        # cleans the data
        df_cleaned1 = transform_data(df_test1, cols)
        df_cleaned2 = transform_data(df_test2, cols)
        assertDataFrameEqual(df_cleaned1, df_cleaned2)


if __name__ == "__main__":
    unittest.main()

