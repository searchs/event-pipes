import unittest
from unittest import TestCase
from pyspark.sql import SparkSession



class UtilsTestCase(TestCase):
    spark = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HolaSparkTest") \
            .getOrCreate()

    def test_datafile_loading(self):
        pass

    def test_country_count(self):
        pass


    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
