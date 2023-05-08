import unittest
from pyspark.sql import SparkSession


class PySparkUnitTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Unit testing in PySpark").master('local[*]').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
