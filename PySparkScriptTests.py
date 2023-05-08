from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, StructField, LongType, DoubleType

from PySparkScript import PySparkScript
from PySparkUnitTestBase import PySparkUnitTestBase


def are_dfs_equal(df1: DataFrame, df2: DataFrame):
    return (df1.schema == df2.schema) and (df1.collect() == df2.collect())


class PySparkScriptTests(PySparkUnitTestBase):

    def test_group_by_with_count(self):
        # Given
        print('Inside PySparkScriptTests test_df_agg')
        challenges = [
            {"user": 1, "country": "USA", "progress": 50},
            {"user": 2, "country": "PT", "progress": 75},
            {"user": 3, "country": "BR", "progress": 25},
            {"user": 4, "country": "BR", "progress": 40},
            {"user": 5, "country": "USA", "progress": 80}
        ]
        df = self.spark.createDataFrame(challenges)

        # When
        aggregate = PySparkScript().group_by_with_count(df, "country", "user", "users_per_country")

        # Then
        expected = [
            {"country": "USA", "users_per_country": 2},
            {"country": "PT", "users_per_country": 1},
            {"country": "BR", "users_per_country": 2}
        ]
        expected_df = self.spark.createDataFrame(expected, schema=StructType([
            StructField("country", StringType(), True),
            StructField("users_per_country", LongType(), False)]))

        self.assertTrue(are_dfs_equal(aggregate, expected_df))

    def test_group_by_with_average(self):
        # Given
        print('Inside PySparkScriptTests test_df_agg')
        challenges = [
            {"user": 1, "country": "USA", "progress": 50},
            {"user": 2, "country": "PT", "progress": 75},
            {"user": 3, "country": "BR", "progress": 25},
            {"user": 4, "country": "BR", "progress": 40},
            {"user": 5, "country": "USA", "progress": 80}
        ]
        df = self.spark.createDataFrame(challenges)

        # When
        aggregate = PySparkScript().group_by_with_average(df, "country", "progress", "average_progress")

        # Then
        expected = [
            {"country": "USA", "average_progress": 65.0},
            {"country": "PT", "average_progress": 75.0},
            {"country": "BR", "average_progress": 32.5}
        ]
        expected_df = self.spark.createDataFrame(expected, schema=StructType([
            StructField("country", StringType(), True),
            StructField("average_progress", DoubleType(), True)]))

        self.assertTrue(are_dfs_equal(aggregate, expected_df))

