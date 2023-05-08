from pyspark.sql.functions import avg, count
from pyspark.sql import DataFrame, Column


class PySparkScript:

    def group_by_with_count(self, df: DataFrame, group_by_column: str, sum_column: str, derived_column: str):
        print('Inside group_by_with_average')
        aggregated_df = df.groupby(group_by_column).agg(count(sum_column).alias(derived_column))
        aggregated_df.show(truncate=False)
        print('Exiting group_by_with_average')
        return aggregated_df

    def group_by_with_average(self, df: DataFrame, group_by_column: str, avg_column: str, derived_column: str):
        print('Inside group_by_with_average')
        aggregated_df = df.groupby(group_by_column).agg(avg(avg_column).alias(derived_column))
        aggregated_df.show(truncate=False)
        print('Exiting group_by_with_average')
        return aggregated_df

