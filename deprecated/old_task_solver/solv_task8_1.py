"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql import types as t

from deprecated.old_task_solver.schemas import SCHEMA_TASK_8
from utils import read_json, write_result_df


def find_working_hours(dataframe):
    """
    :param dataframe: data frame with data
    :return: dataframe with working hours for each day in week
    """
    df_working_hours = (dataframe
                        .select('business_id', 'name', f.explode(f.col('hours')))
                        .withColumn('work_hours',
                                    (f.regexp_extract(f.split(f.col('value'), '-').getItem(1), '(.*)(:)', 1) -
                                     f.regexp_extract(f.split(f.col('value'), '-').getItem(0), '(.*)(:)', 1)))
                        .withColumn('work_hours',
                                    f.when(f.col('work_hours') < 0, f.col('work_hours') + 24)
                                    .when(f.col('work_hours') == 0.0, 24.0)
                                    .otherwise(f.col('work_hours')))
                        .withColumn('is_work_12_hours',
                                    f.when(f.col('work_hours') >= 12.0, True).otherwise(False))
                        .withColumn('is_open_for_24h', f.when(f.col('work_hours') == 24.0, True).otherwise(False))
                        )

    return df_working_hours


def find_business(prepared_df):
    """
    :param prepared_df: data frame with working hours in 24-format
    :return: dataframe with avg_working_hours and 24-working business
    """
    result_df = (prepared_df
                 .groupBy('business_id', 'name')
                 .agg(f.avg('work_hours').cast(t.FloatType()).alias('avg_hours_open'),
                      f.when(f.count(f.when(f.col('is_open_for_24h'),
                                            f.col('is_open_for_24h'))) > 0, True).otherwise(False).alias(
                          'is_open_for_24h'),
                      f.count(f.when(f.col("is_work_12_hours"), f.col("is_work_12_hours"))).alias('12hours'))
                 .where(f.col('12hours') > 0)
                 .drop('is_work_12_hours', '12hours'))

    return result_df


def solv_task_8_1(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    business_df = read_json(spark=spark,
                            path=file_path,
                            schema_json=SCHEMA_TASK_8,
                            header='true')

    df_working_hours = find_working_hours(dataframe=business_df)

    result_df = find_business(prepared_df=df_working_hours)

    write_result_df(df=result_df, file_path=save_file_path)
