"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f

from deprecated.old_task_solver.schemas import SCHEMA_TASK_8_2_1
from utils import read_json, write_result_df


def find_business_wifi(dataframe):
    """
    :param dataframe: data frame with data
    :return: dataframe with info about wifi in business
    """
    df_with_wifi = dataframe.select('business_id', 'name', 'attributes.WiFi')

    result_df = (df_with_wifi
                 .withColumn('free_wifi', f.when(df_with_wifi.WiFi.contains('free'), True).otherwise(False))
                 .drop('WiFi'))

    return result_df


def solv_task_8_2_1(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    business_df = read_json(spark=spark,
                            path=file_path,
                            schema_json=SCHEMA_TASK_8_2_1,
                            header='true')

    result_df = find_business_wifi(dataframe=business_df)

    write_result_df(df=result_df, file_path=save_file_path)
