"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f

from deprecated.old_task_solver.schemas import SCHEMA_TASK_8_2_2
from utils import read_json, write_result_df


def find_parking_business(dataframe):
    """
    :param dataframe: data frame with data
    :return: dataframe with info about parking in business
    """
    selected_df = dataframe.select('business_id', 'name', 'attributes.BusinessParking', 'attributes.BikeParking')

    result_df = (selected_df
                 .withColumn('has_car_parking',
                             f.when(selected_df.BusinessParking.contains('True'), True).otherwise(False))
                 .withColumn('has_bike_parking', f.when(f.col('BikeParking') == 'True', True).otherwise(False))
                 .drop('BusinessParking', 'BikeParking')
                 )

    return result_df


def solv_task_8_2_2(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    business_df = read_json(spark=spark,
                            path=file_path,
                            schema_json=SCHEMA_TASK_8_2_2,
                            header='true')

    result_df = find_parking_business(dataframe=business_df)

    write_result_df(df=result_df, file_path=save_file_path)
