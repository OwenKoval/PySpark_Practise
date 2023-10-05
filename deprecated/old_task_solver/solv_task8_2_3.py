"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql import types as t

from deprecated.old_task_solver.schemas import SCHEMA_TASK_8_2_3
from utils import read_json, write_result_df_json


def find_gas_station(dataframe):
    """
    :param dataframe: data frame with data
    :return: dataframe with info about gas station
    """
    result_df = (dataframe
                 .select('business_id', 'name', 'categories', 'attributes.RestaurantsPriceRange2')
                 .withColumn('price_range', f.col('RestaurantsPriceRange2').cast(t.IntegerType()))
                 .drop('RestaurantsPriceRange2')
                 .withColumn('has_cafe', f.when(dataframe.categories.contains('Cafe'), True).otherwise(False))
                 .withColumn('has_store', f.when(dataframe.categories.contains('Store'), True).otherwise(False))
                 .withColumn('categories', f.split(f.col('categories'), ','))
                 .where((dataframe.categories.contains('Gas Station')) & (f.col('RestaurantsPriceRange2') <= 3)))

    return result_df


def solv_task_8_2_3(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    business_df = read_json(spark=spark,
                            path=file_path,
                            schema_json=SCHEMA_TASK_8_2_3,
                            header='true')

    result_df = find_gas_station(dataframe=business_df)

    write_result_df_json(df=result_df, file_path=save_file_path)
