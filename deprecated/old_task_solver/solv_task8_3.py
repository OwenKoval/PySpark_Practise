"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.schemas import SCHEMA_TASK_8_3
from utils import read_json, write_result_df


def find_checkin_business(dataframe):
    """
    :param dataframe: data frame with data
    :return: dataframe with info about checkin in business
    """

    windowed_business = Window().partitionBy('business_id', 'year').orderBy('year')

    df_with_dates = (dataframe.withColumn('dates', f.split(f.col('date'), ',')))

    result_df = (df_with_dates
                 .select('business_id', f.explode(df_with_dates.dates).alias('dates'))
                 .withColumn('date', f.to_date(f.col('dates'), 'yyyy-MM-dd HH:mm:ss'))
                 .withColumn('year', f.year(f.col('date')))
                 .withColumn('year_number', f.row_number().over(windowed_business))
                 .withColumn('checkin_count', f.max('year_number').over(windowed_business))
                 .where(f.col('year_number') == f.col('checkin_count'))
                 .drop('year_number', 'dates', 'date'))

    return result_df


def solv_task_8_3(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    business_checkin_df = read_json(spark=spark,
                                    path=file_path,
                                    schema_json=SCHEMA_TASK_8_3,
                                    header='true')

    result_df = find_checkin_business(dataframe=business_checkin_df)

    write_result_df(df=result_df, file_path=save_file_path)
