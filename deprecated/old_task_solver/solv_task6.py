"""
Task 6 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.schemas import SCHEMA_TASK_6
from deprecated.old_task_solver.const import RESULT_TASK6_DATA, DATE_FORMAT_FULL
from utils import read_csv, write_result_df


def check_visits_months(dataframe, end_date='2016-09-30'):
    """
    :param dataframe: data frame with dates and id's
    :param end_date: the end_date to check visitors visits
    :return: dataframe with statistic by months
    """
    w_grouped = Window().partitionBy('patient_id').orderBy('effective_from_date')
    w_patient = Window().partitionBy('patient_id').orderBy('patient_id')

    filtered_df = (dataframe
                   .withColumn('effective_from_date', f.to_date(f.col('effective_from_date'), DATE_FORMAT_FULL))
                   .withColumn('month', f.month(f.col('effective_from_date')))
                   .withColumn('add_month', f.month(f.add_months(f.col('effective_from_date'), 1)))
                   .withColumn('end_date', f.lit(end_date))
                   .withColumn('start_date', f.add_months(f.col('end_date'), -12))
                   .withColumn('diff_dates', f.col('add_month') - f.lag(f.col('month'), -1).over(w_grouped))
                   .where(f.col('effective_from_date').between(f.col('start_date'), f.col('end_date')))
                   .where(f.col('diff_dates') == 0))

    numbered_df = (filtered_df
                   .withColumn('row_num', f.row_number().over(w_grouped))
                   .withColumn('max_rank', f.max('row_num').over(w_patient))
                   .where(f.col('row_num') == f.col('max_rank')))

    result_df = (numbered_df
                 .withColumn('5month', f.when(f.col('max_rank') >= 5, True).otherwise(False))
                 .withColumn('7month', f.when(f.col('max_rank') >= 7, True).otherwise(False))
                 .withColumn('9month', f.when(f.col('max_rank') >= 9, True).otherwise(False))
                 .select('patient_id', '5month', '7month', '9month'))

    return result_df


def solv_task_6(spark, file_path):
    """
    :param spark: SparkSession
    :param file_path: path to file with data
    :return: result file and dataframe
    """
    visits_df = read_csv(spark=spark,
                         path=file_path,
                         schema_csv=SCHEMA_TASK_6,
                         header='true')

    result_df = check_visits_months(dataframe=visits_df)

    write_result_df(df=result_df, header='true', file_path=RESULT_TASK6_DATA)
