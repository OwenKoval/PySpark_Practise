"""
Task 5 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.schemas import SCHEMA_TASK_5_2
from deprecated.old_task_solver.const import RESULT_TASK5_2_DATA_FILE
from utils import read_csv, write_result_df


def summ_date_df(month_df):
    """
    :param month_df: dataframe with date for visiting hospital
    :return: prepared dataframe
    """
    numbered_moth = (month_df
                     .withColumn('q1', f.when((f.col('month') == 'mar') |
                                              (f.col('month') == 'feb') | (f.col("month") == 'jan'), 1).otherwise(0))
                     .withColumn('q2', f.when((f.col('month') == 'apr') |
                                              (f.col('month') == 'may') | (f.col("month") == 'jun'), 1).otherwise(0))
                     .withColumn('q3', f.when((f.col('month') == 'jul') |
                                              (f.col('month') == 'aug') | (f.col('month') == 'sep'), 1).otherwise(0))
                     .withColumn('q4', f.when((f.col('month') == 'oct') |
                                              (f.col('month') == 'nov') | (f.col('month') == 'dec'), 1).otherwise(0))
                     .drop(f.col('month')))

    prepared_df = (numbered_moth
                   .withColumn('row', f.row_number().over(Window().partitionBy('id').orderBy(f.col('id'))))
                   .withColumn('Sum-q1', f.sum('q1').over(Window().partitionBy('id')))
                   .withColumn('Sum-q2', f.sum('q2').over(Window().partitionBy('id')))
                   .withColumn('Sum-q3', f.sum('q3').over(Window().partitionBy('id')))
                   .withColumn('Sum-q4', f.sum('q4').over(Window().partitionBy('id')))
                   .where(f.col('row') == 1)
                   .select('id', 'Sum-q1', 'Sum-q2', 'Sum-q3', 'Sum-q4')
                   .orderBy('id'))

    return prepared_df


def quarter_visit_hospital(prepared_df):
    """
    :param prepared_df: summed df with columns of quarter
    :return: result df with quarter visits
    """
    result_df = (prepared_df
                 .withColumn('q1', f.when(f.col('Sum-q1') >= 1, 1).otherwise(0))
                 .withColumn('q2', f.when(f.col('Sum-q2') >= 1, 1).otherwise(0))
                 .withColumn('q3', f.when(f.col('Sum-q3') >= 1, 1).otherwise(0))
                 .withColumn('q4', f.when(f.col('Sum-q4') >= 1, 1).otherwise(0))
                 .drop('Sum-q1', 'Sum-q2', 'Sum-q3', 'Sum-q4'))

    return result_df


def solv_task5_2(spark,
                 file_path):
    """
    :param spark: SparkSession
    :param file_path: path to file with data
    :return: result file and dataframe
    """
    month_df = read_csv(spark=spark,
                        path=file_path,
                        schema_csv=SCHEMA_TASK_5_2,
                        header='true')

    numbered_moth = summ_date_df(month_df=month_df)

    result_df = quarter_visit_hospital(prepared_df=numbered_moth)

    write_result_df(df=result_df, header='true', file_path=RESULT_TASK5_2_DATA_FILE)
