"""
Task 7 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.schemas import SCHEMA_TASK_7
from deprecated.old_task_solver.const import DATE_FORMAT_FULL
from utils import read_csv, write_result_df


def preparing_visits_df(dataframe, end_date='2016-09-30'):
    """
    :param dataframe: data frame with dates and id's
    :param end_date: the end_date to check visitors visits
    :return: dataframe with statistic the longest streak after end_date
    """
    w_grouped = Window().partitionBy('patient_id').orderBy('effective_from_date')

    prepared_visits_df = (dataframe
                          .withColumn('effective_from_date', f.to_date(f.col('effective_from_date'), DATE_FORMAT_FULL))
                          .withColumn('end_date', f.lit(end_date))
                          .withColumn('start_date', f.add_months(f.col('end_date'), -12))
                          .withColumn('month', f.month(f.col('effective_from_date')))
                          .withColumn('numb', f.row_number().over(w_grouped))
                          .withColumn('diff', f.col('month') - f.col('numb')))

    return prepared_visits_df


def longest_visits_to_end_date(prepared_df):
    """
    :param prepared_df: data frame with dates and id's
    :return: dataframe with statistic the longest streak after end_date
    """
    p_grouped = Window().partitionBy('patient_id')

    longest_visits_df = (prepared_df
                         .where(f.col('effective_from_date').between(f.col('start_date'), f.col('end_date')))
                         .groupby('patient_id', 'diff')
                         .agg(f.count('diff').alias('longest'))
                         .withColumn('max', f.max('longest').over(p_grouped))
                         .where(f.col('longest') == f.col('max'))
                         .select('patient_id', 'longest'))

    return longest_visits_df


def longest_visits_after_end_date(prepared_df):
    """
        :param prepared_df: data frame with dates and id's
        :return: dataframe with statistic the longest streak after end_date
    """
    p_grouped = Window().partitionBy('patient_id')

    longest_since_end_date = (prepared_df
                              .where(f.col('effective_from_date') > f.col('end_date'))
                              .groupby('patient_id', 'diff')
                              .agg(f.count('diff').alias('longest_since_end_date'))
                              .withColumn('max', f.max('longest_since_end_date').over(p_grouped))
                              .where(f.col('longest_since_end_date') == f.col('max'))
                              .select('patient_id', 'longest_since_end_date'))

    return longest_since_end_date


def visits_to_since_end_date(prepared_longest_df, prepared_longest_after_df):
    """
       :param prepared_longest_df: prepared df with the longest streak of month to end_date
       :param prepared_longest_after_df: prepared df with the longest streak of month since end_date
       :return: dataframe with statistic the longest streak after end_date
   """
    result_df = (prepared_longest_df
                 .join(prepared_longest_after_df, on='patient_id', how='left')
                 .fillna(value=0)
                 .distinct())

    return result_df


def solv_task_7(spark, file_path, save_file_path):
    """
        :param spark: SparkSession
        :param file_path: path to file with data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    visits_df = read_csv(spark=spark,
                         path=file_path,
                         schema_csv=SCHEMA_TASK_7,
                         header='true')

    prepared_visits_df = preparing_visits_df(dataframe=visits_df)

    longest_visits_df = longest_visits_to_end_date(prepared_df=prepared_visits_df)

    longest_since_end_date = longest_visits_after_end_date(prepared_df=prepared_visits_df)

    result_df = visits_to_since_end_date(prepared_longest_df=longest_visits_df,
                                         prepared_longest_after_df=longest_since_end_date)

    write_result_df(df=result_df, file_path=save_file_path)
