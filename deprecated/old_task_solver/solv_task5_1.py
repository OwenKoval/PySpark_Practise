"""
Task 5 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f

from deprecated.old_task_solver.schemas import SCHEMA_TASK_5_1
from deprecated.old_task_solver.const import RESULT_TASK5_1_DATA_FILE
from utils import read_csv, write_result_df


def quarter_visit_hospital(month_df):
    """
    :param month_df: df with the data
    :return: summed df for quarter
    """
    prepared_df = (month_df
                   .withColumn('q1', f.abs(f.col('m1')) + f.abs(f.col('m2')) + f.abs(f.col('m3')))
                   .withColumn('q2', f.abs(f.col('m4')) + f.abs(f.col('m5')) + f.abs(f.col('m6')))
                   .withColumn('q3', f.abs(f.col('m7')) + f.abs(f.col('m8')) + f.abs(f.col('m9')))
                   .withColumn('q4', f.abs(f.col('m10')) + f.abs(f.col('m11')) + f.abs(f.col('m12'))))

    quarter_df = (prepared_df
                  .withColumn('q1', f.when(f.col('q1') >= 1, 1).otherwise(0))
                  .withColumn('q2', f.when(f.col('q1') >= 1, 1).otherwise(0))
                  .withColumn('q3', f.when(f.col('q1') >= 1, 1).otherwise(0))
                  .withColumn('q4', f.when(f.col('q1') >= 1, 1).otherwise(0))
                  )

    result_df = (quarter_df.select('id', 'q1', 'q2', 'q3', 'q4'))

    return result_df


def solv_task5_1(spark,
                 file_path):
    """
    :param spark: SparkSession
    :param file_path: path to file with data
    :return: result file and dataframe
    """
    month_df = read_csv(spark=spark,
                        path=file_path,
                        schema_csv=SCHEMA_TASK_5_1,
                        header='true')

    result_df = quarter_visit_hospital(month_df=month_df)

    write_result_df(df=result_df, header='true', file_path=RESULT_TASK5_1_DATA_FILE)
