"""
Task 4 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.const import RESULT_FILE_TASK_4
from deprecated.old_task_solver.schemas import SCHEMA_TASK_DF4
from utils import read_csv, write_result_df


def preparing_code_df(pation_df, code_naming):
    """
    :param pation_df: df for preparing
    :param code_naming: name of code, which will partitioned
    :return: prepared dataframe
    """

    df_code = pation_df.groupBy('patient_id', code_naming).count()

    grouped_proc_code = Window().partitionBy('patient_id').orderBy('patient_id', f.desc('count'))

    result_df = (df_code
                 .withColumn('order', f.row_number().over(grouped_proc_code))
                 .where(f.col('order') == 1)
                 .orderBy('patient_id'))

    return result_df


def popular_proc_fac_cods(proc_df, fac_df):
    """
    :param proc_df: prepared proc_df
    :param fac_df: prepared fac_df
    :return: df with the most popular proc and fac code in df
    """
    finally_df = (proc_df
                  .join(fac_df, on='patient_id', how='inner'))

    result_df = (finally_df
                 .select('patient_id', 'proc_code', 'fac_prof')
                 .orderBy('patient_id'))

    return result_df


def solv_task4(spark,
               file_path):
    """
        Calling all functions which needed to solv the task
        :param spark: SparkSession
        :param file_path: path to raio file
        :return: Result file and dataframe
        """
    pation_df = read_csv(spark=spark,
                         path=file_path,
                         schema_csv=SCHEMA_TASK_DF4,
                         header='true')

    df_proc_code = preparing_code_df(pation_df=pation_df, code_naming='proc_code')

    df_fac_prof = preparing_code_df(pation_df=pation_df, code_naming='fac_prof')

    result_df = popular_proc_fac_cods(proc_df=df_proc_code, fac_df=df_fac_prof)

    write_result_df(df=result_df, header='true', file_path=RESULT_FILE_TASK_4)
