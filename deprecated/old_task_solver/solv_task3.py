"""
Task 3 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window

from deprecated.old_task_solver.const import DATE_FORMAT, DATE_FORMAT_FULL, RESULT_FILE_TASK_3
from deprecated.old_task_solver.schemas import SCHEMA_RATIO
from utils import read_csv, write_result_df_json


def preparing_ratio_df(ratio_df, num=37):
    """
    Casting the columns to needed format
    Preparing ratio_df
    Cast the needed column to DateType
    :param ratio_df: dataframe for casting
    :param num: number of ratio in file
    :return casted_df: casted dataframe to DateType
    """

    selected_column = ['index', 'ratio_key', 'facility_id', 'medicare_id',
                       'start_date', 'end_date', 'valid_flag', 'ratios']

    combine = f.udf(lambda x, y: list(zip(x, y)),
                    t.ArrayType(t.StructType([t.StructField('zipped_rev', t.DoubleType()),
                                              t.StructField('zipped_chg', t.DoubleType())])))

    casted_ratio_df = (ratio_df
                       .withColumn('start_date', f.to_date(f.col('start_date'), DATE_FORMAT))
                       .withColumn('end_date', f.to_date(f.col('end_date'), DATE_FORMAT))
                       .withColumn('zipped_rat',
                                   f.array(*(f.col(c).alias(c) for c in [f'ratio_{s}' for s in range(1, num)])))
                       .withColumn('zipped_drat',
                                   f.array(*(f.col(c).alias(c) for c in [f'dratio_{s}' for s in range(1, num)])))
                       # pylint: disable=redundant-keyword-arg
                       .withColumn('ratios', combine('zipped_rat', 'zipped_drat'))
                       .withColumn('facility_id', f.regexp_extract(f.col('ratio_key'), '(.)(F)(.*)', 3))
                       .withColumn('facility_id',
                                   f.when(f.col('facility_id') == '', None).otherwise(f.col('facility_id')))
                       .withColumn('medicare_id', f.regexp_extract(f.col('ratio_key'), '(.)(M)(.*)', 3))
                       .withColumn('medicare_id',
                                   f.when(f.col('medicare_id') == '', None).otherwise(f.col('medicare_id')))
                       .select(*selected_column)
                       .where(f.col('valid_flag') > 3))

    return casted_ratio_df


def preparing_hospital_enc_df(he_df_cast):
    """
    Preparing hospital_enc_df
    Selecting the needed columns and casting discharge_date to date formant
    :param he_df_cast: csv_dataframe for casting column
    :return: casted DataFrame
    """
    prepared_henc_df = (he_df_cast
                        .withColumn('discharge_date', f.to_date(f.col('discharge_date'), DATE_FORMAT_FULL))
                        .withColumn('fac_medicare_id', f.col('fac_medicare_id').cast(t.StringType()))
                        .withColumn('record_identifier', f.col('record_identifier').cast(t.StringType())))

    selected_he_df = (prepared_henc_df.select(
        f.col('record_identifier'),
        f.col('facility_id'),
        f.col('fac_medicare_id'),
        f.col('discharge_date'),
        f.col('patient_id')))

    return selected_he_df


def join_hosp_enc_and_ratios(ratio_df, henc_df):
    """
    Joining two df and select the needed column them fill Null like 0
    Join first and second df by number and record_identify
    :param henc_df: prepared hospitalenc_dataframe
    :param ratio_df: prepared ratio_dtaframe
    :return: return the joined dataframe
    """

    grouped = Window().partitionBy('record_identifier').orderBy('year_rate')

    joined_df = (henc_df
                 .join(ratio_df, on=(((henc_df['facility_id'] == ratio_df['facility_id']) |
                                      (henc_df['fac_medicare_id'] == ratio_df['medicare_id'])) &
                                     (henc_df['discharge_date'].between(
                                         f.add_months(ratio_df['start_date'], -36),
                                         f.add_months(ratio_df['end_date'], 36)))), how='left')
                 .drop(ratio_df['facility_id'])
                 .drop(ratio_df['medicare_id'])
                 .withColumn('year_rate',
                             f.when(f.col('discharge_date').between(
                                 f.col('start_date'),
                                 f.col('end_date')), 0)
                             .when(f.col('discharge_date').between(
                                 f.add_months(f.col('start_date'), -12),
                                 f.add_months(f.col('end_date'), 12)), 1)
                             .when(f.col('discharge_date').between(
                                 f.add_months(f.col('start_date'), -24),
                                 f.add_months(f.col('end_date'), 24)), 2)
                             .when(f.col('discharge_date').between(
                                 f.add_months(f.col('start_date'), -36),
                                 f.add_months(f.col('end_date'), 36)), 3))
                 .withColumn('rank', f.rank().over(grouped))
                 .where(f.col('rank') == 1)
                 .drop(f.col('year_rate'))
                 .drop(f.col('rank'))
                 .orderBy(f.col('record_identifier')))

    return joined_df


def solv_task3(spark,
               ratio_path,
               hospitalenc_path
               ):
    """
        Calling all functions which needed to solv the task
        :param spark: SparkSession
        :param ratio_path: path to raio file
        :param hospitalenc_path: path to he.csv file
        :return: Result file and dataframe
        """
    revtobucket_df = read_csv(spark=spark,
                              path=ratio_path,
                              schema_csv=SCHEMA_RATIO,
                              sep=';')

    hosp_enc_df = read_csv(spark=spark,
                           path=hospitalenc_path,
                           header='true',
                           inferschema='true')

    ratio_df = preparing_ratio_df(ratio_df=revtobucket_df)

    hospital_enc_df = preparing_hospital_enc_df(he_df_cast=hosp_enc_df)

    result_df = join_hosp_enc_and_ratios(ratio_df=ratio_df, henc_df=hospital_enc_df)

    write_result_df_json(df=result_df, header='true', file_path=RESULT_FILE_TASK_3)
