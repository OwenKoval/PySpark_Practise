"""
Task 2 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql import types as t

from deprecated.old_task_solver.const import NUMBER_CODS, RESULT_FILE, DATE_FORMAT
from deprecated.old_task_solver.schemas import SCHEMA_REVTOBUCKET
from utils import read_csv, write_result_df


def preparing_revbckt_df(revbckt_df_cast):
    """
    Casting the columns to needed format
    Preparing revbckt_df
    Cast the needed column to DateType
    :param revbckt_df_cast: dataframe for casting
    :return casted_df: casted dataframe to DateType
    """
    revbckt_df_preperade = (revbckt_df_cast
                            .withColumn('begin_date', f.to_date(f.col('begin_date'), DATE_FORMAT))
                            .withColumn('end_date', f.to_date(f.col('end_date'), DATE_FORMAT))
                            .withColumn('rev_code', revbckt_df_cast['rev_code'].cast(t.IntegerType())))

    return revbckt_df_preperade


def preparing_hospital_enc_df(he_df_cast, num=NUMBER_CODS):
    """
    Preparing hospital_enc_df
    Selecting the needed columns and casting discharge_date to date formant
    :param num: tae number of columns
    :param he_df_cast: csv_dataframe for casting column
    :return: casted DataFrame
    """
    rev_codes = [f'rev_code{s}' for s in range(1, num)]
    chg_codes = [f'chg{s}' for s in range(1, num)]

    he_df_casted = he_df_cast.select(
        'record_identifier',
        'discharge_date',
        *(f.col(c).alias(c) for c in rev_codes),
        *(f.col(c).alias(c) for c in chg_codes))

    he_df_arrayed = (he_df_casted
                     .withColumn('discharge_date', f.to_date(f.col('discharge_date'), 'MMddyyyy'))
                     .withColumn('zipped_rev', f.array(*(f.col(c).alias(c) for c in rev_codes)))
                     .withColumn('zipped_chg', f.array(*(f.col(c).alias(c) for c in chg_codes))))

    combine = f.udf(lambda x, y: list(zip(x, y)),
                    t.ArrayType(t.StructType([t.StructField('zipped_rev', t.StringType()),
                                              t.StructField('zipped_chg', t.StringType())])))

    he_df_zipped = (he_df_arrayed
                    .select('record_identifier', 'discharge_date', 'zipped_rev', 'zipped_chg')
                    # pylint: disable=redundant-keyword-arg
                    .withColumn('new', combine('zipped_rev', 'zipped_chg'))
                    .withColumn('new', f.explode('new')))

    he_df_filtered = (he_df_zipped
                      .select('record_identifier',
                              'discharge_date',
                              f.col('new.zipped_rev').alias('rev_code'),
                              f.col('new.zipped_chg').alias('chg'))
                      .withColumn('chg', f.col('chg').cast(t.DecimalType(scale=2)))
                      .withColumn('rev_code', f.col('rev_code').cast(t.IntegerType()))
                      .where(f.col('rev_code').isNotNull()))

    return he_df_filtered


def income_for_service(csv_dataframe, bucket_dataframe):
    """
        Joining two df and select the needed column them fill Null like 0
        Join first and second df by number and record_identify
        :param csv_dataframe: prepared csv_dataframe
        :param bucket_dataframe: prepared bucket_dataframe
        :return: return the joined dataframe
        """

    joined_two_df = (csv_dataframe
                     .join(bucket_dataframe, on='rev_code', how='inner')
                     .where(f.col('discharge_date').between(f.col('begin_date'), f.col('end_date'))))

    finally_df = (joined_two_df
                  .groupBy('rev_code')
                  .agg(f.sum('chg'))
                  .orderBy('rev_code'))

    return finally_df


def solv_task2(spark,
               revtbckt_path,
               he_path, num):
    """
    Calling all functions which needed to solv the task
    :param spark: SparkSession
    :param revtbckt_path: path to revtobucket file
    :param he_path: path to he.csv file
    :param num: number of chg and rv_code columns
    :return: Result file and dataframe
    """
    revtobucket_df = read_csv(spark=spark,
                              path=revtbckt_path,
                              schema_csv=SCHEMA_REVTOBUCKET,
                              sep=';')

    he_df = read_csv(spark=spark, path=he_path, header='true', inferschema='true')

    prepared_revbck_df = preparing_revbckt_df(revbckt_df_cast=revtobucket_df)

    prepared_he_df = preparing_hospital_enc_df(he_df_cast=he_df, num=num)

    result_df = income_for_service(
        csv_dataframe=prepared_he_df,
        bucket_dataframe=prepared_revbck_df)

    write_result_df(df=result_df, file_path=RESULT_FILE)
