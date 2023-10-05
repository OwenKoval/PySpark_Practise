import sys

from deprecated.old_task_solver import REVTOBUCKET_TEST_FILE, HE_TEST_FILE, PREPARED_REVTBCKT_FILE, \
    PREPARED_HE_FILE, RESULT_TEST_FILE
from deprecated.old_task_solver import SCHEMA_REVTOBUCKET
from deprecated.old_task_solver import preparing_revbckt_df, preparing_hospital_enc_df, \
    income_for_service
from deprecated.old_tests.schemas import SCHEMA_PREPARING_REVBCKT_DF, SCHEMA_PREPARING_HE_DF, \
    SCHEMA_SUM_SELECT_DF
from deprecated.old_tests.utils import assert_df_equal, reader_test_csv

sys.path.append('/')


def test_preparing_revbckt_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_REVTBCKT_FILE,
                                  schema_csv=SCHEMA_PREPARING_REVBCKT_DF,
                                  sep=','
                                  )

    input_data = reader_test_csv(spark=spark,
                                 path=REVTOBUCKET_TEST_FILE,
                                 schema_csv=SCHEMA_REVTOBUCKET,
                                 sep=';')

    actual_df = preparing_revbckt_df(revbckt_df_cast=input_data)

    assert_df_equal(expected_df, actual_df)


def test_preparing_hospital_enc_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_HE_FILE,
                                  schema_csv=SCHEMA_PREPARING_HE_DF,
                                  header='true')

    input_data = reader_test_csv(spark=spark,
                                 path=HE_TEST_FILE,
                                 header='true',
                                 inferschema='true')

    actual_df = preparing_hospital_enc_df(he_df_cast=input_data, num=4)

    assert_df_equal(expected_df, actual_df)


def test_income_for_service(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_TEST_FILE,
                                  schema_csv=SCHEMA_SUM_SELECT_DF,
                                  header='true',
                                  sep=',')

    revtobucket_df = reader_test_csv(spark,
                                     path=PREPARED_REVTBCKT_FILE,
                                     schema_csv=SCHEMA_REVTOBUCKET,
                                     sep=',')

    he_df = reader_test_csv(spark=spark, path=PREPARED_HE_FILE, schema_csv=SCHEMA_PREPARING_HE_DF, header='true')

    result_df = income_for_service(csv_dataframe=he_df,
                                   bucket_dataframe=revtobucket_df)

    actual_df = result_df

    assert_df_equal(expected_df, actual_df)
