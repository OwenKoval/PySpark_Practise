import sys

from deprecated.old_task_solver import preparing_ratio_df, preparing_hospital_enc_df, join_hosp_enc_and_ratios
from deprecated.old_task_solver import HENC_TEST_FILE, RAT_TEST_FILE, PREPARED_RATIO_FILE, \
    PREPARED_HENC_FILE, RESULT_TASK3_FILE, PREPARED_HENC_FOR_JOIN
from deprecated.old_tests.utils import assert_df_equal, reader_test_csv, reader_test_json
from deprecated.old_tests.schemas import SCHEMA_RATIO_DF, SCHEMA_PREPEDER_RAT_DF, SCHEMA_PREPEDER_HE_DF, \
    SCHEMA_RESULT_TASK3

sys.path.append('/')


def test_preparing_ratio_df(spark):
    expected_df = reader_test_json(spark,
                                   path=PREPARED_RATIO_FILE,
                                   schema_json=SCHEMA_PREPEDER_RAT_DF,
                                   header='true')

    input_data = reader_test_csv(spark=spark,
                                 path=RAT_TEST_FILE,
                                 schema_csv=SCHEMA_RATIO_DF,
                                 sep=';')

    actual_df = preparing_ratio_df(ratio_df=input_data, num=3)

    assert_df_equal(expected_df, actual_df)


def test_preparing_hospital_enc_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_HENC_FILE,
                                  schema_csv=SCHEMA_PREPEDER_HE_DF,
                                  header='true')

    input_data = reader_test_csv(spark=spark,
                                 path=HENC_TEST_FILE,
                                 header='true',
                                 inferschema='true')

    actual_df = preparing_hospital_enc_df(he_df_cast=input_data)

    assert_df_equal(expected_df, actual_df)


def test_join_hosp_enc_and_ratios(spark):
    expected_df = reader_test_json(spark,
                                   path=RESULT_TASK3_FILE,
                                   schema_json=SCHEMA_RESULT_TASK3,
                                   header='true')

    ratio_df = reader_test_json(spark,
                                path=PREPARED_RATIO_FILE,
                                schema_json=SCHEMA_PREPEDER_RAT_DF,
                                header='true')

    henc_df = reader_test_csv(spark,
                              path=PREPARED_HENC_FOR_JOIN,
                              schema_csv=SCHEMA_PREPEDER_HE_DF,
                              header='true')

    actual_df = join_hosp_enc_and_ratios(ratio_df=ratio_df, henc_df=henc_df)

    assert_df_equal(expected_df, actual_df)
