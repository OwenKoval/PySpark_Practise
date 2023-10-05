import sys

from deprecated.old_tests.utils import assert_df_equal, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_TASK4_DF, SCHEMA_PREPARED_PROC_DF, SCHEMA_PREPARED_FAC_DF, \
    SCHEMA_RESULT_PROC_FAC_DF
from deprecated.old_task_solver import preparing_code_df, popular_proc_fac_cods
from deprecated.old_task_solver import TEST_DATA_TASK4, PREPARED_PROC_DATA, PREPARED_FAC_DATA, \
    PREPARED_PROC_DF, PREPARED_FAC_DF, PREPARED_RESULT_PROC_FAC_DF

sys.path.append('/')


def test_preparing_proc_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_PROC_DATA,
                                  header='true',
                                  schema_csv=SCHEMA_PREPARED_PROC_DF)

    input_data = reader_test_csv(spark,
                                 path=TEST_DATA_TASK4,
                                 header='true',
                                 schema_csv=SCHEMA_TASK4_DF)

    actual_df = preparing_code_df(pation_df=input_data, code_naming='proc_code')

    assert_df_equal(expected_df, actual_df)


def test_preparing_fac_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_FAC_DATA,
                                  header='true',
                                  schema_csv=SCHEMA_PREPARED_FAC_DF)

    input_data = reader_test_csv(spark,
                                 path=TEST_DATA_TASK4,
                                 header='true',
                                 schema_csv=SCHEMA_TASK4_DF)

    actual_df = preparing_code_df(pation_df=input_data, code_naming='fac_prof')

    assert_df_equal(expected_df, actual_df)


def test_popular_proc_fac_cods(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_RESULT_PROC_FAC_DF,
                                  header='true',
                                  schema_csv=SCHEMA_RESULT_PROC_FAC_DF)

    proc_data = reader_test_csv(spark,
                                path=PREPARED_PROC_DF,
                                header='true',
                                schema_csv=SCHEMA_PREPARED_PROC_DF)

    fac_data = reader_test_csv(spark,
                               path=PREPARED_FAC_DF,
                               header='true',
                               schema_csv=SCHEMA_PREPARED_FAC_DF)

    actual_df = popular_proc_fac_cods(proc_df=proc_data, fac_df=fac_data)

    assert_df_equal(expected_df, actual_df)
