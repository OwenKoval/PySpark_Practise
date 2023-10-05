import sys

from deprecated.old_tests.utils import assert_df_equal, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_DATA_TASK_5_2, SUMMED_SCHEMA_DATA_TASK_5_2, RESULT_DF_TASK_5
from deprecated.old_task_solver import TEST_TASK5_2_DATA, SUMMED_DATA_TASK5_2, TEST_SUMMED_DATA_TASK5_2, \
    RESULT_TEST_DATA_TASK5_2
from deprecated.old_task_solver import summ_date_df, quarter_visit_hospital

sys.path.append('/')


def test_summ_date_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=SUMMED_DATA_TASK5_2,
                                  schema_csv=SUMMED_SCHEMA_DATA_TASK_5_2,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=TEST_TASK5_2_DATA,
                                 schema_csv=SCHEMA_DATA_TASK_5_2,
                                 header='true')

    actual_df = summ_date_df(month_df=input_data)

    assert_df_equal(expected_df, actual_df)


def test_quarter_visit_hospital(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_TEST_DATA_TASK5_2,
                                  schema_csv=RESULT_DF_TASK_5,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=TEST_SUMMED_DATA_TASK5_2,
                                 schema_csv=SUMMED_SCHEMA_DATA_TASK_5_2,
                                 header='true')

    actual_df = quarter_visit_hospital(prepared_df=input_data)

    assert_df_equal(expected_df, actual_df)
