import sys

from deprecated.old_tests.utils import assert_df_equal, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_DATA_TASK_5_1, RESULT_DF_TASK_5
from deprecated.old_task_solver import TEST_DATA_TASK5_1, RESULT_TEST_DATA_TASK5_1
from deprecated.old_task_solver import quarter_visit_hospital

sys.path.append('/')


def test_quarter_visit_hospital(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_TEST_DATA_TASK5_1,
                                  schema_csv=RESULT_DF_TASK_5,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=TEST_DATA_TASK5_1,
                                 schema_csv=SCHEMA_DATA_TASK_5_1,
                                 header='true')

    actual_df = quarter_visit_hospital(month_df=input_data)

    assert_df_equal(expected_df, actual_df)
