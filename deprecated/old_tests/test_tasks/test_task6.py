import sys

from deprecated.old_tests.utils import assert_df_equal, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_DATA_TASK_6, RESULT_DATA_TASK_6
from deprecated.old_task_solver import TEST_TASK6_DATA, RESULT_TEST_TASK6_DATA
from deprecated.old_task_solver.solv_task6 import check_visits_months

sys.path.append('/')


def test_check_visits_months(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_TEST_TASK6_DATA,
                                  schema_csv=RESULT_DATA_TASK_6,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=TEST_TASK6_DATA,
                                 schema_csv=SCHEMA_DATA_TASK_6,
                                 header='true')

    actual_df = check_visits_months(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)
