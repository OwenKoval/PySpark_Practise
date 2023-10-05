import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_json, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_DATA_TASK_8_1, SCHEMA_EXPECTED_TASK_8_1, RESULT_TEST_TASK_8_1
from deprecated.old_task_solver import TEST_DATA_TASK_8_1, WORKING_HOURS_TASK_8_1, \
    RESULT_DATA_TASK_8_1, RESULT_TEST_DATA_TASK8_1
from deprecated.old_task_solver import find_working_hours, find_business, solv_task_8_1


sys.path.append('/')


def test_find_working_hours(spark):
    expected_df = reader_test_csv(spark,
                                  path=WORKING_HOURS_TASK_8_1,
                                  schema_csv=SCHEMA_EXPECTED_TASK_8_1,
                                  header='true')

    input_data = reader_test_json(spark,
                                  path=TEST_DATA_TASK_8_1,
                                  schema_json=SCHEMA_DATA_TASK_8_1,
                                  header='true')

    actual_df = find_working_hours(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


def test_find_business(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_DATA_TASK_8_1,
                                  schema_csv=RESULT_TEST_TASK_8_1,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=WORKING_HOURS_TASK_8_1,
                                 schema_csv=SCHEMA_EXPECTED_TASK_8_1,
                                 header='true')

    actual_df = find_business(prepared_df=input_data)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task8_1.find_working_hours')
@patch('task_solver.solv_task8_1.find_business')
def test_solv_task_8_1(mock_find_business, mock_working_hours, spark):
    df_working_hours = reader_test_csv(spark,
                                       path=WORKING_HOURS_TASK_8_1,
                                       schema_csv=SCHEMA_EXPECTED_TASK_8_1,
                                       header='true')

    df_find_business = reader_test_csv(spark,
                                       path=RESULT_DATA_TASK_8_1,
                                       schema_csv=RESULT_TEST_TASK_8_1,
                                       header='true')

    mock_working_hours.return_value = df_working_hours
    mock_find_business.return_value = df_find_business

    solv_task_8_1(spark=spark, file_path=TEST_DATA_TASK_8_1, save_file_path=RESULT_TEST_DATA_TASK8_1)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK8_1)
    assert result_file.exists() is True

    expected_read_df = reader_test_csv(spark,
                                       path=RESULT_TEST_DATA_TASK8_1,
                                       schema_csv=RESULT_TEST_TASK_8_1,
                                       header='true')

    assert_df_equal(expected_read_df, mock_find_business())
