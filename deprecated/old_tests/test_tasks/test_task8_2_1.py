import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_json, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_TASK_8_2_1, SCHEMA_EXPECTED_TASK_8_2_1
from deprecated.old_task_solver import TEST_DATA_TASK_8_2_1, FREE_WIFI_TASK_8_2_1, RESULT_TEST_DATA_TASK_8_2_1
from deprecated.old_task_solver import find_business_wifi, solv_task_8_2_1

sys.path.append('/')


def test_find_business_wifi(spark):
    expected_df = reader_test_csv(spark,
                                  path=FREE_WIFI_TASK_8_2_1,
                                  schema_csv=SCHEMA_EXPECTED_TASK_8_2_1,
                                  header='true')

    input_data = reader_test_json(spark,
                                  path=TEST_DATA_TASK_8_2_1,
                                  schema_json=SCHEMA_TASK_8_2_1,
                                  header='true')

    actual_df = find_business_wifi(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task8_2_1.find_business_wifi')
def test_solv_task_8_2_1(mock_wifi, spark):
    df_with_wifi = reader_test_csv(spark,
                                   path=FREE_WIFI_TASK_8_2_1,
                                   schema_csv=SCHEMA_EXPECTED_TASK_8_2_1,
                                   header='true')

    mock_wifi.return_value = df_with_wifi

    solv_task_8_2_1(spark=spark, file_path=TEST_DATA_TASK_8_2_1, save_file_path=RESULT_TEST_DATA_TASK_8_2_1)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK_8_2_1)
    assert result_file.exists() is True

    expected_read_df = reader_test_csv(spark,
                                       path=RESULT_TEST_DATA_TASK_8_2_1,
                                       schema_csv=SCHEMA_EXPECTED_TASK_8_2_1,
                                       header='true')

    assert_df_equal(expected_read_df, mock_wifi())
