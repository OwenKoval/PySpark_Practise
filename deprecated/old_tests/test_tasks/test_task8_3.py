import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_json, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_TASK_8_3, SCHEMA_EXPECTED_TASK_8_3
from deprecated.old_task_solver import TEST_DATA_TASK_8_3, CHECKIN_TASK_8_3, RESULT_TEST_DATA_TASK_8_3
from deprecated.old_task_solver.solv_task8_3 import find_checkin_business, solv_task_8_3

sys.path.append('/')


def test_find_checkin_business(spark):
    expected_df = reader_test_csv(spark,
                                  path=CHECKIN_TASK_8_3,
                                  schema_csv=SCHEMA_EXPECTED_TASK_8_3,
                                  header='true')

    input_data = reader_test_json(spark,
                                  path=TEST_DATA_TASK_8_3,
                                  schema_json=SCHEMA_TASK_8_3,
                                  header='true')

    actual_df = find_checkin_business(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task8_3.find_checkin_business')
def test_solv_task_8_2_3(mock_checkin, spark):
    df_checkin = reader_test_csv(spark,
                                 path=CHECKIN_TASK_8_3,
                                 schema_csv=SCHEMA_EXPECTED_TASK_8_3,
                                 header='true')

    mock_checkin.return_value = df_checkin

    solv_task_8_3(spark=spark, file_path=TEST_DATA_TASK_8_3, save_file_path=RESULT_TEST_DATA_TASK_8_3)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK_8_3)
    assert result_file.exists() is True

    expected_read_df = reader_test_csv(spark,
                                       path=RESULT_TEST_DATA_TASK_8_3,
                                       schema_csv=SCHEMA_EXPECTED_TASK_8_3,
                                       header='true')

    assert_df_equal(expected_read_df, mock_checkin())
