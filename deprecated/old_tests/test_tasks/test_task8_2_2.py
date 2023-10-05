import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_json, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_TASK_8_2_2, SCHEMA_EXPECTED_TASK_8_2_2
from deprecated.old_task_solver import TEST_DATA_TASK_8_2_2, PARKING_TASK_8_2_2, RESULT_TEST_DATA_TASK_8_2_2
from deprecated.old_task_solver import find_parking_business, solv_task_8_2_2

sys.path.append('/')


def test_find_parking_business(spark):
    expected_df = reader_test_csv(spark,
                                  path=PARKING_TASK_8_2_2,
                                  schema_csv=SCHEMA_EXPECTED_TASK_8_2_2,
                                  header='true')

    input_data = reader_test_json(spark,
                                  path=TEST_DATA_TASK_8_2_2,
                                  schema_json=SCHEMA_TASK_8_2_2,
                                  header='true')

    actual_df = find_parking_business(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task8_2_2.find_parking_business')
def test_solv_task_8_2_2(mock_parking, spark):
    df_parking = reader_test_csv(spark,
                                 path=PARKING_TASK_8_2_2,
                                 schema_csv=SCHEMA_EXPECTED_TASK_8_2_2,
                                 header='true')

    mock_parking.return_value = df_parking

    solv_task_8_2_2(spark=spark, file_path=TEST_DATA_TASK_8_2_2, save_file_path=RESULT_TEST_DATA_TASK_8_2_2)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK_8_2_2)
    assert result_file.exists() is True

    expected_read_df = reader_test_csv(spark,
                                       path=RESULT_TEST_DATA_TASK_8_2_2,
                                       schema_csv=SCHEMA_EXPECTED_TASK_8_2_2,
                                       header='true')

    assert_df_equal(expected_read_df, mock_parking())
