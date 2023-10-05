import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_json
from deprecated.old_tests.schemas import SCHEMA_TASK_8_2_3, SCHEMA_EXPECTED_TASK_8_2_3
from deprecated.old_task_solver import TEST_DATA_TASK_8_2_3, GAS_STATION_TASK_8_2_3, RESULT_TEST_DATA_TASK_8_2_3
from deprecated.old_task_solver.solv_task8_2_3 import find_gas_station, solv_task_8_2_3

sys.path.append('/')


def test_find_gas_station(spark):
    expected_df = reader_test_json(spark,
                                   path=GAS_STATION_TASK_8_2_3,
                                   schema_json=SCHEMA_EXPECTED_TASK_8_2_3,
                                   header='true')

    input_data = reader_test_json(spark,
                                  path=TEST_DATA_TASK_8_2_3,
                                  schema_json=SCHEMA_TASK_8_2_3,
                                  header='true')

    actual_df = find_gas_station(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task8_2_3.find_gas_station')
def test_solv_task_8_2_3(mock_gas_station, spark):
    df_gas_station = reader_test_json(spark,
                                      path=GAS_STATION_TASK_8_2_3,
                                      schema_json=SCHEMA_EXPECTED_TASK_8_2_3,
                                      header='true')

    mock_gas_station.return_value = df_gas_station

    solv_task_8_2_3(spark=spark, file_path=TEST_DATA_TASK_8_2_3, save_file_path=RESULT_TEST_DATA_TASK_8_2_3)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK_8_2_3)
    assert result_file.exists() is True

    expected_read_df = reader_test_json(spark,
                                        path=RESULT_TEST_DATA_TASK_8_2_3,
                                        schema_json=SCHEMA_EXPECTED_TASK_8_2_3,
                                        header='true')

    assert_df_equal(expected_read_df, mock_gas_station())
