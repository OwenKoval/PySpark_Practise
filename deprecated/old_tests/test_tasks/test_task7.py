import sys
import pathlib
from unittest.mock import patch

from deprecated.old_tests.utils import assert_df_equal, reader_test_csv
from deprecated.old_tests.schemas import SCHEMA_DATA_TASK_7, PREPARED_DF_TASK_7, LONGEST_DF_TASK_7, \
    SCHEMA_LONGEST_AFTER_DF_TASK_7, RESULT_DF_TASK_7
from deprecated.old_task_solver import TEST_TASK7_DATA, PREPARED_TASK7_DATA, LONGEST_VISITS_DF_TASK_7, \
    LONGEST_AFTER_DF_TASK_7, RESULT_DATA_TASK_7, RESULT_TEST_FILE_TASK_7
from deprecated.old_task_solver.solv_task7 import preparing_visits_df, longest_visits_to_end_date, \
    longest_visits_after_end_date, visits_to_since_end_date, solv_task_7


sys.path.append('/')


def test_preparing_visits_df(spark):
    expected_df = reader_test_csv(spark,
                                  path=PREPARED_TASK7_DATA,
                                  schema_csv=PREPARED_DF_TASK_7,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=TEST_TASK7_DATA,
                                 schema_csv=SCHEMA_DATA_TASK_7,
                                 header='true')

    actual_df = preparing_visits_df(dataframe=input_data)

    assert_df_equal(expected_df, actual_df)


def test_longest_visits_to_end_date(spark):
    expected_df = reader_test_csv(spark,
                                  path=LONGEST_VISITS_DF_TASK_7,
                                  schema_csv=LONGEST_DF_TASK_7,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=PREPARED_TASK7_DATA,
                                 schema_csv=PREPARED_DF_TASK_7,
                                 header='true')

    actual_df = longest_visits_to_end_date(prepared_df=input_data)

    assert_df_equal(expected_df, actual_df)


def test_longest_visits_after_end_date(spark):
    expected_df = reader_test_csv(spark,
                                  path=LONGEST_AFTER_DF_TASK_7,
                                  schema_csv=SCHEMA_LONGEST_AFTER_DF_TASK_7,
                                  header='true')

    input_data = reader_test_csv(spark,
                                 path=PREPARED_TASK7_DATA,
                                 schema_csv=PREPARED_DF_TASK_7,
                                 header='true')

    actual_df = longest_visits_after_end_date(prepared_df=input_data)

    assert_df_equal(expected_df, actual_df)


def test_visits_to_since_end_date(spark):
    expected_df = reader_test_csv(spark,
                                  path=RESULT_DATA_TASK_7,
                                  schema_csv=RESULT_DF_TASK_7,
                                  header='true')

    visits_after_end_date = reader_test_csv(spark,
                                            path=LONGEST_AFTER_DF_TASK_7,
                                            schema_csv=SCHEMA_LONGEST_AFTER_DF_TASK_7,
                                            header='true')

    visits_to_end_date = reader_test_csv(spark,
                                         path=LONGEST_VISITS_DF_TASK_7,
                                         schema_csv=LONGEST_DF_TASK_7,
                                         header='true')

    actual_df = visits_to_since_end_date(prepared_longest_df=visits_to_end_date,
                                         prepared_longest_after_df=visits_after_end_date)

    assert_df_equal(expected_df, actual_df)


@patch('task_solver.solv_task7.visits_to_since_end_date')
@patch('task_solver.solv_task7.preparing_visits_df')
@patch('task_solver.solv_task7.longest_visits_to_end_date')
@patch('task_solver.solv_task7.longest_visits_after_end_date')
def test_solv_task_7(mock_prepared, mock_longest_to, mock_longest_after, mock_all_visits, spark):
    prepared_df = reader_test_csv(spark,
                                  path=PREPARED_TASK7_DATA,
                                  schema_csv=PREPARED_DF_TASK_7,
                                  header='true')

    longest_to_end_date = reader_test_csv(spark,
                                          path=LONGEST_VISITS_DF_TASK_7,
                                          schema_csv=LONGEST_DF_TASK_7,
                                          header='true')

    longest_after_end_date = reader_test_csv(spark,
                                             path=LONGEST_AFTER_DF_TASK_7,
                                             schema_csv=SCHEMA_LONGEST_AFTER_DF_TASK_7,
                                             header='true')

    longest_after_to_end_date = reader_test_csv(spark,
                                                path=RESULT_DATA_TASK_7,
                                                schema_csv=RESULT_DF_TASK_7,
                                                header='true')

    mock_prepared.return_value = prepared_df
    mock_longest_to.return_value = longest_to_end_date
    mock_longest_after.return_value = longest_after_end_date
    mock_all_visits.return_value = longest_after_to_end_date

    solv_task_7(spark=spark, file_path=TEST_TASK7_DATA, save_file_path=RESULT_TEST_FILE_TASK_7)

    result_file = pathlib.Path(RESULT_TEST_FILE_TASK_7)
    assert result_file.exists() is True

    expected_read_df = reader_test_csv(spark,
                                       path=RESULT_TEST_FILE_TASK_7,
                                       schema_csv=RESULT_DF_TASK_7,
                                       header='true')

    assert_df_equal(expected_read_df, mock_all_visits())
