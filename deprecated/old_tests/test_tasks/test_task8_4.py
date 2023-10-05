import sys
import pathlib

from deprecated.old_tests.utils import assert_df_equal, reader_test_json
from deprecated.old_tests.schemas import SCHEMA_USERS, SCHEMA_REVIEW, SCHEMA_BUSINESS, SCHEMA_EXPECTED_TASK_8_4, \
    SCHEMA_EXPECTED_BUSINESSES, SCHEMA_EXPECTED_8_4
from deprecated.old_task_solver import TEST_USERS_DATA, TEST_REVIEWS_DATA, TEST_TIPS_DATA, USERS_FRIENDS_DATA, \
    RESULT_TEST_DATA_TASK_8_4, TEST_BUSINESSES_DATA, USERS_BUSINESSES_DATA, RESULT_USERS_DATA, EXPECTED_RESULT_8_4
from deprecated.old_task_solver.solv_task8_4 import find_users_friends_checkins, find_business_info, find_friends_info, solv_task_8_4

sys.path.append('/')


def test_find_friends(spark):
    expected_df = reader_test_json(spark,
                                   path=USERS_FRIENDS_DATA,
                                   schema_json=SCHEMA_EXPECTED_TASK_8_4,
                                   header='true')

    input_users_data = reader_test_json(spark,
                                        path=TEST_USERS_DATA,
                                        schema_json=SCHEMA_USERS,
                                        header='true')

    input_reviews_data = reader_test_json(spark,
                                          path=TEST_REVIEWS_DATA,
                                          schema_json=SCHEMA_REVIEW,
                                          header='true')

    input_tips_data = reader_test_json(spark,
                                       path=TEST_TIPS_DATA,
                                       schema_json=SCHEMA_REVIEW,
                                       header='true')

    actual_df = find_users_friends_checkins(users_df=input_users_data,
                                            review_df=input_reviews_data,
                                            tips_df=input_tips_data)

    assert_df_equal(expected_df, actual_df)


def test_find_business_info(spark):
    expected_df = reader_test_json(spark,
                                   path=USERS_BUSINESSES_DATA,
                                   schema_json=SCHEMA_EXPECTED_BUSINESSES,
                                   header='true')

    input_data = reader_test_json(spark,
                                  path=USERS_FRIENDS_DATA,
                                  schema_json=SCHEMA_EXPECTED_TASK_8_4,
                                  header='true')

    input_business_data = reader_test_json(spark,
                                           path=TEST_BUSINESSES_DATA,
                                           schema_json=SCHEMA_BUSINESS,
                                           header='true')

    actual_df = find_business_info(users_friends_checkins_df=input_data, business_df=input_business_data)

    assert_df_equal(expected_df, actual_df)


def test_find_friends_info(spark):
    expected_df = reader_test_json(spark,
                                   path=RESULT_USERS_DATA,
                                   schema_json=SCHEMA_EXPECTED_8_4,
                                   header='true')

    input_data = reader_test_json(spark,
                                  path=USERS_BUSINESSES_DATA,
                                  schema_json=SCHEMA_EXPECTED_BUSINESSES,
                                  header='true')

    input_users_data = reader_test_json(spark,
                                        path=TEST_USERS_DATA,
                                        schema_json=SCHEMA_USERS,
                                        header='true')

    actual_df = find_friends_info(users_business_df=input_data, users_df=input_users_data)

    assert_df_equal(expected_df, actual_df)


def test_solv_task_8_4(spark):
    df_users_friends = reader_test_json(spark,
                                        path=EXPECTED_RESULT_8_4,
                                        schema_json=SCHEMA_EXPECTED_8_4,
                                        header='true')

    solv_task_8_4(spark=spark,
                  file_path_users=TEST_USERS_DATA,
                  file_path_review=TEST_REVIEWS_DATA,
                  file_path_tips=TEST_TIPS_DATA,
                  file_path_businesses=TEST_BUSINESSES_DATA,
                  save_file_path=RESULT_TEST_DATA_TASK_8_4)

    result_file = pathlib.Path(RESULT_TEST_DATA_TASK_8_4)
    assert result_file.exists() is True

    expected_read_df = reader_test_json(spark,
                                        path=RESULT_TEST_DATA_TASK_8_4,
                                        schema_json=SCHEMA_EXPECTED_8_4,
                                        header='true')

    assert_df_equal(df_users_friends, expected_read_df)
