import sys
from mock import patch

from task_solver.load_business_info import LoadInfoBusiness
from task_solver.const import TEST_BUSINESS_DATASET, TEST_USERS_DATASET, TEST_REVIEWS_DATASET, TEST_TIPS_DATASET, \
    TEST_CHECKIN_DATASET

from tests.schemas import SCHEMA_TEST_BUSINESS_DATASET, SCHEMA_EXPECTED_TEST_BUSINESS_DATASET, \
    SCHEMA_TEST_USERS_DATASET, SCHEMA_TEST_REVIEWS_TIPS_DATASET, SCHEMA_TEST_CHECKIN_DATASET
from tests.utils import assert_df_equal

sys.path.append('/')


@patch('task_solver.load_business_info.SCHEMA_BUSINESS_INFO_DATASET', SCHEMA_TEST_BUSINESS_DATASET)
@patch('task_solver.load_business_info.SCHEMA_USERS_DATASET', SCHEMA_TEST_USERS_DATASET)
@patch('task_solver.load_business_info.SCHEMA_REVIEWS_TIPS_DATASET', SCHEMA_TEST_REVIEWS_TIPS_DATASET)
@patch('task_solver.load_business_info.SCHEMA_CHECKIN_DATASET', SCHEMA_TEST_CHECKIN_DATASET)
@patch('task_solver.load_business_info.LoadInfoBusiness.__abstractmethods__', set())
def test_prepare_data(spark):
    find_business_info_obj = LoadInfoBusiness(business_path=TEST_BUSINESS_DATASET,
                                              users_path=TEST_USERS_DATASET,
                                              reviews_path=TEST_REVIEWS_DATASET,
                                              tips_path=TEST_TIPS_DATASET,
                                              checking_path=TEST_CHECKIN_DATASET)

    # TEST FOR BUSINESS DF
    expected_test_business_data = [
        ('1',
         'business_name1',
         'hello',
         {'Monday': '10:0-18:0',
          'Tuesday': '11:0-20:0',
          'Wednesday': '10:0-18:0',
          'Thursday': '11:0-20:0',
          'Friday': '11:0-20:0',
          'Saturday': '11:0-20:0',
          'Sunday': '13:0-18:0'},
         "{'garage': False, 'street': False, 'validated': False, 'lot': True, 'valet': False}",
         'True',
         'True',
         3),
        ('2',
         'business_name2',
         'text',
         {'Monday': '10:0-20:0',
          'Tuesday': '9:0-20:0',
          'Wednesday': '8:0-18:0',
          'Thursday': '7:0-20:0',
          'Friday': '6:0-20:0',
          'Sunday': '6:30-18:30'},
         "{'garage': False, 'street': False, 'validated': False}",
         None,
         None,
         1),
    ]
    expected_test_business_df = spark.createDataFrame(data=expected_test_business_data,
                                                      schema=SCHEMA_EXPECTED_TEST_BUSINESS_DATASET)
    assert_df_equal(find_business_info_obj.business_df, expected_test_business_df)

    # TEST FOR USERS DF
    expected_test_users_data = [
        ('1', 'user_name1', 'friend1, friend2, friend3, friend4'),
        ('2', 'user_name2', ''),
        ('3', 'user_name3', None),
        ('4', 'user_name4', 'friend1, friend2, , friend3, friend4'),
    ]
    expected_test_users_df = spark.createDataFrame(data=expected_test_users_data,
                                                   schema=['user_id', 'name', 'friends'])
    assert_df_equal(find_business_info_obj.users_df, expected_test_users_df)

    # TEST FOR REVIEWS DF
    expected_test_reviews_data = [
        (None, None),
        ('user1', '1'),
        ('user2', '2'),
        ('user3', None),
        (None, '4'),
    ]
    expected_test_reviews_df = spark.createDataFrame(data=expected_test_reviews_data,
                                                     schema=['user_id', 'business_id'])
    assert_df_equal(find_business_info_obj.reviews_df, expected_test_reviews_df)

    # TEST FOR TIPS DF
    expected_test_tips_data = [
        ('user5', '5'),
        (None, None),
        ('user6', '6'),
        (None, '7'),
        ('user8', None)
    ]
    expected_test_tips_df = spark.createDataFrame(data=expected_test_tips_data,
                                                  schema=['user_id', 'business_id'])
    assert_df_equal(find_business_info_obj.tips_df, expected_test_tips_df)

    # TEST FOR CHECKIN DF
    expected_test_checkin_data = [
        ('1', '2016-04-26 19:49:16, 2017-04-20 18:39:06, 2018-05-03 17:58:02, 2019-03-19 22:04:48')
    ]
    expected_test_checkin_df = spark.createDataFrame(data=expected_test_checkin_data,
                                                     schema=['business_id', 'date'])
    assert_df_equal(find_business_info_obj.checkin_df, expected_test_checkin_df)

    # TEST FOR REVISIONS DF
    expected_test_revisions_data = [
        ('user1', '1'),
        ('user2', '2'),
        ('user5', '5'),
        ('user6', '6'),
    ]
    expected_test_revisions_df = spark.createDataFrame(data=expected_test_revisions_data,
                                                       schema=['user_id', 'business_id'])
    assert_df_equal(find_business_info_obj.revisions_df, expected_test_revisions_df)
