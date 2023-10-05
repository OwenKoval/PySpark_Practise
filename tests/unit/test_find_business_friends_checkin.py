import sys
from mock import patch

from task_solver.find_business_friends_checkin import FindBusinessFriendsCheckin
from tests.schemas import EXPECTED_BUSINESS_FRIENDS_CHECKIN_8_4
from tests.utils import assert_df_equal

sys.path.append('/')


def test_find_users_friends_checkins(spark):
    input_users_data = [
        ('1', 'user_name1', 'friend1, friend2, friend3, friend4'),
        ('2', 'user_name2', 'friend5, friend14, friend3, friend11'),
        ('3', 'user_name3', 'friend5, friend2, friend3, friend1'),
        ('4', 'user_name4', 'friend2, friend14'),
    ]

    input_users_df = spark.createDataFrame(data=input_users_data, schema=['user_id', 'name', 'friends'])

    input_revisions_data = [
        ('1', '1'),
        ('2', '3'),
        ('friend2', '1'),
        ('friend3', '3'),
        ('friend5', '2'),
        ('friend4', '1'),
        ('friend14', '2'),
        ('4', '2'),
        ('friend10', '5'),
        ('friend11', '3'),
    ]

    input_revisions_df = spark.createDataFrame(data=input_revisions_data, schema=['user_id', 'business_id'])

    expected_users_friends_checkins = [
        ('1', 'user_name1', 'friend2', '1'),
        ('1', 'user_name1', 'friend4', '1'),
        ('2', 'user_name2', 'friend3', '3'),
        ('2', 'user_name2', 'friend11', '3'),
        ('4', 'user_name4', 'friend14', '2')
    ]

    expected_users_friends_checkins_df = spark.createDataFrame(data=expected_users_friends_checkins,
                                                               schema=['user_id', 'user_name', 'friend_id',
                                                                       'business_id'])

    def __init__(self):
        self.spark = spark
        self.users_df = input_users_df
        self.revisions_df = input_revisions_df

    with patch('task_solver.find_business_friends_checkin.FindBusinessFriendsCheckin.__init__', __init__):
        find_business_friends_checkin_obj = FindBusinessFriendsCheckin()

    users_friends_checkins_df = find_business_friends_checkin_obj.find_users_friends_checkins()

    assert_df_equal(expected_users_friends_checkins_df, users_friends_checkins_df)


def test_find_business_info(spark):
    input_business_data = [
        ('1', 'business_1'),
        ('2', 'business_2'),
        ('3', 'business_3'),
        ('4', 'business_4'),
        ('5', 'business_5'),
    ]

    input_business_df = spark.createDataFrame(data=input_business_data, schema=['business_id', 'name'])

    input_users_friends_checkins_data = [
        ('1', 'user_name1', 'friend2', '1'),
        ('1', 'user_name1', 'friend4', '1'),
        ('2', 'user_name2', 'friend3', '3'),
        ('2', 'user_name2', 'friend11', '3'),
        ('4', 'user_name4', 'friend14', '2')
    ]

    input_users_friends_checkins_df = spark.createDataFrame(data=input_users_friends_checkins_data,
                                                            schema=['user_id', 'user_name', 'friend_id',
                                                                    'business_id'])

    expected_users_business_data = [
        ('1', '1', 'user_name1', 'friend2', 'business_1'),
        ('1', '1', 'user_name1', 'friend4', 'business_1'),
        ('2', '4', 'user_name4', 'friend14', 'business_2'),
        ('3', '2', 'user_name2', 'friend3', 'business_3'),
        ('3', '2', 'user_name2', 'friend11', 'business_3')
    ]

    expected_users_business_df = spark.createDataFrame(data=expected_users_business_data,
                                                       schema=['business_id', 'user_id', 'user_name',
                                                               'friend_id', 'business_name'])

    def __init__(self):
        self.spark = spark
        self.business_df = input_business_df

    with patch('task_solver.find_business_friends_checkin.FindBusinessFriendsCheckin.__init__', __init__):
        find_business_friends_checkin_obj = FindBusinessFriendsCheckin()

    users_friends_business_df = find_business_friends_checkin_obj.find_business_info(input_users_friends_checkins_df)

    assert_df_equal(expected_users_business_df, users_friends_business_df)


def test_transform_data(spark):
    input_users_data = [
        ('1', 'user_name1', 'friend1, friend2, friend3, friend4'),
        ('2', 'user_name2', 'friend5, friend14, friend3, friend11'),
        ('3', 'user_name3', 'friend5, friend2, friend3, friend1'),
        ('4', 'user_name4', 'friend2, friend14'),
        ('friend1', 'friend_name4', 'friend13, 1'),
        ('friend2', 'friend_name2', 'friend13, 1, 2, 4'),
        ('friend3', 'friend_name3', 'friend24, 11, 3'),
        ('friend14', 'friend_name14', 'friend12, 2'),
        ('friend11', 'friend_name11', 'friend1, 4, 3'),
        ('friend5', 'friend_name5', 'friend2, friend1')
    ]

    input_users_df = spark.createDataFrame(data=input_users_data, schema=['user_id', 'name', 'friends'])

    input_revisions_data = [
        ('1', '1'),
        ('2', '3'),
        ('friend2', '1'),
        ('friend3', '3'),
        ('friend5', '2'),
        ('friend4', '1'),
        ('friend14', '2'),
        ('4', '2'),
        ('friend10', '5'),
        ('friend11', '3'),
    ]

    input_revisions_df = spark.createDataFrame(data=input_revisions_data, schema=['user_id', 'business_id'])

    input_business_data = [
        ('1', 'business_1'),
        ('2', 'business_2'),
        ('3', 'business_3'),
        ('4', 'business_4'),
        ('5', 'business_5'),
    ]

    input_business_df = spark.createDataFrame(data=input_business_data, schema=['business_id', 'name'])

    expected_data = [
        ('1', 'business_1', '1', 'user_name1', [('friend2', 'friend_name2')]),
        ('3', 'business_3', '2', 'user_name2', [('friend3', 'friend_name3'), ('friend11', 'friend_name11')]),
        ('2', 'business_2', '4', 'user_name4', [('friend14', 'friend_name14')]),
        ('1', 'business_1', 'friend2', 'friend_name2', [('1', 'user_name1')]),
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_BUSINESS_FRIENDS_CHECKIN_8_4)

    def __init__(self):
        self.spark = spark
        self.users_df = input_users_df
        self.revisions_df = input_revisions_df
        self.business_df = input_business_df

    with patch('task_solver.find_business_friends_checkin.FindBusinessFriendsCheckin.__init__', __init__):
        find_business_friends_checkin_obj = FindBusinessFriendsCheckin()

    actual_df = find_business_friends_checkin_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
