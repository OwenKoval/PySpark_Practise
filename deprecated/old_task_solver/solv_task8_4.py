"""
Task 8 for Practise PySpark NIX
KOVAL OLEG
"""

from pyspark.sql import functions as f
from pyspark.sql.window import Window

from deprecated.old_task_solver.schemas import SCHEMA_USERS, SCHEMA_REVIEW, SCHEMA_BUSINESS
from utils import read_json, write_result_df_json


def find_users_friends_checkins(users_df, review_df, tips_df):
    """
        :param users_df: dataframe with users data
        :param review_df: dataframe with users review on business
        :param tips_df: dataframe with users tips on business
        :users_friends_checkins_df: result dataframe with users and friends checkins
    """
    checkin_df = review_df.union(tips_df)

    users_array_friends_df = (users_df
                              .withColumn('arr_friends', f.split(f.col('friends'), ', '))
                              .drop('friends'))

    users_friends_df = (users_array_friends_df
                        .withColumn('friend', f.explode(users_array_friends_df.arr_friends))
                        .drop('arr_friends'))

    users_friends_checkins_df = users_friends_df.join(checkin_df, on='user_id', how='left')

    users_friends_checkins_df = (users_friends_checkins_df.alias('df_a')
                                 .join(checkin_df.alias('df_b'),
                                       on=[(f.col('df_a.friend') == f.col('df_b.user_id')) &
                                           (f.col('df_a.business_id') == f.col('df_b.business_id'))], how='left')
                                 .drop(f.col('df_b.user_id'))
                                 .drop(f.col('df_b.business_id')))

    return users_friends_checkins_df


def find_business_info(users_friends_checkins_df, business_df):
    """
        :param users_friends_checkins_df: dataframe with users and friends checkins
        :param business_df: dataframe with businesses data
        :return: result dataframe with users and businesses info
    """
    business_df = business_df.withColumnRenamed('name', 'business_name')

    users_business_df = (users_friends_checkins_df
                         .join(business_df, on=(users_friends_checkins_df.business_id == business_df.business_id),
                               how='left')
                         .drop(business_df.business_id)
                         .distinct())

    return users_business_df


def find_friends_info(users_business_df, users_df):
    """
        :param users_business_df: dataframe with users and friends checkins and businesses info
        :param users_df: dataframe with users data
        :return: result dataframe with users and friends
    """
    windowed_user = Window().partitionBy('user_id', 'name').orderBy('user_id')

    friends_df = (users_df
                  .withColumnRenamed('user_id', 'friend_id')
                  .withColumnRenamed('name', 'user_name')
                  .drop('friends'))

    result_df = (users_business_df
                 .join(friends_df, on=(users_business_df.friend == friends_df.friend_id),
                       how='left')
                 .withColumn('friends_attendees',
                             f.collect_list(
                                 f.struct(f.col('friend_id').alias('user_id'), f.col('user_name'))).over(
                                 windowed_user))
                 .drop('friend', 'friend_id', 'user_name')
                 .withColumnRenamed('name', 'user_name')
                 .distinct())

    return result_df


def solv_task_8_4(spark, file_path_users, file_path_review, file_path_tips, file_path_businesses, save_file_path):
    """
        :param spark: SparkSession
        :param file_path_users: path to file with users data
        :param file_path_review: path to file with review data
        :param file_path_tips: path to file with tips data
        :param file_path_businesses: path to file with businesses data
        :param save_file_path: path where file will save with data
        :return: result file and dataframe
    """
    users_df = read_json(spark=spark,
                         path=file_path_users,
                         schema_json=SCHEMA_USERS,
                         header='true')

    review_df = read_json(spark=spark,
                          path=file_path_review,
                          schema_json=SCHEMA_REVIEW,
                          header='true')

    tips_df = read_json(spark=spark,
                        path=file_path_tips,
                        schema_json=SCHEMA_REVIEW,
                        header='true')

    business_df = read_json(spark=spark,
                            path=file_path_businesses,
                            schema_json=SCHEMA_BUSINESS,
                            header='true')

    users_friends_checkins_df = find_users_friends_checkins(users_df=users_df,
                                                            review_df=review_df,
                                                            tips_df=tips_df)

    users_businesses_df = find_business_info(users_friends_checkins_df=users_friends_checkins_df,
                                             business_df=business_df)

    result_df = find_friends_info(users_business_df=users_businesses_df,
                                  users_df=users_df)

    write_result_df_json(df=result_df, file_path=save_file_path)
