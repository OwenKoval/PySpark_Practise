from pyspark.sql import functions as f
from pyspark.sql.window import Window

from task_solver.const import RESULT_BUSINESS_FRIENDS_CHECKIN
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessFriendsCheckin(LoadInfoBusiness):

    def find_users_friends_checkins(self):
        """
            Find the user's friends which were in the same business as a user.
            param: an instance of a class that has users_df and revisions_df.
        """

        friend_id = f.explode(f.split(f.col('friends'), ', ')).alias('friend_id')

        users_checkins_df = (self.users_df.join(self.revisions_df, on='user_id', how='inner'))

        users_friends_checkins_df = (users_checkins_df
                                     .select('user_id', 'business_id', f.col('name').alias('user_name'), friend_id)
                                     .alias('df_a')
                                     .join(self.revisions_df.alias('df_b'),
                                           on=[(f.col('df_a.friend_id') == f.col('df_b.user_id')) &
                                               (f.col('df_a.business_id') == f.col('df_b.business_id'))],
                                           how='inner')
                                     .select('df_a.user_id', 'df_a.user_name', 'df_a.friend_id', 'df_a.business_id'))

        return users_friends_checkins_df

    def find_business_info(self, users_friends_checkins_df):
        """
            Find info about businesses, where was user and his friends
            :param users_friends_checkins_df: dataframe with users and friends checkins
            :return: result dataframe with users and businesses info
        """

        users_friends_business_df = (users_friends_checkins_df
                                     .join(self.business_df.select('business_id', f.col('name').alias('business_name')),
                                           on='business_id', how='inner'))

        return users_friends_business_df

    def transform_data(self, *args):
        """
            Find the friends of users, which were in the same business as user and leave tip or review.
            :param self: object of the class FindBusinessFriendsCheckin.
            :return: Dataframe with the info about checkins users, business and friends.
        """

        windowed_user = Window().partitionBy('user_id', 'user_name').orderBy('user_id')

        users_friends_checkins_df = self.find_users_friends_checkins()

        users_friends_business_df = self.find_business_info(users_friends_checkins_df)

        result_df = (users_friends_business_df
                     .join(self.users_df.select(f.col('user_id').alias('friend_id'),
                                                f.col('name').alias('friend_name')), on='friend_id', how='inner')
                     .select('business_id', 'business_name', 'user_id', 'user_name',
                             f.collect_list(f.struct(f.col('friend_id').alias('user_id'),
                                                     f.col('friend_name').alias('user_name'))).over(windowed_user)
                             .alias('friends_attendees'))
                     .distinct())

        return result_df

    def run(self):
        """
            Runs the data transformation pipeline and writes the result to a CSV file.
            param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_BUSINESS_FRIENDS_CHECKIN)
