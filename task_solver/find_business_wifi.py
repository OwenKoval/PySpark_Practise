from pyspark.sql import functions as f

from task_solver.const import RESULT_FREE_WIFI
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessWiFi(LoadInfoBusiness):

    def transform_data(self, *args):
        """
            This method mark businesses, which have a free Wi-Fi.
            :param self: object of the class FindInfoBusiness, which have business_df with info about business Wi-Fi.
            :return: Dataframe with the info about free Wi-Fi.
        """

        free_wifi = f.when(f.col('wifi').contains('free'), True).otherwise(False).alias('free_wifi')

        result_df = self.business_df.select('business_id', 'name', free_wifi)

        return result_df

    def run(self):
        """
            Runs the data transformation pipeline and writes the result to a CSV file.
            param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_FREE_WIFI)
