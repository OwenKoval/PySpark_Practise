from pyspark.sql import functions as f

from task_solver.const import RESULT_BUSINESS_PARKING
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessParking(LoadInfoBusiness):

    def transform_data(self, *args):
        """
            This method mark businesses, which have a free Wi-Fi.
            :param self: object of the class FindInfoBusiness, which have business_df with info about business Wi-Fi.
            :return: Dataframe with the info about free Wi-Fi.
        """

        has_car_parking = (f.when((f.col('business_parking').contains("'garage': True"))
                                  | (f.col('business_parking').contains("'street': True"))
                                  | (f.col('business_parking').contains("'lot': True")), True)
                           .otherwise(False)
                           .alias('has_car_parking'))

        has_bike_parking = (f.when(f.col('bike_parking') == 'True', True)
                            .otherwise(False)
                            .alias('has_bike_parking'))

        result_df = self.business_df.select('business_id', 'name', has_car_parking, has_bike_parking)

        return result_df

    def run(self):
        """
            Runs the data transformation pipeline and writes the result to a CSV file.
            param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_BUSINESS_PARKING)
