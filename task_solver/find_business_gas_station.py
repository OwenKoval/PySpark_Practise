from pyspark.sql import functions as f

from task_solver.const import RESULT_BUSINESS_GAS_STATION
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessGasStation(LoadInfoBusiness):

    def transform_data(self, *args):
        """
        Find gas stations, which has price_range <=3 and has cafe or store.
        :param self: object of the class FindInfoBusiness, which have business_df with info
                     about business categories.
        :return: Dataframe with the info about Gas Station and info about it.
        """

        has_cafe = (f.when(f.col('categories').contains('Cafe'), True)
                    .otherwise(False)
                    .alias('has_cafe'))

        has_store = (f.when(f.col('categories').contains('Store')
                            | (f.col('categories').contains('Grocery'))
                            | (f.col('categories').contains('Market')), True)
                     .otherwise(False)
                     .alias('has_store'))

        is_gas_station = f.when(f.col('categories').contains('Gas Station'), True)

        result_df = (self.business_df
                     .where((f.col('price_range').between(1, 3)) & is_gas_station & (has_cafe | has_store))
                     .select('business_id', 'name', 'price_range', has_cafe, has_store,
                             f.split(f.col('categories'), ', ').alias('categories')))

        return result_df

    def run(self):
        """
        Runs the data transformation pipeline and writes the result to a CSV file.
        param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_BUSINESS_GAS_STATION)
