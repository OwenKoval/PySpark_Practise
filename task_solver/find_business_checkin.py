from pyspark.sql import functions as f
from pyspark.sql.window import Window

from task_solver.const import RESULT_BUSINESS_CHECKIN
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessCheckin(LoadInfoBusiness):

    def transform_data(self, *args):
        """
        Find the number of checks for each year by business.
        :param self: object of the class FindInfoBusiness, which have checkin_df with info about business dates.
        :return: Dataframe with the info about business and date checkins.
        """

        windowed_business = Window().partitionBy('business_id', 'year').orderBy('year')

        year = (f.explode(f.expr("transform(split(date, ','), x -> year(to_date(x, 'yyyy-MM-dd HH:mm:ss')))"))
                .alias('year'))

        result_df = (self.checkin_df
                     .select('business_id', year)
                     .withColumn('year_number', f.row_number().over(windowed_business))
                     .withColumn('checkin_count', f.max('year_number').over(windowed_business))
                     .where(f.col('year_number') == f.col('checkin_count'))
                     .select('business_id', 'year', 'checkin_count'))

        return result_df

    def run(self):
        """
        Runs the data transformation pipeline and writes the result to a CSV file.
        param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_BUSINESS_CHECKIN)
