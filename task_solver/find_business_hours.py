from pyspark.sql import functions as f
from pyspark.sql import types as t

from task_solver.const import DAYS, RESULT_WORKING_HOURS
from task_solver.load_business_info import LoadInfoBusiness
from utils import write_to_csv


class FindBusinessHours(LoadInfoBusiness):
    def transform_data(self, *args):
        """
            This method find businesses, which works more than 12 hours at least one day and find average works hours
            in a week, mark if the business is open 24 hours a day.
            :param self: object of the class FindInfoBusiness, which have business_df with info about
            business work hours
            :return: Dataframe with the average number of hours the business is open per day and
            whether the business is open 24 hours a day
        """

        def split_condition(column_name, delimiter, position):
            """
                Accept the column and separate for needed symbol in the string and accept the needed
                part after splitting
                Input: column_name = '8:0-18:0', separate_value='-', item=0
                Output: '8:0'
                :param:
                    column_name (str): The name of the column containing the work hours.
                    delimiter (str): The separator used to split the string in the column.
                    position (int): The item index to be extracted from the separated values.
                :return: Column with works times
            """
            return f.split(column_name, delimiter).getItem(position)

        def raw_hours_condition(col):
            """
                Accept the column, and find how many times, business is working per day
                :param col: Column with work hours like '8:0-18:0'
                :return: Column: The number of hours that the business is working per day.
            """
            open_works_time = split_condition(col, '-', 0)
            open_hours = split_condition(open_works_time, ':', 0)
            open_minutes = split_condition(open_works_time, ':', 1)

            close_works_time = split_condition(col, '-', 1)
            close_hours = split_condition(close_works_time, ':', 0)
            close_minutes = split_condition(close_works_time, ':', 1)

            return (close_hours + (close_minutes / 60)) - (open_hours + (open_minutes / 60))

        work_hours_expr = [f.when(raw_hours_condition(f'hours.{day}') < 0, (raw_hours_condition(f'hours.{day}') + 24))
                           .when((raw_hours_condition(f'hours.{day}') == 0.0), 24.0)
                           .otherwise(raw_hours_condition(f'hours.{day}'))
                           .alias(day)
                           for day in DAYS]

        more_12_hours = ' or '.join([f'{day} >= 12.0' for day in DAYS])
        more_24_hours = ' or '.join([f'{day} == 24.0' for day in DAYS])
        total_hours = sum(f.coalesce(f.col(day), f.lit(0)) for day in DAYS)

        result_df = (self.business_df
                     .select('business_id', 'name', *work_hours_expr)
                     .where(f.expr(more_12_hours))
                     .withColumn('is_open_for_24h', f.when(f.expr(more_24_hours), True).otherwise(False))
                     .withColumn('avg_hours_open', (total_hours / 7).cast(t.FloatType()))
                     .select('business_id', 'name', 'avg_hours_open', 'is_open_for_24h'))

        return result_df

    def run(self):
        """
            Runs the data transformation pipeline and writes the result to a CSV file.
            param: An instance of a class that has a `transform_data` method.
        """

        result_df = self.transform_data()

        write_to_csv(df=result_df, file_path=RESULT_WORKING_HOURS)
