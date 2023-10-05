from abc import ABC, abstractmethod
from pyspark.sql import functions as f
from pyspark.sql import types as t

from task_solver.const import BUSINESS_DATASET, USERS_DATASET, REVIEWS_DATASET, TIPS_DATASET, CHECKIN_DATASET
from task_solver.schemas import SCHEMA_BUSINESS_INFO_DATASET, SCHEMA_USERS_DATASET, SCHEMA_REVIEWS_TIPS_DATASET, \
    SCHEMA_CHECKIN_DATASET
from utils import read_json, spark_builder


class LoadInfoBusiness(ABC):
    def __init__(self,
                 business_path=BUSINESS_DATASET,
                 users_path=USERS_DATASET,
                 reviews_path=REVIEWS_DATASET,
                 tips_path=TIPS_DATASET,
                 checking_path=CHECKIN_DATASET):
        self.spark = spark_builder(appname='Task 8', partitions=6)
        self.business_df = read_json(spark=self.spark,
                                     path=business_path,
                                     schema_json=SCHEMA_BUSINESS_INFO_DATASET,
                                     header='true',
                                     multiline=False)
        self.users_df = read_json(spark=self.spark,
                                  path=users_path,
                                  schema_json=SCHEMA_USERS_DATASET,
                                  header='true',
                                  multiline=False)
        self.reviews_df = read_json(spark=self.spark,
                                    path=reviews_path,
                                    schema_json=SCHEMA_REVIEWS_TIPS_DATASET,
                                    header='true',
                                    multiline=False)
        self.tips_df = read_json(spark=self.spark,
                                 path=tips_path,
                                 schema_json=SCHEMA_REVIEWS_TIPS_DATASET,
                                 header='true',
                                 multiline=False)
        self.checkin_df = read_json(spark=self.spark,
                                    path=checking_path,
                                    schema_json=SCHEMA_CHECKIN_DATASET,
                                    header='true',
                                    multiline=False).cache()
        self.revisions_df = None

        self.prepare_business_data()
        self.prepare_users_data()
        self.prepare_revisions_data()

    def prepare_business_data(self):
        """
            Selects and cleans the input raw data in `self.business_df` DataFrame, and prepares
            it for further transformations
         """
        self.business_df = (self.business_df
                            .select('business_id', 'name', 'categories', 'hours',
                                    f.col('attributes.BusinessParking').alias('business_parking'),
                                    f.col('attributes.WiFi').alias('wifi'),
                                    f.col('attributes.BikeParking').alias('bike_parking'),
                                    f.col('attributes.RestaurantsPriceRange2').cast(t.IntegerType()).alias(
                                        'price_range'))
                            .where(f.col('business_id').isNotNull())
                            .cache())

    def prepare_users_data(self):
        """
            Selects and cleans the input raw data in `self.users_df` DataFrame, and prepares
            it for further transformations.
         """
        self.users_df = (self.users_df
                         .select('user_id', 'name', 'friends')
                         .where(f.col('user_id').isNotNull() & f.col('name').isNotNull())
                         .cache())

    def prepare_revisions_data(self):
        """
            Selects and union the input raw data in the `self.reviews_df` and `self.tips_df` DataFrames,
            and prepares them for further transformations. Create the `self.revisions_df`.
        """

        valid_revisions = f.col('user_id').isNotNull() & f.col('business_id').isNotNull()

        self.revisions_df = (self.reviews_df
                             .where(valid_revisions)
                             .union(self.tips_df.where(valid_revisions)))

    @abstractmethod
    def transform_data(self, *args):
        """ Do specific transformations here for the task"""
