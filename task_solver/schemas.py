"""
The file which set the schema for dataframe
"""

from pyspark.sql import types as t

SCHEMA_REVTOBUCKET = t.StructType([
    t.StructField('number', t.IntegerType(), False),
    t.StructField('rev_code', t.IntegerType(), False),
    t.StructField('begin_date', t.StringType(), False),
    t.StructField('end_date', t.StringType(), False),
])

SCHEMA_RATIO = t.StructType([t.StructField('index', t.IntegerType(), False),
                             t.StructField('ratio_key', t.StringType(), False),
                             t.StructField('start_date', t.StringType(), False),
                             t.StructField('end_date', t.StringType(), False)] +
                            [t.StructField(f'ratio_{s}', t.DoubleType(),
                                           False) for s in range(1, 38)] +
                            [t.StructField(f'dratio_{col}', t.DoubleType(),
                                           False) for col in range(1, 38)] +
                            [t.StructField('valid_flag', t.IntegerType(), False)])

SCHEMA_BUSINESS_INFO_DATASET = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), False), False),
    t.StructField('categories', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('BusinessParking', t.StringType(), False),
        t.StructField('BikeParking', t.StringType(), False),
        t.StructField('WiFi', t.StringType(), False),
        t.StructField('RestaurantsPriceRange2', t.StringType(), False)
    ]), False)
])

SCHEMA_USERS_DATASET = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('friends', t.StringType(), False),
])

SCHEMA_REVIEWS_TIPS_DATASET = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False),
])

SCHEMA_CHECKIN_DATASET = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('date', t.StringType(), False)
])
