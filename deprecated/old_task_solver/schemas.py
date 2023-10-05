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

SCHEMA_RATIO = t.StructType([
                                t.StructField('index', t.IntegerType(), False),
                                t.StructField('ratio_key', t.StringType(), False),
                                t.StructField('start_date', t.StringType(), False),
                                t.StructField('end_date', t.StringType(), False)] +
                            [t.StructField(f'ratio_{s}', t.DoubleType(),
                                           False) for s in range(1, 38)] +
                            [t.StructField(f'dratio_{col}', t.DoubleType(),
                                           False) for col in range(1, 38)] +
                            [t.StructField('valid_flag', t.IntegerType(), False)]
                            )

SCHEMA_TASK_DF4 = t.StructType([
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('fac_prof', t.StringType(), False),
    t.StructField('proc_code', t.StringType(), False),
])

SCHEMA_TASK_5_1 = t.StructType([
                                   t.StructField('id', t.IntegerType(), False)] +
                               [t.StructField(f'm{s}', t.IntegerType(),
                                              False) for s in range(1, 13)]
                               )

SCHEMA_TASK_5_2 = t.StructType([
    t.StructField('id', t.IntegerType(), False),
    t.StructField('month', t.StringType(), False),
])

SCHEMA_TASK_6 = t.StructType([
    t.StructField('effective_from_date', t.StringType(), False),
    t.StructField('patient_id', t.StringType(), False)
])

SCHEMA_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('effective_from_date', t.StringType(), False)
])

SCHEMA_TASK_8 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), False), False)
])

SCHEMA_TASK_8_2_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('WiFi', t.StringType(), False)]), False)
])

SCHEMA_TASK_8_2_2 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('BusinessParking', t.StringType(), False),
        t.StructField('BikeParking', t.StringType(), False)
    ]), False)
])

SCHEMA_TASK_8_2_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('RestaurantsPriceRange2', t.StringType(), False)]), False)
])

SCHEMA_TASK_8_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('date', t.StringType(), False)
])

SCHEMA_USERS = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('friends', t.StringType(), False),
])

SCHEMA_REVIEW = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False),
])

SCHEMA_BUSINESS = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False)
])
