"""
File with all needed schemas for testing methods
"""

from pyspark.sql import types as t

# SCHEMAS FOR TASK2
SCHEMA_REVBCKT_DATA = t.StructType([
    t.StructField('number', t.IntegerType(), True),
    t.StructField('rev_code', t.StringType(), True),
    t.StructField('begin_date', t.StringType(), True),
    t.StructField('end_date', t.StringType(), True)
])

SCHEMA_HE_DATA = t.StructType([
    t.StructField('record_identifier', t.StringType(), False),
    t.StructField('discharge_date', t.StringType(), False),
    t.StructField('chg1', t.StringType(), False),
    t.StructField('chg2', t.StringType(), False),
    t.StructField('chg3', t.StringType(), False),
    t.StructField('chg4', t.StringType(), False),
    t.StructField('rev_code1', t.StringType(), False),
    t.StructField('rev_code2', t.StringType(), False),
    t.StructField('rev_code3', t.StringType(), False),
    t.StructField('rev_code4', t.StringType(), False)
])

SCHEMA_PREPARING_REVBCKT_DF = t.StructType([
    t.StructField('number', t.IntegerType(), True),
    t.StructField('rev_code', t.IntegerType(), True),
    t.StructField('begin_date', t.DateType(), True),
    t.StructField('end_date', t.DateType(), True)
])

SCHEMA_PREPARING_HE_DF = t.StructType([
    t.StructField('record_identifier', t.StringType(), False),
    t.StructField('discharge_date', t.DateType(), False),
    t.StructField('rev_code', t.IntegerType(), False),
    t.StructField('chg', t.DecimalType(scale=2), False)
])

SCHEMA_SUM_SELECT_DF = t.StructType([
    t.StructField('rev_code', t.IntegerType(), False),
    t.StructField('sum(chg)', t.DecimalType(scale=2), False)
])

# SCHEMAS FOR TASK8

SCHEMA_DATA_BUSINESSES_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.StringType(), True),
    t.StructField('business_parking', t.StringType(), True),
    t.StructField('bike_parking', t.StringType(), True),
    t.StructField('wifi', t.StringType(), True),
    t.StructField('price_range', t.IntegerType(), True),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), False), True)
])

SCHEMA_WORKS_HOURS_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), False), True)
])

EXPECTED_WORKS_HOURS_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('avg_hours_open', t.FloatType(), False),
    t.StructField('is_open_for_24h', t.BooleanType(), False),
])

SCHEMA_TEST_BUSINESS_DATASET = t.StructType([
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

SCHEMA_EXPECTED_TEST_BUSINESS_DATASET = t.StructType([
    t.StructField('business_id', t.StringType(), True),
    t.StructField('name', t.StringType(), True),
    t.StructField('categories', t.StringType(), True),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), True), True),
    t.StructField('business_parking', t.StringType(), True),
    t.StructField('bike_parking', t.StringType(), True),
    t.StructField('wifi', t.StringType(), True),
    t.StructField('price_range', t.IntegerType(), True)
])

SCHEMA_TEST_USERS_DATASET = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('friends', t.StringType(), False),
])

SCHEMA_TEST_REVIEWS_TIPS_DATASET = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False),
])

SCHEMA_TEST_CHECKIN_DATASET = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('date', t.StringType(), False)
])

EXPECTED_WIFI_8_2_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('free_wifi', t.BooleanType(), False)
])

EXPECTED_PARKING_8_2_2 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('has_car_parking', t.BooleanType(), False),
    t.StructField('has_bike_parking', t.BooleanType(), False)
])

SCHEMA_TEST_GAS_STATION_8_2_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.StringType(), True),
    t.StructField('price_range', t.IntegerType(), False)
])

EXPECTED_GAS_STATION_8_2_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.ArrayType(t.StringType()), False),
    t.StructField('price_range', t.IntegerType(), False),
    t.StructField('has_cafe', t.BooleanType(), False),
    t.StructField('has_store', t.BooleanType(), False)
])

EXPECTED_BUSINESS_CHECKIN_8_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('year', t.IntegerType(), False),
    t.StructField('checkin_count', t.IntegerType(), False),
])

EXPECTED_BUSINESS_FRIENDS_CHECKIN_8_4 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('business_name', t.StringType(), False),
    t.StructField('user_id', t.StringType(), False),
    t.StructField('user_name', t.StringType(), False),
    t.StructField('friends_attendees', t.ArrayType(
        t.StructType([t.StructField("user_id", t.StringType(), False),
                      t.StructField("user_name", t.StringType(), False)]), False))
])
