"""
File with all needed schemas for testing methods
"""

from pyspark.sql import types as t

# SCHEMAS FOR TASK2
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

# SCHEMAS FOR TASK3
SCHEMA_RATIO_DF = t.StructType([
    t.StructField('index', t.IntegerType(), False),
    t.StructField('ratio_key', t.StringType(), False),
    t.StructField('start_date', t.StringType(), False),
    t.StructField('end_date', t.StringType(), False),
    t.StructField('ratio_1', t.DoubleType(), False),
    t.StructField('ratio_2', t.DoubleType(), False),
    t.StructField('ratio_3', t.DoubleType(), False),
    t.StructField('ratio_4', t.DoubleType(), False),
    t.StructField('ratio_5', t.DoubleType(), False),
    t.StructField('ratio_6', t.DoubleType(), False),
    t.StructField('ratio_7', t.DoubleType(), False),
    t.StructField('ratio_8', t.DoubleType(), False),
    t.StructField('ratio_9', t.DoubleType(), False),
    t.StructField('ratio_10', t.DoubleType(), False),
    t.StructField('ratio_11', t.DoubleType(), False),
    t.StructField('ratio_12', t.DoubleType(), False),
    t.StructField('ratio_13', t.DoubleType(), False),
    t.StructField('ratio_14', t.DoubleType(), False),
    t.StructField('ratio_15', t.DoubleType(), False),
    t.StructField('ratio_16', t.DoubleType(), False),
    t.StructField('ratio_17', t.DoubleType(), False),
    t.StructField('ratio_18', t.DoubleType(), False),
    t.StructField('ratio_19', t.DoubleType(), False),
    t.StructField('ratio_20', t.DoubleType(), False),
    t.StructField('ratio_21', t.DoubleType(), False),
    t.StructField('ratio_22', t.DoubleType(), False),
    t.StructField('ratio_23', t.DoubleType(), False),
    t.StructField('ratio_24', t.DoubleType(), False),
    t.StructField('ratio_25', t.DoubleType(), False),
    t.StructField('ratio_26', t.DoubleType(), False),
    t.StructField('ratio_27', t.DoubleType(), False),
    t.StructField('ratio_28', t.DoubleType(), False),
    t.StructField('ratio_29', t.DoubleType(), False),
    t.StructField('ratio_30', t.DoubleType(), False),
    t.StructField('ratio_31', t.DoubleType(), False),
    t.StructField('ratio_32', t.DoubleType(), False),
    t.StructField('ratio_33', t.DoubleType(), False),
    t.StructField('ratio_34', t.DoubleType(), False),
    t.StructField('ratio_35', t.DoubleType(), False),
    t.StructField('ratio_36', t.DoubleType(), False),
    t.StructField('ratio_37', t.DoubleType(), False),
    t.StructField('dratio_1', t.DoubleType(), False),
    t.StructField('dratio_2', t.DoubleType(), False),
    t.StructField('dratio_3', t.DoubleType(), False),
    t.StructField('dratio_4', t.DoubleType(), False),
    t.StructField('dratio_5', t.DoubleType(), False),
    t.StructField('dratio_6', t.DoubleType(), False),
    t.StructField('dratio_7', t.DoubleType(), False),
    t.StructField('dratio_8', t.DoubleType(), False),
    t.StructField('dratio_9', t.DoubleType(), False),
    t.StructField('dratio_10', t.DoubleType(), False),
    t.StructField('dratio_11', t.DoubleType(), False),
    t.StructField('dratio_12', t.DoubleType(), False),
    t.StructField('dratio_13', t.DoubleType(), False),
    t.StructField('dratio_14', t.DoubleType(), False),
    t.StructField('dratio_15', t.DoubleType(), False),
    t.StructField('dratio_16', t.DoubleType(), False),
    t.StructField('dratio_17', t.DoubleType(), False),
    t.StructField('dratio_18', t.DoubleType(), False),
    t.StructField('dratio_19', t.DoubleType(), False),
    t.StructField('dratio_20', t.DoubleType(), False),
    t.StructField('dratio_21', t.DoubleType(), False),
    t.StructField('dratio_22', t.DoubleType(), False),
    t.StructField('dratio_23', t.DoubleType(), False),
    t.StructField('dratio_24', t.DoubleType(), False),
    t.StructField('dratio_25', t.DoubleType(), False),
    t.StructField('dratio_26', t.DoubleType(), False),
    t.StructField('dratio_27', t.DoubleType(), False),
    t.StructField('dratio_28', t.DoubleType(), False),
    t.StructField('dratio_29', t.DoubleType(), False),
    t.StructField('dratio_30', t.DoubleType(), False),
    t.StructField('dratio_31', t.DoubleType(), False),
    t.StructField('dratio_32', t.DoubleType(), False),
    t.StructField('dratio_33', t.DoubleType(), False),
    t.StructField('dratio_34', t.DoubleType(), False),
    t.StructField('dratio_35', t.DoubleType(), False),
    t.StructField('dratio_36', t.DoubleType(), False),
    t.StructField('dratio_37', t.DoubleType(), False),
    t.StructField('valid_flag', t.IntegerType(), False)
])

SCHEMA_PREPEDER_RAT_DF = t.StructType([
    t.StructField('index', t.IntegerType(), False),
    t.StructField('ratio_key', t.StringType(), False),
    t.StructField('facility_id', t.StringType(), False),
    t.StructField('medicare_id', t.StringType(), False),
    t.StructField('start_date', t.DateType(), False),
    t.StructField('end_date', t.DateType(), False),
    t.StructField('valid_flag', t.IntegerType(), False),
    t.StructField('ratios', t.ArrayType(
        t.StructType([
            t.StructField('zipped_rev', t.DoubleType(), False),
            t.StructField('zipped_chg', t.DoubleType(), False)
        ])
    ), False),
])

SCHEMA_PREPEDER_HE_DF = t.StructType([
    t.StructField('record_identifier', t.StringType(), False),
    t.StructField('facility_id', t.StringType(), False),
    t.StructField('fac_medicare_id', t.StringType(), False),
    t.StructField('discharge_date', t.DateType(), False),
    t.StructField('patient_id', t.IntegerType(), False)
])

SCHEMA_RESULT_TASK3 = t.StructType([
    t.StructField('facility_id', t.StringType(), False),
    t.StructField('record_identifier', t.StringType(), False),
    t.StructField('fac_medicare_id', t.StringType(), False),
    t.StructField('discharge_date', t.DateType(), False),
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('index', t.IntegerType(), False),
    t.StructField('ratio_key', t.StringType(), False),
    t.StructField('start_date', t.DateType(), False),
    t.StructField('end_date', t.DateType(), False),
    t.StructField('valid_flag', t.IntegerType(), False),
    t.StructField('ratios', t.ArrayType(
        t.StructType([
            t.StructField('zipped_rev', t.DoubleType(), False),
            t.StructField('zipped_chg', t.DoubleType(), False)
        ])
    ), False),
])

# SCHEMAS FOR TASK4

SCHEMA_TASK4_DF = t.StructType([
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('fac_prof', t.StringType(), False),
    t.StructField('proc_code', t.StringType(), False),
])

SCHEMA_PREPARED_PROC_DF = t.StructType([
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('proc_code', t.StringType(), False),
    t.StructField('count', t.LongType(), False),
    t.StructField('order', t.IntegerType(), False)
])

SCHEMA_PREPARED_FAC_DF = t.StructType([
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('fac_prof', t.StringType(), False),
    t.StructField('count', t.LongType(), False),
    t.StructField('order', t.IntegerType(), False)
])

SCHEMA_RESULT_PROC_FAC_DF = t.StructType([
    t.StructField('patient_id', t.IntegerType(), False),
    t.StructField('proc_code', t.StringType(), False),
    t.StructField('fac_prof', t.StringType(), False)
])

# SCHEMAS FOR TASK5

SCHEMA_DATA_TASK_5_1 = t.StructType([t.StructField('id', t.IntegerType(), False)] +
                                    [t.StructField(f'm{s}', t.IntegerType(),
                                                   False) for s in range(1, 13)]
                                    )

RESULT_DF_TASK_5 = t.StructType([
    t.StructField('id', t.IntegerType(), False),
    t.StructField('q1', t.IntegerType(), False),
    t.StructField('q2', t.IntegerType(), False),
    t.StructField('q3', t.IntegerType(), False),
    t.StructField('q4', t.IntegerType(), False),
])

SCHEMA_DATA_TASK_5_2 = t.StructType([
    t.StructField('id', t.IntegerType(), False),
    t.StructField('month', t.StringType(), False)
]
)

SUMMED_SCHEMA_DATA_TASK_5_2 = t.StructType([
    t.StructField('id', t.IntegerType(), False),
    t.StructField('Sum-q1', t.IntegerType(), False),
    t.StructField('Sum-q2', t.IntegerType(), False),
    t.StructField('Sum-q3', t.IntegerType(), False),
    t.StructField('Sum-q4', t.IntegerType(), False)
])

# SCHEMAS FOR TASK6

SCHEMA_DATA_TASK_6 = t.StructType([
    t.StructField('effective_from_date', t.StringType(), False),
    t.StructField('patient_id', t.StringType(), False)
])

RESULT_DATA_TASK_6 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('5month', t.BooleanType(), False),
    t.StructField('7month', t.BooleanType(), False),
    t.StructField('9month', t.BooleanType(), False)
])

# SCHEMAS FOR TASK7

SCHEMA_DATA_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('effective_from_date', t.StringType(), False)
])

PREPARED_DF_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('effective_from_date', t.DateType(), False),
    t.StructField('end_date', t.StringType(), False),
    t.StructField('start_date', t.DateType(), False),
    t.StructField('month', t.IntegerType(), False),
    t.StructField('numb', t.IntegerType(), False),
    t.StructField('diff', t.IntegerType(), False)
])

LONGEST_DF_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('longest', t.LongType(), False)
])

SCHEMA_LONGEST_AFTER_DF_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('longest_since_end_date', t.LongType(), False)
])

RESULT_DF_TASK_7 = t.StructType([
    t.StructField('patient_id', t.StringType(), False),
    t.StructField('longest', t.LongType(), False),
    t.StructField('longest_since_end_date', t.LongType(), False)
])

SCHEMA_DATA_TASK_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('hours', t.MapType(t.StringType(), t.StringType(), False), False)
])

SCHEMA_EXPECTED_TASK_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('key', t.StringType(), False),
    t.StructField('value', t.StringType(), False),
    t.StructField('work_hours', t.DoubleType(), False),
    t.StructField('is_work_12_hours', t.BooleanType(), False),
    t.StructField('is_open_for_24h', t.BooleanType(), False)
])

RESULT_TEST_TASK_8_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('avg_hours_open', t.FloatType(), False),
    t.StructField('is_open_for_24h', t.BooleanType(), False)
])

SCHEMA_TASK_8_2_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('WiFi', t.StringType(), False)]), False)
])

SCHEMA_EXPECTED_TASK_8_2_1 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('free_wifi', t.BooleanType(), False)
])

SCHEMA_TASK_8_2_2 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('BusinessParking', t.StringType(), False),
        t.StructField('BikeParking', t.StringType(), False)
    ]), False)
])

SCHEMA_EXPECTED_TASK_8_2_2 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('has_car_parking', t.BooleanType(), False),
    t.StructField('has_bike_parking', t.BooleanType(), False)
])

SCHEMA_TASK_8_2_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.StringType(), False),
    t.StructField('attributes', t.StructType([
        t.StructField('RestaurantsPriceRange2', t.StringType(), False)]), False)
])

SCHEMA_EXPECTED_TASK_8_2_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('categories', t.ArrayType(t.StringType()), False),
    t.StructField('price_range', t.IntegerType(), False),
    t.StructField('has_cafe', t.BooleanType(), False),
    t.StructField('has_store', t.BooleanType(), False)
])

SCHEMA_TASK_8_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('date', t.StringType(), False)
])

SCHEMA_EXPECTED_TASK_8_3 = t.StructType([
    t.StructField('business_id', t.StringType(), False),
    t.StructField('year', t.IntegerType(), False),
    t.StructField('checkin_count', t.IntegerType(), False)
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
    t.StructField('name', t.StringType(), False),
])

SCHEMA_EXPECTED_TASK_8_4 = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('friend', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False)
])

SCHEMA_EXPECTED_BUSINESSES = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('name', t.StringType(), False),
    t.StructField('friend', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False),
    t.StructField('business_name', t.StringType(), False)
])

SCHEMA_EXPECTED_8_4 = t.StructType([
    t.StructField('user_id', t.StringType(), False),
    t.StructField('user_name', t.StringType(), False),
    t.StructField('business_id', t.StringType(), False),
    t.StructField('business_name', t.StringType(), False),
    t.StructField('friends_attendees', t.ArrayType(t.StructType([
        t.StructField('user_id', t.StringType(), False),
        t.StructField('user_name', t.StringType(), False)
    ])), False)
])
