import sys
from mock import patch

from task_solver.find_business_hours import FindBusinessHours
from tests.schemas import SCHEMA_WORKS_HOURS_8_1, EXPECTED_WORKS_HOURS_8_1
from tests.utils import assert_df_equal

sys.path.append('/')


def test_transform_data(spark):
    input_data = [
        ('1', 'business_1', {'Monday': '8:0-18:0',
                             'Tuesday': '11:0-20:0',
                             'Wednesday': '10:0-18:0',
                             'Thursday': '11:0-20:0',
                             'Friday': '11:0-20:0'}),
        ('2', 'business_2', None),
        ('3', 'business_3', {'Monday': '7:0-19:0',
                             'Tuesday': '0:0-0:0',
                             'Wednesday': '7:0-19:0',
                             'Thursday': '5:0-23:0',
                             'Saturday': '7:30-13:30'}),
        ('4', 'business_4', {'Monday': '12:0-19:0',
                             'Tuesday': '0:0-22:0',
                             'Wednesday': '7:0-19:0',
                             'Thursday': '17:30-23:0',
                             'Saturday': '7:30-13:30'}),
        ('5', 'business_5', {'Monday': '12:0-23:0',
                             'Tuesday': '6:30-15:40',
                             'Wednesday': '12:0-0:0'})
    ]

    input_df = spark.createDataFrame(data=input_data, schema=SCHEMA_WORKS_HOURS_8_1)

    expected_data = [
        ('3', 'business_3', float(10.285714), True),
        ('4', 'business_4', float(7.5), False),
        ('5', 'business_5', float(4.595238), False)
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_WORKS_HOURS_8_1)

    def __init__(self):
        self.spark = spark
        self.business_df = input_df

    with patch('task_solver.find_business_hours.FindBusinessHours.__init__', __init__):
        find_business_hours_obj = FindBusinessHours()

    actual_df = find_business_hours_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
