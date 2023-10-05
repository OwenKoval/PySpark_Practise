import sys
from mock import patch

from task_solver.find_business_parking import FindBusinessParking
from tests.schemas import EXPECTED_PARKING_8_2_2
from tests.utils import assert_df_equal

sys.path.append('/')


def test_transform_data(spark):
    input_data = [
        ('1', 'business_1', "{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': False}",
         'False'),
        ('2', 'business_2', None, 'False'),
        ('3', 'business_3', "{'garage': False, 'street': True, 'validated': False, 'lot': False, 'valet': True}",
         'True'),
        ('4', 'business_4', "None", None),
        ('5', 'business_5', "{'garage': False, 'street': False, 'validated': True, 'lot': False, 'valet': False}",
         'True'),
        ('6', 'business_6', "{'garage': True, 'street': False, 'validated': False, 'lot': False, 'valet': False}",
         'None'),
        ('7', 'business_7', "{'garage': False, 'street': False, 'validated': False, 'lot': False, 'valet': True}",
         'True'),
    ]

    input_df = spark.createDataFrame(data=input_data,
                                     schema=['business_id', 'name', 'business_parking', 'bike_parking'])

    expected_data = [
        ('1', 'business_1', False, False),
        ('2', 'business_2', False, False),
        ('3', 'business_3', True, True),
        ('4', 'business_4', False, False),
        ('5', 'business_5', False, True),
        ('6', 'business_6', True, False),
        ('7', 'business_7', False, True)
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_PARKING_8_2_2)

    def __init__(self):
        self.spark = spark
        self.business_df = input_df

    with patch('task_solver.find_business_parking.FindBusinessParking.__init__', __init__):
        find_business_parking_obj = FindBusinessParking()

    actual_df = find_business_parking_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
