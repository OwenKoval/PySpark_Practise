import sys
from mock import patch

from task_solver.find_business_wifi import FindBusinessWiFi
from tests.schemas import EXPECTED_WIFI_8_2_1
from tests.utils import assert_df_equal

sys.path.append('/')


def test_transform_data(spark):
    input_data = [
        ('1', 'business_1', 'Wifi'),
        ('2', 'business_2', 'free Wifi'),
        ('3', 'business_3', None),
        ('4', 'business_4', 'u no'),
        ('5', 'business_5', 'free Wifi'),
    ]

    input_df = spark.createDataFrame(data=input_data, schema=['business_id', 'name', 'wifi'])

    expected_data = [
        ('1', 'business_1', False),
        ('2', 'business_2', True),
        ('3', 'business_3', False),
        ('4', 'business_4', False),
        ('5', 'business_5', True),
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_WIFI_8_2_1)

    def __init__(self):
        self.spark = spark
        self.business_df = input_df

    with patch('task_solver.find_business_wifi.FindBusinessWiFi.__init__', __init__):
        find_business_wifi_obj = FindBusinessWiFi()

    actual_df = find_business_wifi_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
