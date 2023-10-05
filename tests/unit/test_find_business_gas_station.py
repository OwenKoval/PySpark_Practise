import sys
from mock import patch

from task_solver.find_business_gas_station import FindBusinessGasStation
from tests.schemas import SCHEMA_TEST_GAS_STATION_8_2_3, EXPECTED_GAS_STATION_8_2_3
from tests.utils import assert_df_equal

sys.path.append('/')


def test_transform_data(spark):
    input_data = [
        ('1', 'business_1', 'Gas Station, Shopping, Store, Cafe', 2),
        ('2', 'business_2', 'Shopping', 3),
        ('3', 'business_3', None, 4),
        ('4', 'business_4', 'Gas Station, Cafe', 4),
        ('5', 'business_5', 'Gas Station, Store, Cafe', 1),
        ('6', 'business_6', 'Gas Station, Cafe', 3),
        ('7', 'business_7', '', 2),
        ('8', 'business_8', 'Gas Station, Store', 4),
        ('9', 'business_9', 'Gas Station, Store, Cafe', 7),
        ('10', 'business_10', 'Gas Station, Market, Cafe', 2),
        ('11', 'business_11', 'Gas Station, Grocery, Cafe', 1),
        ('12', 'business_12', 'Gas Station, Grocery', 3),
        ('13', 'business_13', 'Gas Station, Market', 4),
        ('14', 'business_14', 'Gas Station', 2)
    ]

    input_df = spark.createDataFrame(data=input_data, schema=SCHEMA_TEST_GAS_STATION_8_2_3)

    expected_data = [
        ('1', 'business_1', ['Gas Station', 'Shopping', 'Store', 'Cafe'], 2, True, True),
        ('5', 'business_5', ['Gas Station', 'Store', 'Cafe'], 1, True, True),
        ('6', 'business_6', ['Gas Station', 'Cafe'], 3, True, False),
        ('10', 'business_10', ['Gas Station', 'Market', 'Cafe'], 2, True, True),
        ('11', 'business_11', ['Gas Station', 'Grocery', 'Cafe'], 1, True, True),
        ('12', 'business_12', ['Gas Station', 'Grocery'], 3, False, True)
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_GAS_STATION_8_2_3)

    def __init__(self):
        self.spark = spark
        self.business_df = input_df

    with patch('task_solver.find_business_gas_station.FindBusinessGasStation.__init__', __init__):
        find_business_gas_station_obj = FindBusinessGasStation()

    actual_df = find_business_gas_station_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
