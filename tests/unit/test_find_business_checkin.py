import sys
from mock import patch

from task_solver.find_business_checkin import FindBusinessCheckin
from tests.schemas import EXPECTED_BUSINESS_CHECKIN_8_3
from tests.utils import assert_df_equal

sys.path.append('/')


def test_transform_data(spark):
    input_data = [
        ('1', '2016-04-26 19:49:16, 2016-08-30 18:36:57, 2016-10-15 02:45:18, 2016-11-18 01:54:50,2016-12-12 12:11:14'),
        ('2', '2017-10-16 10:01:21, 2017-10-24 11:42:23, 2018-05-04 11:42:23, 2019-11-18 09:23:56'),
        ('3', '2014-02-03 01:01:01, 2014-11-24 01:01:01, 2014-07-15 01:01:01, 2015-07-07 01:01:01'),
        ('4', '2011-02-03 01:01:01, 2011-01-24 01:01:01, 2011-09-12 01:01:01, 2011-06-06 01:01:01'),
        ('5', '2020-02-03 01:01:01, 2020-06-24 01:01:01, 2021-08-02 01:01:01, 2020-05-12 01:01:01'),
        ('6', '2010-10-11 19:49:16, 2014-06-23 18:36:57, 2018-11-13 02:45:18, 2020-11-04 01:54:50'),
        ('7', None),
    ]

    input_df = spark.createDataFrame(data=input_data, schema=['business_id', 'date'])

    expected_data = [
        ('1', 2016, 5),
        ('2', 2017, 2),
        ('2', 2018, 1),
        ('2', 2019, 1),
        ('3', 2014, 3),
        ('3', 2015, 1),
        ('4', 2011, 4),
        ('5', 2020, 3),
        ('5', 2021, 1),
        ('6', 2010, 1),
        ('6', 2014, 1),
        ('6', 2018, 1),
        ('6', 2020, 1),
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=EXPECTED_BUSINESS_CHECKIN_8_3)

    def __init__(self):
        self.spark = spark
        self.checkin_df = input_df

    with patch('task_solver.find_business_checkin.FindBusinessCheckin.__init__', __init__):
        find_business_checkin_obj = FindBusinessCheckin()

    actual_df = find_business_checkin_obj.transform_data()

    assert_df_equal(expected_df, actual_df)
