import sys
from mock import patch
from datetime import date
from decimal import Decimal

from task_solver.solve_task2 import SolveTask2
from tests.schemas import SCHEMA_REVBCKT_DATA, SCHEMA_HE_DATA, SCHEMA_PREPARING_REVBCKT_DF, SCHEMA_PREPARING_HE_DF, \
    SCHEMA_SUM_SELECT_DF
from tests.utils import assert_df_equal

sys.path.append('/')


def test_preparing_revbckt_df(spark):
    input_data = [
        (0, '0974', '1/1/1990', '12/31/2099'),
        (0, '0988', '1/1/1990', '12/31/2099'),
        (0, '0987', '1/1/1990', '12/31/2099'),
        (0, '0657', '2/1/1992', '12/31/2099'),
        (0, '0960', '2/1/1993', '12/31/2099'),
        (0, '0961', '2/1/1994', '12/31/2099'),
        (0, '0962', '1/1/1990', '12/31/2099'),
        (0, '0963', '1/1/1990', '12/31/2099'),
        (0, '0964', '1/1/1990', '12/31/2099'),
        (0, '0969', '1/1/1990', '12/31/2099'),
        (0, '0971', '5/5/2003', '12/31/2099'),
        (0, '0972', '1/1/1990', '12/31/2099'),
        (0, '0973', '1/1/2000', '12/31/2099')
    ]

    expected_data = [
        (0, 974, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 988, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 987, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 657, date(1992, 2, 1), date(2099, 12, 31)),
        (0, 960, date(1993, 2, 1), date(2099, 12, 31)),
        (0, 961, date(1994, 2, 1), date(2099, 12, 31)),
        (0, 962, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 963, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 964, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 969, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 971, date(2003, 5, 5), date(2099, 12, 31)),
        (0, 972, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 973, date(2000, 1, 1), date(2099, 12, 31))
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=SCHEMA_PREPARING_REVBCKT_DF)

    input_df = spark.createDataFrame(data=input_data, schema=SCHEMA_REVBCKT_DATA)

    def __init__(self):
        self.spark = spark

    with patch('task_solver.solve_task2.SolveTask2.__init__', __init__):
        solv_task2 = SolveTask2()

    actual_df = solv_task2.preparing_rev_df(rev_code_df=input_df)

    assert_df_equal(expected_df, actual_df)


def test_preparing_hospital_enc_df(spark):
    input_data = [
        (1, '03022010', 1710, 251, 3328.63, 588, 650, 600, 250, 270),
        (2, '03022011', 123, 255, 4444.44, 688, 1, 450, 3, 6)
    ]

    expected_data = [
        (1, date(2010, 3, 2), 650, Decimal(1710.00)),
        (1, date(2010, 3, 2), 600, Decimal(251.00)),
        (1, date(2010, 3, 2), 250, Decimal(3328.63)),
        (1, date(2010, 3, 2), 270, Decimal(588.00)),
        (2, date(2011, 3, 2), 1, Decimal(123.00)),
        (2, date(2011, 3, 2), 450, Decimal(255.00)),
        (2, date(2011, 3, 2), 3, Decimal(4444.44)),
        (2, date(2011, 3, 2), 6, Decimal(688.00))
    ]

    expected_df = spark.createDataFrame(data=expected_data, schema=SCHEMA_PREPARING_HE_DF)

    input_df = spark.createDataFrame(data=input_data, schema=SCHEMA_HE_DATA)

    def __init__(self):
        self.spark = spark

    with patch('task_solver.solve_task2.SolveTask2.__init__', __init__):
        solv_task2 = SolveTask2()

    with patch('task_solver.solve_task2.NUMBER_CODES', 5):
        actual_df = solv_task2.preparing_hospital_enc_df(he_df=input_df)

    assert_df_equal(expected_df, actual_df)


def test_income_for_service(spark):
    input_revtbckt_data = [
        (0, 0, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 1, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 25, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 36, date(1992, 2, 1), date(2099, 12, 31)),
        (0, 958, date(1993, 2, 1), date(2099, 12, 31)),
        (0, 961, date(1994, 2, 1), date(2099, 12, 31)),
        (0, 962, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 555, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 662, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 111, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 358, date(2003, 5, 5), date(2099, 12, 31)),
        (0, 11, date(1990, 1, 1), date(2099, 12, 31)),
        (0, 22, date(2000, 1, 1), date(2099, 12, 31))
    ]

    input_he_data = [
        (1, date(2010, 3, 2), 0, Decimal(1710.00)),
        (1, date(2010, 3, 2), 600, Decimal(251.00)),
        (1, date(2010, 3, 2), 0, Decimal(3328.63)),
        (1, date(2010, 3, 2), 270, Decimal(588.00)),
        (2, date(2011, 3, 2), 1, Decimal(123.00)),
        (2, date(2011, 3, 2), 450, Decimal(255.00)),
        (2, date(2011, 3, 2), 36, Decimal(4444.44)),
        (3, date(2015, 3, 2), 22, Decimal(688.00)),
        (3, date(2011, 3, 2), 11, Decimal(12.00)),
        (3, date(2014, 3, 2), 22, Decimal(35.00)),
        (4, date(2013, 3, 2), 454, Decimal(58.89)),
        (4, date(1985, 3, 2), 11, Decimal(7889.89)),
        (4, date(2012, 3, 2), 987, Decimal(564.55))

    ]

    expected_data = [
        (0, Decimal(5038.63)),
        (1, Decimal(123.00)),
        (11, Decimal(12.00)),
        (22, Decimal(723.00)),
        (36, Decimal(4444.44))
    ]

    input_he_df = spark.createDataFrame(data=input_he_data, schema=SCHEMA_PREPARING_HE_DF)

    input_rev_df = spark.createDataFrame(data=input_revtbckt_data, schema=SCHEMA_PREPARING_REVBCKT_DF)

    def __init__(self):
        self.spark = spark

    with patch('task_solver.solve_task2.SolveTask2.__init__', __init__):
        solv_task2 = SolveTask2()

    actual_df = solv_task2.income_for_service(
        csv_dataframe=input_he_df,
        bucket_dataframe=input_rev_df)

    expected_df = spark.createDataFrame(data=expected_data, schema=SCHEMA_SUM_SELECT_DF)

    assert_df_equal(expected_df, actual_df)
