"""
The file with ALL needed constants
"""
import os

NUMBER_CODES = 51

DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

DATE_FORMAT = 'MM/dd/yyyy'
DATE_FORMAT_FULL = 'MMddyyyy'

DATA_PATH = os.path.join(os.path.abspath((os.path.join(os.path.dirname(__file__), '..'))), 'data')

DATA_TEST_PATH = os.path.join(os.path.abspath((os.path.join(os.path.dirname(__file__), '..'))), 'tests', 'input_data')

# Consts for Task2
REVTOBUCKET_FILE = os.path.join(DATA_PATH, 'input_data_task_2', 'REVTOBUCKET')

HE_FILE = os.path.join(DATA_PATH, 'input_data_task_2', 'hospitalEncounter.csv')

REVTOBUCKET_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task2', 'test_rev')

HE_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task2', 'test_HE.csv')

PREPARED_REVTBCKT_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'cast_revtobucket')

PREPARED_HE_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'casted_he_df.csv')

RESULT_TEST_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'test_result.csv')

RESULT_FILE = os.path.join(DATA_PATH, 'output_data_task_2', 'result.csv')

# Consts for Task8

BUSINESS_DATASET = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_business.json')

USERS_DATASET = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_user.json')

REVIEWS_DATASET = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_review.json')

TIPS_DATASET = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_tip.json')

CHECKIN_DATASET = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_checkin.json')

RESULT_WORKING_HOURS = os.path.join(DATA_PATH, 'output_data_task_8', 'result_working_hours.csv')

RESULT_FREE_WIFI = os.path.join(DATA_PATH, 'output_data_task_8', 'result_free_wifi.csv')

RESULT_BUSINESS_PARKING = os.path.join(DATA_PATH, 'output_data_task_8', 'result_business_parking.csv')

RESULT_BUSINESS_GAS_STATION = os.path.join(DATA_PATH, 'output_data_task_8', 'result_business_gas_station.csv')

RESULT_BUSINESS_CHECKIN = os.path.join(DATA_PATH, 'output_data_task_8', 'result_business_checking.csv')

RESULT_BUSINESS_FRIENDS_CHECKIN = os.path.join(DATA_PATH, 'output_data_task_8', 'result_business_friends_checking.csv')

TEST_BUSINESS_DATASET = os.path.join(DATA_TEST_PATH, 'task8', 'business.json')

TEST_USERS_DATASET = os.path.join(DATA_TEST_PATH, 'task8', 'users.json')

TEST_REVIEWS_DATASET = os.path.join(DATA_TEST_PATH, 'task8', 'reviews.json')

TEST_TIPS_DATASET = os.path.join(DATA_TEST_PATH, 'task8', 'tips.json')

TEST_CHECKIN_DATASET = os.path.join(DATA_TEST_PATH, 'task8', 'checkin.json')
