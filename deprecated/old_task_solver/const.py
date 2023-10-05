"""
The file with ALL needed constants
"""
import os

NUMBER_CODS = 50

DATE_FORMAT = 'MM/dd/yyyy'
DATE_FORMAT_FULL = 'MMddyyyy'

DATA_PATH = os.path.join(os.path.abspath((os.path.join(os.path.dirname(__file__), '../..'))), 'data')

DATA_TEST_PATH = os.path.join(os.path.abspath((os.path.join(os.path.dirname(__file__), '../..'))), 'tests', 'data')

# Consts for Task2
REVTOBUCKET_FILE = os.path.join(DATA_PATH, 'input_data_task_2', 'REVTOBUCKET')

HE_FILE = os.path.join(DATA_PATH, 'input_data_task_2', 'hospitalEncounter.csv')

REVTOBUCKET_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task2', 'test_rev')

HE_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task2', 'test_HE.csv')

PREPARED_REVTBCKT_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'cast_revtobucket')

PREPARED_HE_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'casted_he_df.csv')

RESULT_TEST_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task2', 'test_result.csv')

RESULT_FILE = os.path.join(DATA_PATH, 'output_data_task_2', 'result.csv')

# Consts for Task3
HOSPITAL_ENC_FILE = os.path.join(DATA_PATH, 'input_data_task_3', 'hospitlEncounter.csv')

RATIO_FILE = os.path.join(DATA_PATH, 'input_data_task_3', 'RATIO9')

RESULT_FILE_TASK_3 = os.path.join(DATA_PATH, 'output_data_task_3', 'result_task3.json')

HENC_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task3', 'HE_test.csv')

RAT_TEST_FILE = os.path.join(DATA_TEST_PATH, 'input_data', 'task3', 'RAT_test')

PREPARED_RATIO_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task3', 'prepared_rat_df.json')

PREPARED_HENC_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task3', 'prepared_he_df.csv')

PREPARED_HENC_FOR_JOIN = os.path.join(DATA_TEST_PATH, 'expected_data', 'task3', 'prepared_for_join_henc_file.csv')

RESULT_TASK3_FILE = os.path.join(DATA_TEST_PATH, 'expected_data', 'task3', 'result_task3.json')

# Consts for Task4

TASK4_DATA_FILE = os.path.join(DATA_PATH, 'input_data_task_4', 'task4_data.csv')

RESULT_FILE_TASK_4 = os.path.join(DATA_PATH, 'output_data_task_4', 'result_task4.csv')

TEST_DATA_TASK4 = os.path.join(DATA_TEST_PATH, 'input_data', 'task4', 'data_task4.csv')

PREPARED_PROC_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task4', 'prepared_proc_df.csv')

PREPARED_FAC_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task4', 'prepared_fac_df.csv')

PREPARED_PROC_DF = os.path.join(DATA_TEST_PATH, 'input_data', 'task4', 'proc_df.csv')

PREPARED_FAC_DF = os.path.join(DATA_TEST_PATH, 'input_data', 'task4', 'fac_df.csv')

PREPARED_RESULT_PROC_FAC_DF = os.path.join(DATA_TEST_PATH, 'expected_data', 'task4', 'result_df.csv')

# Consts for Task5

TASK5_1_DATA_FILE = os.path.join(DATA_PATH, 'input_data_task_5', 'task5.1_data.csv')

RESULT_TASK5_1_DATA_FILE = os.path.join(DATA_PATH, 'output_data_task_5', 'result_task5.1_data.csv')

TEST_DATA_TASK5_1 = os.path.join(DATA_TEST_PATH, 'input_data', 'task5', 'data_task_1.csv')

RESULT_TEST_DATA_TASK5_1 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task5', 'result_df_task5_1.csv')

TASK5_2_DATA_FILE = os.path.join(DATA_PATH, 'input_data_task_5', 'task5.2_data.csv')

RESULT_TASK5_2_DATA_FILE = os.path.join(DATA_PATH, 'output_data_task_5', 'result_task5.2_data.csv')

TEST_TASK5_2_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task5', 'data_task_2.csv')

SUMMED_DATA_TASK5_2 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task5', 'summed_data_task5_2.csv')

TEST_SUMMED_DATA_TASK5_2 = os.path.join(DATA_TEST_PATH, 'input_data', 'task5', 'summed_task5_2.csv')

RESULT_TEST_DATA_TASK5_2 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task5', 'result_df_task5_2.csv')

# Consts for Task6

TASK6_DATA = os.path.join(DATA_PATH, 'input_data_task_6', 'enroll.csv')

RESULT_TASK6_DATA = os.path.join(DATA_PATH, 'output_data_task_6', 'result_task6.csv')

TEST_TASK6_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task6', 'data_task6.csv')

RESULT_TEST_TASK6_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task6', 'result_task_6.csv')

# Consts for Task7

TASK7_DATA = os.path.join(DATA_PATH, 'input_data_task_7', 'task7.csv')

TEST_TASK7_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task7', 'test.csv')

PREPARED_TASK7_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task7', 'prepared_df.csv')

TEST_FILE_ABS = os.path.join(DATA_TEST_PATH, 'input_data', 'task7', 'res1.txt')

LONGEST_VISITS_DF_TASK_7 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task7', 'longest_visits_to_end_date.csv')

LONGEST_AFTER_DF_TASK_7 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task7', 'longest_visits_after_end_date.csv')

RESULT_DATA_TASK_7 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task7', 'result_df.csv')

RESULT_FILE_TASK_7 = os.path.join(DATA_PATH, 'output_data_task_7', 'result_task7.csv')

RESULT_TEST_FILE_TASK_7 = os.path.join(DATA_TEST_PATH, 'output_data', 'task7', 'result_task7.csv')

# Consts for Task8_1

TASK8_1_DATA = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_business.json')

RESULT_FILE_TASK_8_1 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_1.csv')

TEST_DATA_TASK_8_1 = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'test.json')

WORKING_HOURS_TASK_8_1 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'working_hours.csv')

RESULT_DATA_TASK_8_1 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'result.csv')

RESULT_TEST_DATA_TASK8_1 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_1.csv')

# Consts for Task8_2

RESULT_FILE_TASK_8_2_1 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_2_1.csv')

TEST_DATA_TASK_8_2_1 = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'test8_2_1.json')

FREE_WIFI_TASK_8_2_1 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'wifi_df.csv')

RESULT_TEST_DATA_TASK_8_2_1 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_2_1.csv')

RESULT_FILE_TASK_8_2_2 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_2_2.csv')

TEST_DATA_TASK_8_2_2 = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'test8_2_2.json')

PARKING_TASK_8_2_2 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'parking_df.csv')

RESULT_TEST_DATA_TASK_8_2_2 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_2_2.csv')

RESULT_FILE_TASK_8_2_3 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_2_3.json')

TEST_DATA_TASK_8_2_3 = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'test8_2_3.json')

GAS_STATION_TASK_8_2_3 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'gas_stations.json')

RESULT_TEST_DATA_TASK_8_2_3 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_2_3.json')

# Consts for Task8_3

TASK8_3_DATA = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_checkin.json')

RESULT_FILE_TASK_8_3 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_3.csv')

TEST_DATA_TASK_8_3 = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'test8_3.json')

CHECKIN_TASK_8_3 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'checkin_business.csv')

RESULT_TEST_DATA_TASK_8_3 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_3.csv')

# Consts for Task8_4

USERS_DATA = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_user.json')

REVIEWS_DATA = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_review.json')

TIPS_DATA = os.path.join(DATA_PATH, 'input_data_task_8', 'yelp_academic_dataset_tip.json')

RESULT_FILE_TASK_8_4 = os.path.join(DATA_PATH, 'output_data_task_8', 'result_task8_3.json')

TEST_USERS_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'users_data.json')

TEST_REVIEWS_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'reviews_data.json')

TEST_TIPS_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'tips_data.json')

TEST_BUSINESSES_DATA = os.path.join(DATA_TEST_PATH, 'input_data', 'task8', 'businesses_info.json')

USERS_FRIENDS_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'users_friends.json')

USERS_BUSINESSES_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'users_businesses_info.json')

RESULT_USERS_DATA = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'result_8_4.json')

RESULT_TEST_DATA_TASK_8_4 = os.path.join(DATA_TEST_PATH, 'output_data', 'task8', 'result_task8_4.json')

EXPECTED_RESULT_8_4 = os.path.join(DATA_TEST_PATH, 'expected_data', 'task8', 'expected_result_8_4.json')
