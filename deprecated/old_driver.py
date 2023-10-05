"""
The main file which run every function
"""

from deprecated.old_task_solver import REVTOBUCKET_FILE, HE_FILE, RATIO_FILE, HOSPITAL_ENC_FILE, \
    TASK4_DATA_FILE, TASK5_1_DATA_FILE, TASK5_2_DATA_FILE, TASK6_DATA, TASK7_DATA, RESULT_FILE_TASK_7, \
    TASK8_1_DATA, RESULT_FILE_TASK_8_1, RESULT_FILE_TASK_8_2_1, RESULT_FILE_TASK_8_2_2, RESULT_FILE_TASK_8_2_3, \
    TASK8_3_DATA, RESULT_FILE_TASK_8_3, USERS_DATA, REVIEWS_DATA, TIPS_DATA, RESULT_FILE_TASK_8_4
from deprecated.old_task_solver import solv_task2
from deprecated.old_task_solver import solv_task3
from deprecated.old_task_solver import solv_task4
from deprecated.old_task_solver import solv_task5_1
from deprecated.old_task_solver import solv_task5_2
from deprecated.old_task_solver.solv_task6 import solv_task_6
from deprecated.old_task_solver.solv_task7 import solv_task_7
from deprecated.old_task_solver import solv_task_8_1
from deprecated.old_task_solver import solv_task_8_2_1
from deprecated.old_task_solver import solv_task_8_2_2
from deprecated.old_task_solver.solv_task8_2_3 import solv_task_8_2_3
from deprecated.old_task_solver.solv_task8_3 import solv_task_8_3
from deprecated.old_task_solver.solv_task8_4 import solv_task_8_4
from utils import spark_builder

spark = spark_builder()


def main():
    """
    main function which have all functions which need to do the Task
    """

    solv_task2(spark=spark,
               revtbckt_path=REVTOBUCKET_FILE,
               he_path=HE_FILE,
               num=50)

    solv_task3(spark=spark,
               ratio_path=RATIO_FILE,
               hospitalenc_path=HOSPITAL_ENC_FILE)

    solv_task4(spark=spark,
               file_path=TASK4_DATA_FILE)

    solv_task5_1(spark=spark,
                 file_path=TASK5_1_DATA_FILE)

    solv_task5_2(spark=spark,
                 file_path=TASK5_2_DATA_FILE)

    solv_task_6(spark=spark,
                file_path=TASK6_DATA)

    solv_task_7(spark=spark,
                file_path=TASK7_DATA,
                save_file_path=RESULT_FILE_TASK_7)

    solv_task_8_1(spark=spark,
                  file_path=TASK8_1_DATA,
                  save_file_path=RESULT_FILE_TASK_8_1)

    solv_task_8_2_1(spark=spark,
                    file_path=TASK8_1_DATA,
                    save_file_path=RESULT_FILE_TASK_8_2_1)

    solv_task_8_2_2(spark=spark,
                    file_path=TASK8_1_DATA,
                    save_file_path=RESULT_FILE_TASK_8_2_2)

    solv_task_8_2_3(spark=spark,
                    file_path=TASK8_1_DATA,
                    save_file_path=RESULT_FILE_TASK_8_2_3)

    solv_task_8_3(spark=spark,
                  file_path=TASK8_3_DATA,
                  save_file_path=RESULT_FILE_TASK_8_3)

    solv_task_8_4(spark=spark,
                  file_path_users=USERS_DATA,
                  file_path_review=REVIEWS_DATA,
                  file_path_tips=TIPS_DATA,
                  file_path_businesses=TASK8_1_DATA,
                  save_file_path=RESULT_FILE_TASK_8_4)


if __name__ == "__main__":
    main()
