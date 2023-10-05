"""
The main file which run every function
"""
from task_solver.const import REVTOBUCKET_FILE, HE_FILE
from task_solver.solve_task2 import SolveTask2
from task_solver.find_business_hours import FindBusinessHours
from task_solver.find_business_wifi import FindBusinessWiFi
from task_solver.find_business_parking import FindBusinessParking
from task_solver.find_business_gas_station import FindBusinessGasStation
from task_solver.find_business_checkin import FindBusinessCheckin
from task_solver.find_business_friends_checkin import FindBusinessFriendsCheckin


def main():
    """
        main function which have all functions which run all Task-solvers
    """
    task2 = SolveTask2(rev_code_path=REVTOBUCKET_FILE,
                       he_path=HE_FILE)
    task2.solve_task2()

    find_business_hours_obj = FindBusinessHours()
    find_business_hours_obj.run()

    find_business_wifi_obj = FindBusinessWiFi()
    find_business_wifi_obj.run()

    find_business_parking_obj = FindBusinessParking()
    find_business_parking_obj.run()

    find_business_gas_station_obj = FindBusinessGasStation()
    find_business_gas_station_obj.run()

    find_business_checkin_obj = FindBusinessCheckin()
    find_business_checkin_obj.run()

    find_business_friends_checkin_obj = FindBusinessFriendsCheckin()
    find_business_friends_checkin_obj.run()


if __name__ == '__main__':
    main()
