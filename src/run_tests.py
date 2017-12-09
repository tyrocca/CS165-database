"""
Simple python script to run all tests.
Takes in three optional args:
    --test_nums : which test numbers to run
    --milestone : which milestone to run up to
    --large     : bull flag if want to run large data sets
"""
import os
import sys
import time


##
# @brief Function to run python tests
#
# @param test_nums
# @param milestone
# @param large
#
# @return
def run_tests(test_nums=[], milestone=5, large=False):
    if not test_nums:
        os.system("make clean > /dev/null")
        test_nums = {
            1: range(1, 10),
            2: range(1, 18),
            3: range(1, 30),
            4: range(1, 36),
            5: range(1, 42)
        }[milestone]

    # make
    os.system("make > /dev/null")

    test_dir = "../project_tests"
    if large:
        test_dir += "_1M/"
    else:
        test_dir += "/"

    server_running = False
    # run all tests
    for test_num in test_nums:
        # if server not running run
        if not server_running:
            os.system("./server > server.out &")
            server_running = True
            time.sleep(1)

        if test_num < 10:
            test_str = "0%d" % test_num
        else:
            test_str = str(test_num)

        print("Running test%s: " % test_str, end="")

        # run test
        start = time.time()
        os.system("./client < %stest%s.dsl > %stest%s.out" % (test_dir, test_str, test_dir, test_str))
        end = time.time()

        # check output if large
        if large:
            exp = open("%stest%s.exp" % (test_dir, test_str), "r").read()
            out = open("%stest%s.out" % (test_dir, test_str), "r").read()

            if exp == out:
                print("%s us" % int((end - start) * 1000000))
            else:
                print("%s us (FAIL)" % int((end - start) * 1000000))

        # check if shutdown command at end of test
        if "shutdown" in open("%stest%s.dsl" % (test_dir, test_str), "r").read():
            server_running = False


if __name__ == "__main__":
    milestone = 5
    test_nums = []
    large = False

    # parse args
    i = 1
    while i < len(sys.argv):
        arg_name = sys.argv[i]
        i += 1

        if arg_name == "--milestone":
            arg_value = sys.argv[i]
            i += 1

            if arg_value not in ["1", "2", "3", "4", "5"]:
                print("Warning: invalid milestone arg -- must be 1,2,3,4 or 5")

            milestone = int(arg_value)

        elif arg_name == "--test_nums":
            arg_value = sys.argv[i]
            i += 1

            test_nums = [int(item) for item in arg_value.split(",")]
            for num in test_nums:
                if num not in range(1, 42):
                    print("Warning: invalid test num %d -- must be between 1 and 41")
                    test_nums.remove(num)

        elif arg_name == "--large":
            large = True

    run_tests(test_nums, milestone, large)
