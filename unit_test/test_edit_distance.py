import os
import os.path
from pathlib import Path
import unittest

from shared.constants import *
from edit_distance.edit_distance import EditDistanceCalculator

class TestEditDistance(unittest.TestCase):

    def test_calculate_edit_distance_from_file(self):
        print("\n=== test_calculate_edit_distance_from_file ===")
        script_directory = os.path.dirname(os.path.realpath(__file__)) 
        test_data_dir = Path(script_directory).parent / TEST_DATA / EDIT_DISTANCE
        string_pair_test_file1 = test_data_dir / "stringPairTest1.txt"
        print("test file 1 is: {}".format(string_pair_test_file1))
        self.assertTrue(os.path.isfile(string_pair_test_file1))
        string_pair_test_file2 = test_data_dir / "stringPairTest2.txt"
        print("test file 2 is: {}".format(string_pair_test_file2))
        self.assertTrue(os.path.isfile(string_pair_test_file2))
        edit_distance_calc = EditDistanceCalculator()
        edit_distance1_expected = 3
        edit_distance1_result = edit_distance_calc.calculate_edit_distance_from_file(str(string_pair_test_file1))
        print("test 1 result versus expected: {} ?= {}".format(edit_distance1_result, edit_distance1_expected))
        self.assertEqual(edit_distance1_result, edit_distance1_expected)
        edit_distance2_expected = 3
        edit_distance2_result = edit_distance_calc.calculate_edit_distance_from_file(str(string_pair_test_file2))
        print("test 2 result versus expected: {} ?= {}".format(edit_distance2_result, edit_distance2_expected))
        self.assertEqual(edit_distance2_result, edit_distance2_expected)

    def test_calculate_edit_distance(self):
        print("\n=== test_calculate_edit_distance ===")
        edit_distance_calc = EditDistanceCalculator()
        test_str_1a = "ISLANDER"
        test_str_1b = "SLANDER"
        edit_distance1_expected = 1
        edit_distance1_result = edit_distance_calc.calculate_edit_distance(test_str_1a, test_str_1b)
        print("test 1 result versus expected: {} ?= {}".format(edit_distance1_result, edit_distance1_expected))
        self.assertEqual(edit_distance1_result, edit_distance1_expected)
        test_str_2a = "MART"
        test_str_2b = "KARMA"
        edit_distance2_expected = 3
        edit_distance2_result = edit_distance_calc.calculate_edit_distance(test_str_2a, test_str_2b)
        print("test 2 result versus expected: {} ?= {}".format(edit_distance2_result, edit_distance2_expected))
        self.assertEqual(edit_distance2_result, edit_distance2_expected)
        test_str_3a = "KITTEN"
        test_str_3b = "SITTING"
        edit_distance3_expected = 3
        edit_distance3_result = edit_distance_calc.calculate_edit_distance(test_str_3a, test_str_3b)
        print("test 3 result versus expected: {} ?= {}".format(edit_distance3_result, edit_distance3_expected))
        self.assertEqual(edit_distance3_result, edit_distance3_expected)
        test_str_4a = "INTENTION"
        test_str_4b = "EXECUTION"
        edit_distance4_expected = 5
        edit_distance4_result = edit_distance_calc.calculate_edit_distance(test_str_4a, test_str_4b)
        print("test 4 result versus expected: {} ?= {}".format(edit_distance4_result, edit_distance4_expected))
        self.assertEqual(edit_distance4_result, edit_distance4_expected)
        



if __name__ == '__main__':
    sys.path.insert(0, "..")
    unittest.main()
