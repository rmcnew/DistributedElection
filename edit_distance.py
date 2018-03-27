from ctypes import CDLL


class EditDistanceCalculator:
    """Python wrapper for C implementation of Wagner-Fischer edit distance calculation"""

    def __init__(self):
        self.wagnerfischer = CDLL("./libwagnerfischer.so")

    def calculate_edit_distance(self, string_a, string_b):
        return self.wagnerfischer.calculate_edit_distance(string_a.encode(), string_b.encode())

    def calculate_edit_distance_from_file(self, filename):
        return self.wagnerfischer.calculate_edit_distance_from_string_pair_file(filename.encode())
