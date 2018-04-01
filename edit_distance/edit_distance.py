from ctypes import CDLL
from pathlib import Path


class EditDistanceCalculator:
    """Python wrapper for C implementation of Wagner-Fischer edit distance calculation"""

    def __init__(self):
        self.libpath = Path("edit_distance/libwagnerfischer.so").resolve()
        self.wagnerfischer = CDLL(self.libpath)

    def calculate_edit_distance(self, string_a, string_b):
        return self.wagnerfischer.calculate_edit_distance(string_a.encode(), string_b.encode())

    def calculate_edit_distance_from_file(self, filename):
        return self.wagnerfischer.calculate_edit_distance_from_string_pair_file(filename.encode())
