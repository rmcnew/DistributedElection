import time

from edit_distance.edit_distance import EditDistanceCalculator
from shared.constants import *


class EditDistanceWorker:

    def __init__(self, work_in_queue, work_out_queue):
        self.work_in_queue = work_in_queue
        self.work_out_queue = work_out_queue
        self.calculator = EditDistanceCalculator()

    def run(self):
        while True:
            if not self.work_in_queue.empty():
                (string_pair_id, string_a, string_b) = self.work_in_queue.get(block=False,
                                                                              timeout=WORK_IN_QUEUE_TIMEOUT)
                print("Received from work_in_queue: string_pair_id={}, string_a={}, string_b={}"
                      .format(string_pair_id, string_a, string_b))
                if string_pair_id == SHUTDOWN:
                    break
                edit_distance = self.calculator.calculate_edit_distance(string_a, string_b)
                ret_val = (string_pair_id, edit_distance)
                self.work_out_queue.put(ret_val)
            time.sleep(EDIT_DISTANCE_WORKER_SLEEP)
