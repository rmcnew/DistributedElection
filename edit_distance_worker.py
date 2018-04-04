import logging

from edit_distance.edit_distance import EditDistanceCalculator
from messages import *


class EditDistanceWorker:

    def __init__(self, my_id, work_in_queue, work_out_queue):
        self.my_id = my_id
        self.work_in_queue = work_in_queue
        self.work_out_queue = work_out_queue
        self.calculator = EditDistanceCalculator()

    def run(self):
        while True:
            # blocking wait to get the next message
            message = self.work_in_queue.get()
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.debug("Shutting down . . .")
                break
            elif message[MESSAGE_TYPE] == WORK_RESPONSE:
                # a work_response_message is received
                logging.debug("Received work_response message: {}".format(message))
                string_pair_id = message[STRING_PAIR_ID]
                string_a = message[STRING_A]
                string_b = message[STRING_B]
                edit_distance = self.calculator.calculate_edit_distance(string_a, string_b)
                result = work_result_message(self.my_id, string_pair_id, edit_distance)
                self.work_out_queue.put(result)
            else:
                logging.error("EditDistanceWorker does not know how to handle message_type: {}!  "
                              "Received this message:  {}".format(message[MESSAGE_TYPE], message))
