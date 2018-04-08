import logging

from edit_distance.edit_distance import EditDistanceCalculator
from messages import *


class EditDistanceWorker:

    def __init__(self, my_id, work_in_queue, work_out_queue):
        self.my_id = my_id
        self.work_in_queue = work_in_queue
        self.work_out_queue = work_out_queue
        self.calculator = EditDistanceCalculator()
        self.active_worker = False
        self.work_queue_primed = False
        self.work_requested = False

    def request_work(self):
        if self.work_queue_primed and self.active_worker and not self.work_requested:
            logging.info("Requesting work for ID {}".format(self.my_id))
            self.work_out_queue.put(work_request_message(self.my_id))

    def do_work(self, work_response_msg):
        if work_response_msg[REQUESTER_ID] == self.my_id:
            logging.info("Got work!  Running edit distance calculation")
            string_pair_id = work_response_msg[STRING_PAIR_ID]
            string_a = work_response_msg[STRING_A]
            string_b = work_response_msg[STRING_B]
            receipt_handle = work_response_msg[RECEIPT_HANDLE]
            edit_distance = self.calculator.calculate_edit_distance(string_a, string_b)
            result = work_result_message(self.my_id, string_pair_id, edit_distance, receipt_handle)
            logging.info("Work result ready!  Sending result message: {}".format(result))
            self.work_out_queue.put(result)
            self.work_requested = False

    def run(self):
        while True:
            # blocking wait to get the next message
            message = self.work_in_queue.get()
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.debug("Shutting down . . .")
                break
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_OVERSEER:
                self.active_worker = False
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_WORKER:
                self.active_worker = True
            elif message[MESSAGE_TYPE] == WORK_QUEUE_READY:
                self.work_queue_primed = True
                self.request_work()
            elif message[MESSAGE_TYPE] == WORK_RESPONSE:
                logging.debug("Received work_response message: {}".format(message))
                self.do_work(message)
            else:
                pass
