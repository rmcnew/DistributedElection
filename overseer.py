import logging

from aws.s3 import SimpleStorageService
from aws.work_queue import WorkQueue
from messages import *
from shared.shared import *


class Overseer:

    def __init__(self, my_id, overseer_in_queue, overseer_out_queue):
        self.my_id = my_id
        self.overseer_in_queue = overseer_in_queue
        self.overseer_out_queue = overseer_out_queue
        self.work_queue = WorkQueue()
        self.s3 = SimpleStorageService()
        self.active_overseer = False
        self.input_folder_contents = None
        self.input_folder_not_submitted = None
        self.work_queue_primed = False
        self.temp_dir = get_temp_dir()

    def setup_input_folder_not_submitted(self):
        self.input_folder_not_submitted = set()
        for key in self.input_folder_contents:
            self.input_folder_not_submitted.add(key)

    def broadcast_work_list(self):
        self.input_folder_contents = self.s3.list_input_folder_contents()
        self.overseer_out_queue.put(work_list_message(self.input_folder_contents))
        self.setup_input_folder_not_submitted()

    def prime_work_queue(self):
        for item in self.input_folder_contents:
            temp_file = item.replace("/", "_")
            temp_path = "{}/{}".format(self.temp_dir, temp_file)
            self.s3.download_string_pair(item, temp_path)
            string_pair_file = open(temp_path, READ_ONLY)
            string_a = string_pair_file.readline()
            string_b = string_pair_file.readline()
            self.work_queue.send_message(work_item_message(item, string_a, string_b))
            self.overseer_out_queue.put(prime_work_queue_message(item))
        # announce when the work queue is primed
        self.overseer_out_queue.put(work_queue_ready_message())
        self.work_queue_primed = True

    def handle_work_request(self, message):
        requester_id = message[REQUESTER_ID]
        work_item_msg = self.work_queue.receive_message()
        if work_item_msg is not None:
            string_pair_id = work_item_msg[STRING_PAIR_ID]
            string_a = work_item_msg[STRING_A]
            string_b = work_item_msg[STRING_B]
            self.overseer_out_queue.put(work_response_message(requester_id, string_pair_id, string_a, string_b))
        else:  # work_item_mgs is None
            # if the work queue is empty, notify workers individually to shutdown
            self.overseer_out_queue.put(individual_shutdown_message(requester_id))

    def upload_edit_distance(self, string_pair_id, edit_distance):
        temp_result_filename = "{}/{}_result".format(self.temp_dir, string_pair_id)
        temp_result_file = open(temp_result_filename, WRITE_ONLY)
        temp_result_file.write("{}, {}".format(string_pair_id, edit_distance))
        temp_result_file.close()
        self.s3.upload_string_pair_result("{}_result".format(string_pair_id), temp_result_filename)

    def handle_work_result_message(self, message):
        requester_id = message[REQUESTER_ID]
        string_pair_id = message[STRING_PAIR_ID]
        edit_distance = message[EDIT_DISTANCE]
        receipt_handle = message[RECEIPT_HANDLE]
        # upload the result to S3
        self.upload_edit_distance(string_pair_id, edit_distance)
        # send out result message
        self.overseer_out_queue.put(work_result_received_message(requester_id, string_pair_id))
        # delete the work queue message
        self.work_queue.delete_message(receipt_handle)

    def run(self):
        while True:
            # blocking get
            message = self.overseer_in_queue.get()
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.debug("Shutting down . . .")
                break
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_OVERSEER:
                self.active_overseer = True
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_WORKER:
                self.active_overseer = False
            else:
                if self.active_overseer:  # we are the active overseer!
                    if self.input_folder_contents is None:
                        # get input folder contents and broadcast it to standby overseers
                        self.broadcast_work_list()
                    if not self.work_queue_primed:
                        # prime the work queue and notify as we go
                        self.prime_work_queue()
                    # begin taking work requests
                    else:
                        if message[MESSAGE_TYPE] == WORK_REQUEST:
                            self.handle_work_request(message)
                        elif message[MESSAGE_TYPE] == WORK_RESULT:
                            self.handle_work_result_message(message)
                            # after all work is down notify coordinator to shutdown

                else:  # we are a standby overseer, track messages from the active overseer
                    logging.info("Stand-by overseer mode:  Tracking the active overseer . . .")
                    # get the input folder contents from the work_list_message
                    if message[MESSAGE_TYPE] == WORK_LIST:
                        self.input_folder_contents = message[WORK_LIST]
                        self.setup_input_folder_not_submitted()
                    # track as the work queue is primed
                    elif message[MESSAGE_TYPE] == PRIME_WORK_QUEUE:
                        self.input_folder_not_submitted.discard(message[WORK_ITEM_SUBMITTED])
                    # track work responses and work results in flight
