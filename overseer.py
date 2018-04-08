import logging

from aws.s3 import SimpleStorageService
from aws.work_queue import WorkQueue
from messages import *
from shared.concurrent_set import ConcurrentSet
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
        self.input_folder_not_submitted = ConcurrentSet()
        self.work_queue_primed = False
        self.temp_dir = get_temp_dir()

    def setup_input_folder_not_submitted(self):
        logging.debug("Setting up input_folder_not_submitted")
        for key in self.input_folder_contents:
            self.input_folder_not_submitted.insert(key)

    def broadcast_work_list(self):
        logging.debug("Broadcasting work list to standby overseers")
        self.input_folder_contents = self.s3.list_input_folder_contents()
        self.overseer_out_queue.put(work_list_message(self.input_folder_contents))
        self.setup_input_folder_not_submitted()

    def calculate_work_queue_primer_pool_size(self):
        # pool_size = max( (os.cpu_count() * WORK_QUEUE_PRIMER_FACTOR), MAX_WORK_QUEUE_PRIMER_THREADS)
        pool_size = 2
        logging.info("Priming work queue with {} threads".format(pool_size))
        return pool_size

    def prime_single_work_item(self, item):
        """item is a single S3 key"""
        logging.info("[ACTIVE OVERSEER] Preparing to enqueue {} in work_queue".format(item))
        # create local AWS objects for each thread
        # s3 = SimpleStorageService()
        # work_queue = WorkQueue()
        temp_file = item.replace("/", "_")
        temp_path = "{}/{}".format(self.temp_dir, temp_file)
        self.s3.download_string_pair(item, temp_path)
        string_pair_file = open(temp_path, READ_ONLY)
        string_a = string_pair_file.readline()
        string_b = string_pair_file.readline()
        self.work_queue.send_message(work_item_message(item, string_a, string_b))
        self.overseer_out_queue.put(prime_work_queue_message(item))
        self.input_folder_not_submitted.remove(item)

    def prime_work_queue(self):
        """Prime the work queue using parallel threads"""
        logging.info("[ACTIVE OVERSEER] Priming the work queue")
        # with multiprocessing.Pool(self.calculate_work_queue_primer_pool_size()) as pool:
        #    pool.map(self.prime_single_work_item, self.input_folder_contents)
        for item in self.input_folder_not_submitted.get_items():
            self.prime_single_work_item(item)
        # announce when the work queue is primed
        logging.info("[ACTIVE OVERSEER] Work queue is primed.  Notifying workers to send requests")
        self.overseer_out_queue.put(work_queue_ready_message())
        self.work_queue_primed = True

    def handle_work_request(self, message):
        logging.info("[ACTIVE OVERSEER] Handling work request")
        requester_id = message[REQUESTER_ID]
        work_item_msg = self.work_queue.receive_message()
        if work_item_msg is not None:
            string_pair_id = work_item_msg[STRING_PAIR_ID]
            string_a = work_item_msg[STRING_A]
            string_b = work_item_msg[STRING_B]
            receipt_handle = work_item_msg[RECEIPT_HANDLE]
            self.overseer_out_queue.put(work_response_message(requester_id, string_pair_id,
                                                              string_a, string_b, receipt_handle))
        else:  # work_item_mgs is None
            # if the work queue is empty, notify workers individually to shutdown
            self.overseer_out_queue.put(individual_shutdown_message(requester_id))

    def upload_edit_distance(self, string_pair_id, edit_distance):
        logging.info("[ACTIVE OVERSEER] Uploading edit_distance to S3")
        temp_result_filename = "{}/{}_result".format(self.temp_dir, string_pair_id)
        temp_result_file = open(temp_result_filename, WRITE_ONLY)
        temp_result_file.write("{}, {}".format(string_pair_id, edit_distance))
        temp_result_file.close()
        self.s3.upload_string_pair_result("{}_result".format(string_pair_id), temp_result_filename)

    def handle_work_result_message(self, message):
        logging.info("[ACTIVE OVERSEER] Handling work result message")
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

    def do_active_overseer_tasks(self):
        if self.active_overseer:  # we are the active overseer!
            if self.input_folder_contents is None:
                # get input folder contents and broadcast it to standby overseers
                self.broadcast_work_list()
            if not self.work_queue_primed:
                # prime the work queue and notify as we go
                self.prime_work_queue()

    def run(self):
        while True:
            self.do_active_overseer_tasks()
            # blocking get
            message = self.overseer_in_queue.get()
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.info("Shutting down . . .")
                break
            elif message[MESSAGE_TYPE] == ELECTION_BEGIN or message[MESSAGE_TYPE] == ELECTION_ID_DECLARE or \
                    message[MESSAGE_TYPE] == ELECTION_COMPARE or message[MESSAGE_TYPE] == ELECTION_END or \
                    message[MESSAGE_TYPE] == NULL_MESSAGE:
                pass  # ignore election messages and null messages
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_OVERSEER:
                self.active_overseer = True
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_WORKER:
                self.active_overseer = False
            else:  # begin taking work requests
                if self.active_overseer:
                    if message[MESSAGE_TYPE] == WORK_REQUEST:
                        self.handle_work_request(message)
                    elif message[MESSAGE_TYPE] == WORK_RESULT:
                        self.handle_work_result_message(message)
                    else:  # ignore other message types
                        pass
                else:  # we are a standby overseer, track messages from the active overseer
                    # get the input folder contents from the work_list_message
                    if message[MESSAGE_TYPE] == WORK_LIST:
                        self.input_folder_contents = message[WORK_LIST]
                        self.setup_input_folder_not_submitted()
                        logging.info("[STANDBY OVERSEER]  Finished setup of my input_folder_not_submitted")
                    # track as the work queue is primed
                    elif message[MESSAGE_TYPE] == PRIME_WORK_QUEUE:
                        work_item_submitted = message[WORK_ITEM_SUBMITTED]
                        self.input_folder_not_submitted.remove(work_item_submitted)
                        logging.info("[STANDBY OVERSEER]  Removed {} from my "
                                     "input_folder_not_submitted ".format(work_item_submitted))
                    elif message[MESSAGE_TYPE] == WORK_QUEUE_READY:
                        self.work_queue_primed = True
                        logging.info("[STANDBY OVERSEER]  Tracking work_queue_primed is TRUE.")
                    else:  # ignore other message types
                        pass
