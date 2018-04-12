import logging
import math
import os
import time
from pathlib import Path

from aws.s3 import SimpleStorageService
from aws.work_queue import WorkQueue
from messages import *
from overseer_primer import OverseerPrimer
from shared.shared import *


class Overseer:

    def __init__(self, my_id, overseer_in_queue, overseer_out_queue):
        self.my_id = my_id
        self.overseer_in_queue = overseer_in_queue
        self.overseer_out_queue = overseer_out_queue
        self.running = True
        self.work_queue = WorkQueue()
        self.s3 = SimpleStorageService()
        self.active_overseer = False
        self.input_folder_contents = None
        self.temp_dir = get_temp_dir()
        self.primer_threads = []

    def broadcast_work_list(self):
        logging.debug("Broadcasting work list to standby overseers")
        self.input_folder_contents = self.s3.list_input_folder_contents()
        self.overseer_out_queue.put(work_list_message(self.input_folder_contents))

    def prime_single_work_item(self, item):
        """item is a single S3 key"""
        logging.info("[ACTIVE OVERSEER] Preparing to enqueue {} in work_queue".format(item))
        temp_file = item.replace("/", "_")
        temp_path = "{}/{}".format(self.temp_dir, temp_file)
        self.s3.download_string_pair(item, temp_path)
        string_pair_file = open(temp_path, READ_ONLY)
        string_a = string_pair_file.readline()
        string_b = string_pair_file.readline()
        self.work_queue.send_message(work_item_message(item, string_a, string_b))
        self.overseer_out_queue.put(prime_work_queue_message(item))
        self.s3.move_from_input_to_primed(item)

    def prime_work_queue(self):
        """Prime the work queue using single thread"""
        logging.info("[ACTIVE OVERSEER] Priming the work queue")
        for item in self.input_folder_contents:
            self.prime_single_work_item(item)
        # announce when the work queue is primed
        logging.info("[ACTIVE OVERSEER] Work queue is primed.  Notifying workers to send requests")
        self.overseer_out_queue.put(work_queue_ready_message())

    def calculate_work_queue_primer_threads_to_use(self):
        pool_size = max((os.cpu_count() * WORK_QUEUE_PRIMER_FACTOR), MAX_WORK_QUEUE_PRIMER_THREADS)
        logging.info("Priming work queue with {} threads".format(pool_size))
        return pool_size

    def prime_work_queue_parallel(self):
        """Prime the work queue using parallel threads"""
        logging.info("[ACTIVE OVERSEER] Priming the work queue in parallel")
        threads_to_use = self.calculate_work_queue_primer_threads_to_use()
        last_index = len(self.input_folder_contents)
        partition_size = math.ceil(last_index / threads_to_use)
        index = 0
        while index < last_index:
            start_index = index
            end_index = min(index + partition_size, last_index)
            logging.info("Preparing primer thread for range: {} to {}".format(start_index, end_index))
            primer = OverseerPrimer(self.input_folder_contents[start_index:end_index],
                                    self.overseer_out_queue, self.temp_dir)
            self.primer_threads.append(primer)
            index = end_index
        # start the primer threads
        logging.info("Starting primer_threads . . .")
        for thread in self.primer_threads:
            thread.start()

    def handle_work_request(self, message):
        logging.info("[ACTIVE OVERSEER] Handling work request")
        requester_id = message[REQUESTER_ID]
        work_item_msg = self.work_queue.receive_message()
        if work_item_msg is not None:
            string_pair_id = work_item_msg[STRING_PAIR_ID]
            string_a = work_item_msg[STRING_A]
            string_b = work_item_msg[STRING_B]
            message_id = work_item_msg[MESSAGE_ID]
            receipt_handle = work_item_msg[RECEIPT_HANDLE]
            self.overseer_out_queue.put(work_response_message(requester_id, string_pair_id,
                                                              string_a, string_b,
                                                              message_id, receipt_handle))
        else:  # work_item_mgs is None
            # if the work queue is empty, notify workers individually to shutdown
            self.overseer_out_queue.put(individual_shutdown_message(requester_id))

    def upload_edit_distance(self, long_string_pair_id, edit_distance):
        string_pair_id = Path(long_string_pair_id).name
        logging.info("[ACTIVE OVERSEER] Writing to temp file")
        temp_result_filename = "{}/{}_result".format(self.temp_dir, string_pair_id)
        temp_result_file = open(temp_result_filename, WRITE_ONLY)
        temp_result_file.write("{}, {}".format(string_pair_id, edit_distance))
        temp_result_file.close()
        logging.info("[ACTIVE OVERSEER] Uploading temp file to S3")
        time.sleep(1)
        self.s3.upload_string_pair_result("{}_result".format(string_pair_id), temp_result_filename)
        logging.info("[ACTIVE OVERSEER] Removing temp file")
        os.remove(temp_result_filename)

    def handle_work_result_message(self, message):
        logging.info("[ACTIVE OVERSEER] Handling work result message")
        requester_id = message[REQUESTER_ID]
        string_pair_id = message[STRING_PAIR_ID]
        edit_distance = message[EDIT_DISTANCE]
        message_id = message[MESSAGE_ID]
        receipt_handle = message[RECEIPT_HANDLE]
        # upload the result to S3
        logging.info("[ACTIVE OVERSEER] Uploading edit_distance to S3")
        self.upload_edit_distance(string_pair_id, edit_distance)
        # send out result message
        logging.info("[ACTIVE OVERSEER] Sending work result received message to worker ID: {}".format(requester_id))
        self.overseer_out_queue.put(work_result_received_message(requester_id, string_pair_id))
        # delete the work queue message
        logging.info("[ACTIVE OVERSEER] Deleting work item from work queue")
        self.work_queue.delete_message(message_id, receipt_handle)

    def can_quit(self):
        return self.active_overseer and \
               self.work_queue.receive_message() is None and \
               len(self.s3.list_output_folder_contents()) == len(self.s3.list_primed_folder_contents())

    def priming_done(self):
        return len(self.s3.list_input_folder_contents()) == 0

    def handle_priming_done(self):
        logging.info("Waiting for primer threads . . .")
        for thread_done in self.primer_threads:
            thread_done.join()
        self.primer_threads = []
        # announce when the work queue is primed
        logging.info("[ACTIVE OVERSEER] Work queue is primed.  Notifying workers to send requests")
        self.overseer_out_queue.put(work_queue_ready_message())

    def do_active_overseer_tasks(self):
        if self.active_overseer and not self.priming_done() and len(self.primer_threads) == 0:
            logging.info("[ACTIVE OVERSEER] Doing active overseer tasks!")
            self.broadcast_work_list()
            # self.prime_work_queue()
            self.prime_work_queue_parallel()
        elif self.active_overseer and self.priming_done() and len(self.primer_threads) > 0:
            self.handle_priming_done()

    def run(self):
        while self.running:
            self.do_active_overseer_tasks()
            # blocking get
            message = self.overseer_in_queue.get()
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.info("Shutting down . . .")
                self.running = False
            elif message[MESSAGE_TYPE] == ELECTION_BEGIN or \
                    message[MESSAGE_TYPE] == ELECTION_ID_DECLARE or \
                    message[MESSAGE_TYPE] == ELECTION_COMPARE:
                self.active_overseer = False
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_OVERSEER:
                self.active_overseer = True
                logging.info("[OVERSEER] Becoming Active Overseer!")
                if self.priming_done():
                    self.overseer_out_queue.put(work_queue_ready_message())
            elif message[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_WORKER:
                self.active_overseer = False
            elif message[MESSAGE_TYPE] == INTERNAL_CAN_QUIT and self.can_quit():
                self.overseer_out_queue.put(shutdown_message())
            elif self.active_overseer and message[MESSAGE_TYPE] == WORK_REQUEST:
                self.handle_work_request(message)
            elif self.active_overseer and message[MESSAGE_TYPE] == WORK_RESULT:
                self.handle_work_result_message(message)
            else:  # ignore other message types
                pass
