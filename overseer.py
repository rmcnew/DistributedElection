import logging

from aws.s3 import SimpleStorageService
from aws.work_queue import WorkQueue
from messages import *


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

    def setup_input_folder_not_submitted(self):
        self.input_folder_not_submitted = set()
        for key in self.input_folder_contents:
            self.input_folder_not_submitted.add(key)

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
                        self.input_folder_contents = self.s3.list_input_folder_contents()
                        self.overseer_out_queue.put(work_list_message(self.input_folder_contents))
                        self.setup_input_folder_not_submitted()

                    # prime the work queue and notify as we go

                    # announce when the work queue is primed

                    # begin taking work requests

                    # if the work queue is empty and there are no requests out, notify workers individually to shutdown

                    # after all work is down notify coordinator to shutdown

                else:  # we are a standby overseer, track messages from the active overseer
                    # get the input folder contents from the work_list_message
                    if message[MESSAGE_TYPE] == WORK_LIST:
                        self.input_folder_contents = message[WORK_LIST]
                        self.setup_input_folder_not_submitted()
                    # track as the work queue is primed

                    # track work responses and work results in flight
