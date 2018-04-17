import logging
from queue import Empty
from threading import Thread

from aws.s3 import SimpleStorageService
from aws.work_queue import WorkQueue
from messages import *


class OverseerPrimer(Thread):

    def __init__(self, item_list, overseer_out_queue, temp_dir, primer_threads_queue):
        super().__init__(daemon=True)  # do not block if the parent Ctrl-C interrupt is triggered
        self.item_list = item_list
        self.overseer_out_queue = overseer_out_queue
        self.temp_dir = temp_dir
        self.primer_threads_queue = primer_threads_queue

    def run(self):
        # create local AWS objects for each thread
        s3 = SimpleStorageService()
        work_queue = WorkQueue()
        counter = 0
        for item in self.item_list:
            try:
                message = self.primer_threads_queue.get_nowait()
                if message[MESSAGE_TYPE] == ABORT_PRIMING:
                    logging.info("[ACTIVE OVERSEER PRIMER] Received abort priming message.  Aborting . . .")
                    break
            except Empty:
                pass
            logging.info("[ACTIVE OVERSEER PRIMER] Preparing to enqueue {} in work_queue".format(item))
            temp_file = item.replace("/", "_")
            temp_path = "{}/{}".format(self.temp_dir, temp_file)
            if s3.download_string_pair(item, temp_path):
                string_pair_file = open(temp_path, READ_ONLY)
                string_a = string_pair_file.readline()
                string_b = string_pair_file.readline()
                work_queue.send_message(work_item_message(item, string_a, string_b))
                s3.move_from_input_to_primed(item)
                counter = counter + 1
                if counter % 7 == 0:  # only notify every N messages
                    self.overseer_out_queue.put(prime_work_queue_message(item))
