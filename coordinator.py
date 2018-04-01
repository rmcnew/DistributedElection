import logging
from multiprocessing import Process, Queue
from os import getpid

from edit_distance_worker import EditDistanceWorker
from shared.constants import *
from shared.shared import get_ip_address


class Coordinator:

    def __init__(self):
        self.id = "{}{}".format(getpid(), get_ip_address().replace(DOT, EMPTY))
        self.work_in_queue = Queue()
        self.work_out_queue = Queue()
        self.worker = EditDistanceWorker(self.work_in_queue, self.work_out_queue)
        self.worker_process = Process(target=self.worker.run)
        self.worker_process.start()

    def process_edit_distance(self, string_pair_id, string_a, string_b):
        work_item = (string_pair_id, string_a, string_b)
        self.work_in_queue.put_nowait(work_item)

    def edit_distance_ready(self):
        return not self.work_out_queue.empty()

    def get_edit_distance_result(self):
        return self.work_out_queue.get_nowait()

    def compare_ids(self, their_id):
        return int(self.id) > int(their_id)


def main():
    log_file = COORDINATOR_LOG
    logging.basicConfig(format='%(message)s',
                        filename=log_file,
                        level=logging.DEBUG)


if __name__ == "__main__":
    main()
