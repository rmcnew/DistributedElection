import logging
import signal
import time
from multiprocessing import Process, Queue
from os import getpid

from aws.message_queue import MessageQueue
from coordinator_modes import Mode
from edit_distance_worker import EditDistanceWorker
from elector import Elector
from messages import *
from overseer import Overseer
from shared.constants import *
from shared.shared import *


class Coordinator:

    def __init__(self):
        self.my_id = "{}{}".format(getpid(), get_ip_address().replace(DOT, EMPTY))
        # setup Election subprocess
        self.election_in_queue = Queue()
        self.election_out_queue = Queue()
        self.elector = Elector(self.election_in_queue, self.election_out_queue)
        self.elector_process = Process(target=self.elector.run)
        # setup Overseer subprocess
        self.overseer_in_queue = Queue()
        self.overseer_out_queu = Queue()
        self.overseer = Overseer()
        self.overseer_process = Process(target=self.overseer.run)
        # setup EditDistanceWorker subprocess
        self.work_in_queue = Queue()
        self.work_out_queue = Queue()
        self.worker = EditDistanceWorker(self.work_in_queue, self.work_out_queue)
        self.worker_process = Process(target=self.worker.run)
        # setup MessageQueue
        self.message_queue = MessageQueue(self.my_id)
        self.running = True
        self.mode = Mode.ELECTION
        # start subprocesses
        self.elector_process.start()
        self.overseer_process.start()
        self.worker_process.start()

    def shutdown(self):
        logging.info("Shutting down edit distance worker subprocess . . .")
        # send shutdown message to subprocesses
        shutdown_msg = shutdown_message()
        self.work_in_queue.put_nowait(shutdown_msg)
        self.overseer_in_queue.put_nowait(shutdown_msg)
        self.election_in_queue.put_nowait(shutdown_msg)
        time.sleep(2)
        # join subprocesses
        self.worker_process.join()
        self.overseer_process.join()
        self.elector_process.join()
        time.sleep(2)
        # shutdown message queue
        self.message_queue.shutdown()

    def run(self):
        while self.running:
            # check subprocess out queues and send messages as appropriate

            # get message from message queue; if no message received within timeout, send NULL_MESSAGE
            message = self.message_queue.receive_message()
            if message is None:
                message = null_message()
            # route received messages to appropriate subprocess for handling



def main():
    def ctrl_c_handler(signum, frame):
        coordinator.shutdown()

    signal.signal(signal.SIGINT, ctrl_c_handler)
    log_file = COORDINATOR_LOG
    logging.basicConfig(format='%(message)s',
                        # filename=log_file,
                        level=logging.INFO)
    coordinator = Coordinator()
    coordinator.run()


if __name__ == "__main__":
    main()
