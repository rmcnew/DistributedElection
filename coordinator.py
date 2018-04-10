import logging
import signal
import time
from multiprocessing import Process, Queue
from os import getpid

from aws.message_queue import MessageQueue
from edit_distance_worker import EditDistanceWorker
from elector import Elector
from messages import *
from overseer import Overseer
from shared.constants import *
from shared.shared import *


class Coordinator:

    def __init__(self):
        self.my_id = "{}{}".format(getpid(), get_ip_address().replace(DOT, EMPTY))
        self.in_shutdown = False
        # setup Election subprocess
        self.election_in_queue = Queue()
        self.election_out_queue = Queue()
        self.elector = Elector(self.my_id, self.election_in_queue, self.election_out_queue)
        self.elector_process = Process(target=self.elector.run)
        # setup Overseer subprocess
        self.overseer_in_queue = Queue()
        self.overseer_out_queue = Queue()
        self.overseer = Overseer(self.my_id, self.overseer_in_queue, self.overseer_out_queue)
        self.overseer_process = Process(target=self.overseer.run)
        # setup EditDistanceWorker subprocess
        self.worker_in_queue = Queue()
        self.worker_out_queue = Queue()
        self.worker = EditDistanceWorker(self.my_id, self.worker_in_queue, self.worker_out_queue)
        self.worker_process = Process(target=self.worker.run)
        # setup MessageQueue
        self.message_queue = MessageQueue(self.my_id)
        self.running = True
        # start subprocesses
        self.elector_process.start()
        self.overseer_process.start()
        self.worker_process.start()

    def shutdown(self):
        if not self.in_shutdown:  # prevent multiple calls
            logging.info("Received SHUTDOWN directive!")
            self.running = False
            self.in_shutdown = True
            logging.info("Shutting down subprocesses . . .")
            # send shutdown message to subprocesses
            shutdown_msg = internal_shutdown_message()
            self.worker_in_queue.put(shutdown_msg)
            self.overseer_in_queue.put(shutdown_msg)
            self.election_in_queue.put(shutdown_msg)
            time.sleep(2)
            logging.info("Shutting joining subprocesses . . .")
            # join subprocesses
            self.worker_process.join()
            self.overseer_process.join()
            self.elector_process.join()
            time.sleep(2)
            logging.info("Unsubscribing and deleting message queue . . .")
            # shutdown message queue
            self.message_queue.shutdown()
            logging.info("Exiting . . .")

    def send_outbound_messages(self, out_queue):
        while self.running and not out_queue.empty():
            message = out_queue.get()
            message_obj = json.loads(message)
            if message_obj[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_WORKER or \
                    message_obj[MESSAGE_TYPE] == INTERNAL_MODE_SWITCH_TO_OVERSEER or \
                    message_obj[MESSAGE_TYPE] == INTERNAL_CAN_QUIT:
                self.election_in_queue.put(message_obj)
                self.overseer_in_queue.put(message_obj)
                self.worker_in_queue.put(message_obj)
            elif message_obj[MESSAGE_TYPE] == SHUTDOWN:
                self.shutdown()
            else:
                self.message_queue.send_message(message)

    def get_and_route_messages(self):
        # get message from message queue;
        # route received messages to all subprocess in_queues; let subprocesses filter messages of interest
        if self.running:
            message = self.message_queue.receive_message()
            logging.debug("message is {}".format(message))
            if message is None:  # if no message received within timeout, send NULL_MESSAGE
                message = null_message()
            if message[MESSAGE_TYPE] == INDIVIDUAL_SHUTDOWN and message[TO_ID] == self.my_id:
                self.shutdown()
            elif message[MESSAGE_TYPE] == SHUTDOWN:
                self.shutdown()
            else:
                self.election_in_queue.put(message)
                self.overseer_in_queue.put(message)
                self.worker_in_queue.put(message)

    def run(self):
        while self.running:
            # check subprocess out queues and send messages as appropriate
            logging.debug("Sending election_out_queue messages")
            self.send_outbound_messages(self.election_out_queue)
            logging.debug("Sending overseer_out_queue messages")
            self.send_outbound_messages(self.overseer_out_queue)
            logging.debug("Sending work_out_queue messages")
            self.send_outbound_messages(self.worker_out_queue)
            logging.debug("Getting messages and routing to subprocesses")
            self.get_and_route_messages()


def main():
    log_file = "{}-{}.log".format(COORDINATOR, getpid())
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d : %(message)s',
                        # filename=log_file,
                        level=logging.INFO)
    coordinator = Coordinator()

    def ctrl_c_handler(signum, frame):
        coordinator.shutdown()

    signal.signal(signal.SIGINT, ctrl_c_handler)
    coordinator.run()


if __name__ == "__main__":
    main()
