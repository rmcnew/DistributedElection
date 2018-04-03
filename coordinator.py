import signal
import time
from multiprocessing import Process, Queue
from os import getpid

from aws.message_queue import MessageQueue
from coordinator_modes import Mode
from edit_distance_worker import EditDistanceWorker
from messages import *
from shared.constants import *
from shared.shared import *


class Coordinator:

    def __init__(self):
        self.id = "{}{}".format(getpid(), get_ip_address().replace(DOT, EMPTY))
        self.work_in_queue = Queue()
        self.work_out_queue = Queue()
        self.worker = EditDistanceWorker(self.work_in_queue, self.work_out_queue)
        self.worker_process = Process(target=self.worker.run)
        self.worker_process.start()
        self.message_queue = MessageQueue(self.id)
        self.running = True
        self.mode = Mode.ELECTION
        self.work_submitted = set()
        self.work_queue_primed = False

    def process_edit_distance(self, string_pair_id, string_a, string_b):
        work_item = (string_pair_id, string_a, string_b)
        self.work_in_queue.put_nowait(work_item)

    def edit_distance_ready(self):
        return not self.work_out_queue.empty()

    def get_edit_distance_result(self):
        return self.work_out_queue.get_nowait()

    def compare_ids(self, their_id):
        return int(self.id) > int(their_id)

    def shutdown(self):
        logging.info("Shutting down edit distance worker subprocess . . .")
        # shutdown edit distance subprocess
        shutdown_item = (SHUTDOWN, "", "")
        self.work_in_queue.put_nowait(shutdown_item)
        time.sleep(2)
        self.worker_process.join()
        time.sleep(2)
        # shutdown message queue
        self.message_queue.shutdown()

    def conduct_election(self, ext_message=None):
        logging.info("Conducting election:  My ID is {} . . .".format(self.id))
        election_over = False
        self.message_queue.send_message(election_begin_message(self.id))
        no_message_count = 0
        election_winner = True
        while not election_over:
            if ext_message is not None:
                message = ext_message
            else:
                message = self.message_queue.receive_message()
            if message is not None:
                logging.debug("Received message: {}".format(message))
                if message[MESSAGE_TYPE] == ELECTION_BEGIN:
                    logging.debug("Message type is ELECTION_BEGIN")
                    their_id = message[ID]
                    if int(their_id) == int(self.id):
                        continue  # ignore the message I sent
                    elif self.compare_ids(their_id):
                        logging.info("My ID={} beats THEIR_ID={}".format(self.id, their_id))
                        self.message_queue.send_message(election_compare_message(self.id, their_id))
                    else:
                        logging.info("My ID={} loses to THEIR_ID={}".format(self.id, their_id))
                        election_winner = False
                elif message[MESSAGE_TYPE] == ELECTION_COMPARE:
                    logging.debug("Message type is ELECTION_COMPARE")
                    logging.info("{}".format(message))
                elif message[MESSAGE_TYPE] == ELECTION_END:
                    logging.debug("Message type is ELECTION_END")
                    logging.info("Election is over.  New Overseer is: {}".format(message[WINNER_ID]))
                    election_over = True
                    if election_winner:
                        logging.debug("Changing to OVERSEER mode!")
                        self.mode = Mode.OVERSEER
                    else:
                        logging.debug("Changing to WORKER mode!")
                        self.mode = Mode.WORKER
                else:
                    logging.debug("Unknown message type: {}".format(message[MESSAGE_TYPE]))
            else:
                logging.debug("No message received.")
                no_message_count = no_message_count + 1
                if election_winner and no_message_count > ELECTION_WINNER_WAIT_CYCLES:
                    self.message_queue.send_message(election_end_message(self.id))

    def do_work(self):
        logging.info("Doing edit distance work . . .")
        # wait until work queue is primed
        while not self.work_queue_primed:
            message = self.message_queue.receive_message()
            if message is not None:
                logging.info("Message received")
            else:
                logging.debug("No message received.")
                time.sleep(2)

    def oversee_work(self):
        logging.info("Overseeing work . . .")
        # prime work queue


    def run(self):
        while self.running:
            if self.mode is Mode.ELECTION:
                self.conduct_election()
            elif self.mode is Mode.WORKER:
                self.do_work()
            elif self.mode is Mode.OVERSEER:
                self.oversee_work()
            elif self.mode is Mode.SHUTDOWN:
                self.shutdown()
            else:
                logging.error("Unknown mode: {}!  Shutting down . . .".format(self.mode.name)) 
                self.mode = Mode.SHUTDOWN


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
