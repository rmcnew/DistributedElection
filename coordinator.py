import logging
import datetime
from multiprocessing import Process, Queue
from os import getpid

from edit_distance_worker import EditDistanceWorker
from messages import *
from shared.constants import *
from shared.shared import *
from aws.message_queue import MessageQueue
from coordinator_modes import Mode


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
        logging.info("Shutting down . . .")
        shutdown_item = (SHUTDOWN, "", "")
        self.work_in_queue.put_nowait(shutdown_item)
        self.worker_process.join()
        self.message_queue.shutdown()

    def conduct_election(self):
        logging.info("Conducting election . . .")
        election_over = False
        self.message_queue.send_message(election_begin_message(self.id))
        no_message_count = 0
        election_winner = True
        while not election_over:
            message = self.message_queue.receive_message()
            if message != None:
                logging.debug("Received message: {}".format(message))
                if message[MESSAGE_TYPE] == ELECTION_BEGIN:
                    logging.debug("Message type is ELECTION_BEGIN")
                    their_id = message[ID]
                    if compare_ids(their_id):
                        logging.debug("My ID={} beats THEIR_ID={}".format(self.id, their_id))
                        self.message_queue.send_message(election_compare_message(self.id, their_id))
                    else:
                        logging.debug("My ID={} loses to THEIR_ID={}".format(self.id, their_id))
                        election_winner = False
                elif message[MESSAGE_TYPE] == ELECTION_COMPARE:
                    logging.debug("Message type is ELECTION_COMPARE")
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

    def oversee_work(self):
        logging.info("Overseeing work . . .")

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
    log_file = COORDINATOR_LOG
    logging.basicConfig(format='%(message)s',
                        filename=log_file,
                        level=logging.DEBUG)
    coodinator = Coordinator()
    coodinator.run()


if __name__ == "__main__":
    main()
