import logging

from messages import *


class Elector:

    def __init__(self, my_id, election_in_queue, election_out_queue):
        self.my_id = my_id
        self.election_in_queue = election_in_queue
        self.election_out_queue = election_out_queue
        self.election_over = False
        self.election_winner = None
        self.null_message_count = 0

    def compare_ids(self, their_id):
        return int(self.my_id) > int(their_id)

    def update_election_winner(self, result):
        if self.election_winner is None:
            self.election_winner = result
        else:
            self.election_winner = self.election_winner and result

    def conduct_election(self):
        # set tracking variables
        self.election_over = False
        self.null_message_count = 0
        self.election_winner = None
        # send out my_id in a election_id_declare_message
        self.election_out_queue.put(election_id_declare_message(self.my_id))
        # then wait for responses from other processes
        while not self.election_over:
            # blocking get
            message = self.election_in_queue.get()
            logging.debug("Received message: {}".format(message))
            # compare my_id against another process's id
            if message[MESSAGE_TYPE] == ELECTION_ID_DECLARE:
                logging.debug("Message type is ELECTION_ID_DECLARE")
                their_id = message[ID]
                if int(their_id) == int(self.my_id):
                    continue  # ignore the message I sent
                elif self.compare_ids(their_id):
                    logging.info("My ID={} beats THEIR_ID={}".format(self.my_id, their_id))
                    self.election_out_queue.put(election_compare_message(self.my_id, their_id))
                    self.update_election_winner(I_WIN)
                    # self.election_out_queue.put(election_id_declare_message(self.my_id))  # declare again
                else:
                    logging.info("My ID={} loses to THEIR_ID={}".format(self.my_id, their_id))
                    self.update_election_winner(THEY_WIN)
            elif message[MESSAGE_TYPE] == ELECTION_COMPARE:
                logging.debug("Message type is ELECTION_COMPARE")
                logging.info("{}".format(message))
            elif message[MESSAGE_TYPE] == ELECTION_END:
                logging.debug("Message type is ELECTION_END")
                logging.info("Election is over.  New Overseer is: {}".format(message[WINNER_ID]))
                self.election_over = True
                self.null_message_count = 0  # reset to enable detection of missing active overseer
                if self.election_winner and (message[WINNER_ID]) == int(self.my_id):
                    logging.info("Election won!  Changing to OVERSEER mode!")
                    self.election_out_queue.put(internal_mode_switch_to_overseer_message())
                else:
                    logging.info("Election lost.  Changing to WORKER mode!")
                    self.election_out_queue.put(internal_mode_switch_to_worker_message())
            elif message[MESSAGE_TYPE] == NULL_MESSAGE:
                logging.debug("Null message received.")
                # time.sleep(0.5)  # wait to make sure no more messages are received
                self.null_message_count = self.null_message_count + 1
                if self.election_winner and self.null_message_count > ELECTION_WINNER_WAIT_CYCLES:
                    self.election_out_queue.put(election_end_message(self.my_id))
            else:  # ignore other message types
                pass

    def run(self):
        # on initial start-up, kick off an election
        logging.info("Conducting election:  My ID is {} . . .".format(self.my_id))
        self.election_out_queue.put(election_begin_message())
        # otherwise, wait for messages announcing a new election
        while True:
            # blocking message get
            message = self.election_in_queue.get()
            # respond to shutdown request
            if message[MESSAGE_TYPE] == SHUTDOWN:
                logging.debug("Shutting down . . .")
                break
            # if we get too many null messages, call for an election
            elif message[MESSAGE_TYPE] == NULL_MESSAGE:
                self.null_message_count = self.null_message_count + 1
                if self.null_message_count >= ACTIVE_OVERSEER_MIA_NULL_MESSAGE_LIMIT and not self.election_winner:
                    logging.info("No response from active Overseer.  "
                                 "Conducting election:  My ID is {} . . .".format(self.my_id))
                    self.election_out_queue.put(election_begin_message())
                elif self.null_message_count >= ACTIVE_OVERSEER_MIA_NULL_MESSAGE_LIMIT and self.election_winner:
                    logging.info("No activity.  Asking Overseer process if quit criteria reached")
                    self.election_out_queue.put(internal_can_quit_message())
            # if we get an ELECTION_BEGIN message, conduct an election
            elif message[MESSAGE_TYPE] == ELECTION_BEGIN:
                self.conduct_election()
            # as long as we see messages from the active overseer, reset the null message counter
            elif message[MESSAGE_TYPE] == WORK_LIST or message[MESSAGE_TYPE] == PRIME_WORK_QUEUE or \
                    message[MESSAGE_TYPE] == WORK_QUEUE_READY or message[MESSAGE_TYPE] == WORK_RESPONSE or \
                    message[MESSAGE_TYPE] == WORK_RESULT_RECEIVED:
                self.null_message_count = 0
            else:  # ignore other message types
                pass
