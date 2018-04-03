import json

from shared.constants import *
from shared.shared import timestamp


def election_begin_message(my_id):
    message = {MESSAGE_TYPE: ELECTION_BEGIN,
               ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def election_compare_message(my_id, their_id):
    message = {MESSAGE_TYPE: ELECTION_COMPARE,
               WINNER_ID: my_id,
               LOSER_ID: their_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def election_end_message(my_id):
    message = {MESSAGE_TYPE: ELECTION_END,
               WINNER_ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def prime_work_queue_message(work_submitted_list):
    message = {MESSAGE_TYPE: PRIME_WORK_QUEUE,
               WORK_SUBMITTED_LIST: work_submitted_list,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_queue_ready_message():
    message = {MESSAGE_TYPE: WORK_QUEUE_READY,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_request_message(my_id):
    message = {MESSAGE_TYPE: WORK_REQUEST,
               REQUESTER_ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_response_message(requester_id, string_pair_id, string_a, string_b):
    message = {MESSAGE_TYPE: WORK_RESPONSE,
               REQUESTER_ID: requester_id,
               STRING_PAIR_ID: string_pair_id,
               STRING_A: string_a,
               STRING_B: string_b,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_result_message(my_id, string_pair_id, edit_distance):
    message = {MESSAGE_TYPE: WORK_RESULT,
               REQUESTER_ID: my_id,
               STRING_PAIR_ID: string_pair_id,
               EDIT_DISTANCE: edit_distance,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_result_received_message(requester_id, string_pair_id):
    message = {MESSAGE_TYPE: WORK_RESULT_RECEIVED,
               REQUESTER_ID: requester_id,
               STRING_PAIR_ID: string_pair_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)
