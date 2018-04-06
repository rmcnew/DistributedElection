import json

from shared.constants import *
from shared.shared import timestamp


def null_message():
    """Empty message that represents no message received during the previous message poll period"""
    message = {MESSAGE_TYPE: NULL_MESSAGE}
    return json.dumps(message)


def election_begin_message():
    """Message that indicates an election is starting"""
    message = {MESSAGE_TYPE: ELECTION_BEGIN,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def election_id_declare_message(my_id):
    """Message that gives process ID for comparison"""
    message = {MESSAGE_TYPE: ELECTION_ID_DECLARE,
               ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def election_compare_message(my_id, their_id):
    """Message that indicates a process won an ID comparison against another process"""
    message = {MESSAGE_TYPE: ELECTION_COMPARE,
               WINNER_ID: my_id,
               LOSER_ID: their_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def election_end_message(my_id):
    """Message that indicates a process has defeated all challengers and
       not received additional challenges before the timeout"""
    message = {MESSAGE_TYPE: ELECTION_END,
               WINNER_ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_list_message(work_list):
    """Message that gives the list of all work that will be put on the work queue"""
    message = {MESSAGE_TYPE: WORK_LIST,
               WORK_LIST: work_list,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def prime_work_queue_message(work_item_submitted):
    """Message that gives work items submitted to the work queue by the overseer"""
    message = {MESSAGE_TYPE: PRIME_WORK_QUEUE,
               WORK_ITEM_SUBMITTED: work_item_submitted,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_queue_ready_message():
    """Message that indicates that the work queue is ready and workers can begin asking for work"""
    message = {MESSAGE_TYPE: WORK_QUEUE_READY,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_request_message(my_id):
    """Message sent by a worker to the overseer to request work"""
    message = {MESSAGE_TYPE: WORK_REQUEST,
               REQUESTER_ID: my_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_item_message(string_pair_id, string_a, string_b):
    """Message placed in work_queue"""
    message = {MESSAGE_TYPE: WORK_ITEM,
               STRING_PAIR_ID: string_pair_id,
               STRING_A: string_a,
               STRING_B: string_b}
    return json.dumps(message)


def work_response_message(requester_id, string_pair_id, string_a, string_b, receipt_handle):
    """Message sent by the overseer to a worker to provide a work item"""
    message = {MESSAGE_TYPE: WORK_RESPONSE,
               REQUESTER_ID: requester_id,
               STRING_PAIR_ID: string_pair_id,
               STRING_A: string_a,
               STRING_B: string_b,
               RECEIPT_HANDLE: receipt_handle,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_result_message(my_id, string_pair_id, edit_distance, receipt_handle):
    """Message sent by a worker to the overseer to give a work item result"""
    message = {MESSAGE_TYPE: WORK_RESULT,
               REQUESTER_ID: my_id,
               STRING_PAIR_ID: string_pair_id,
               EDIT_DISTANCE: edit_distance,
               RECEIPT_HANDLE: receipt_handle,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def work_result_received_message(requester_id, string_pair_id):
    """Message sent by the overseer to a worker indicating that a work item result was received and recorded"""
    message = {MESSAGE_TYPE: WORK_RESULT_RECEIVED,
               REQUESTER_ID: requester_id,
               STRING_PAIR_ID: string_pair_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def individual_shutdown_message(to_id):
    """Message that directs ONLY the receiver to cleanly shutdown"""
    message = {MESSAGE_TYPE: SHUTDOWN,
               TO_ID: to_id,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def shutdown_message():
    """Message that directs the receiver to cleanly shutdown"""
    message = {MESSAGE_TYPE: SHUTDOWN,
               TIMESTAMP: timestamp()}
    return json.dumps(message)


def internal_mode_switch_to_overseer_message():
    """Internal message directing coordinator mode switch to overseer"""
    message = {MESSAGE_TYPE: INTERNAL_MODE_SWITCH_TO_OVERSEER}
    return json.dumps(message)


def internal_mode_switch_to_worker_message():
    """Internal message directing coordinator mode switch to edit distance worker"""
    message = {MESSAGE_TYPE: INTERNAL_MODE_SWITCH_TO_WORKER}
    return json.dumps(message)
