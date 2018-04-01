import json
import logging
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

