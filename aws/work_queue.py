import json
import logging

import boto3

from shared.constants import *


class WorkQueue:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage for work queue"""

    def __init__(self):
        self.sqs = boto3.resource(SQS, region_name='us-west-2')
        self.queue = self.sqs.get_queue_by_name(QueueName=LFDE_SQS_QUEUE)

    def send_message(self, msg):
        response = self.queue.send_message(MessageBody=msg)
        message_id = response[MESSAGE_ID]
        logging.debug("Sent message: {}".format(msg))
        return message_id

    def receive_message(self):
        messages = self.queue.receive_messages()
        if len(messages) > 0:
            message = json.loads(messages[0].body)
            message_id = messages[0].message_id
            receipt_handle = messages[0].receipt_handle
            message[MESSAGE_ID] = message_id
            message[RECEIPT_HANDLE] = receipt_handle
            logging.debug("Returning message: {}".format(message))
            return message
        else:
            return None

    def delete_message(self, message_id, receipt_handle):
        self.queue.delete_messages(
            Entries=[
                {
                    'Id': message_id,
                    'ReceiptHandle': receipt_handle
                },
            ]
        )
