import json
import logging

import boto3

from shared.constants import *


class WorkQueue:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage for work queue"""

    def __init__(self):
        self.sqs = boto3.resource(SQS)
        self.sqs_client = boto3.client(SQS)
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
            receipt_handle = message[0].receipt_handle
            body = message[MESSAGE]
            body_dict = json.loads(body)
            body_dict[MESSAGE_ID] = message_id
            body_dict[RECEIPT_HANDLE] = receipt_handle
            logging.debug("Returning message: {}".format(body))
            messages[0].delete()
            return body_dict
        else:
            return None

    def delete_message(self, receipt_handle):
        self.sqs_client.delete_message(QueueUrl=self.queue.url, ReceiptHandle=receipt_handle)
