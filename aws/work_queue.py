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
        logging.debug("Sent message: \'{}\' with message_id: {}".format(msg, message_id))
        return message_id

    def receive_message(self):
        message_list = self.queue.receive_messages()
        if len(message_list) > 0:
            message = message_list[0]
            body = message.body
            message_id = message.message_id
            logging.debug("Received message: \'{}\' with message_id: {}".format(body, message_id))
            message.delete()
            return body
        else:
            return None

    def delete_message(self, receipt_handle):
        self.sqs_client.delete_message(QueueUrl=self.queue.url, ReceiptHandle=receipt_handle)
