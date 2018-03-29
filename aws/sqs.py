import logging

import boto3

from shared.constants import *


class SimpleQueueService:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage"""

    def __init__(self):
        self.sqs = boto3.resource(SQS)
        self.queue = self.sqs.get_queue_by_name(QueueName=LFDE_SQS_QUEUE)

    def send_message(self, msg):
        response = self.queue.send_message(MessageBody=msg)
        message_id = response[MESSAGE_ID]
        logging.debug("Sent message: \'{}\' with message_id: {}".format(msg, message_id))
        return message_id

    def receive_message(self):
        message_list = self.queue.receive_messages()
        message = message_list[0]
        body = message.body
        message_id = message.message_id
        logging.debug("Received message: \'{}\' with message_id: {}".format(body, message_id))
        message.delete()
        return body
