import logging

import boto3

from shared.constants import *


class MessageQueue:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage for message queue"""

    def __init__(self, queue_name):
        self.sqs = boto3.resource(SQS)
        self.queue = self.sqs.create_queue(QueueName=queue_name, MESSAGE_QUEUE_ATTRIBUTES)

    def get_arn(self):
        return self.queue.attributes[QUEUE_ARN]

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

