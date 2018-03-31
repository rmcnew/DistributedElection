import logging

import boto3

from shared.constants import *


class MessageQueue:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage for use as message queue via SNS"""

    def __init__(self, queue_name):
        self.sns = boto3.resource(SNS)
        self.topic = self.sns.create_topic(Name=LFDE_SNS_TOPIC)
        self.topic_arn = self.topic.arn
        self.sqs = boto3.resource(SQS)
        self.queue_attributes = MESSAGE_QUEUE_ATTRIBUTES
        self.queue_attributes[POLICY] = create_write_from_sns_policy(self.topic_arn)
        self.queue = self.sqs.create_queue(QueueName=queue_name, Attributes=self.queue_attributes)
        self.subscription = self.topic.subscribe(Protocol=SQS, Endpoint=self.topic_arn)

    def __del__(self):
        self.subscription.delete()

    def get_arn(self):
        return self.queue.attributes[QUEUE_ARN]

    @staticmethod
    def create_write_from_sns_policy(topicarn):
        policy_document = """{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Sid":"MyPolicy",
              "Effect":"Allow",
              "Principal" : {{"AWS" : "*"}},
              "Action":"SQS:SendMessage",
              "Resource": "{}",
            }}
          ]
        }}""".format(topicarn)
        return policy_document

    def send_message(self, msg):
        logging.debug("Publishing message: {}".format(message))
        result = self.topic.publish(Message=message)
        logging.debug("Publish result: {}".format(result))
        return result

    def receive_message(self):
        message_list = self.queue.receive_messages()
        if len(message_list) > 0:
            message = message_list[0]
            body = message.body
            message_id = message.message_id
            logging.debug("Received message: \'{}\' with message_id: {}".format(body, message_id))
            message.delete()
            return body

