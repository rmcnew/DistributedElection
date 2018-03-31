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
        self.queue = self.sqs.create_queue(QueueName=queue_name, Attributes=MESSAGE_QUEUE_ATTRIBUTES)
        self.queue_arn =
        self.subscription = self.topic.subscribe(Protocol=SQS, Endpoint=self.topic_arn)

    def get_arn(self):
        return self.queue.attributes[QUEUE_ARN]

    @staticmethod
    def create_write_to_sns_policy(topicarn, queuearn):
        policy_document = """{{
          "Version":"2012-10-17",
          "Statement":[
            {{
              "Sid":"MyPolicy",
              "Effect":"Allow",
              "Principal" : {{"AWS" : "*"}},
              "Action":"SQS:SendMessage",
              "Resource": "{}",
              "Condition":{{
                "ArnEquals":{{
                  "aws:SourceArn": "{}"
                }}
            }}
            }}
          ]
        }}""".format(queuearn, topicarn)
        return policy_document

    def subscribe_to_topic(self, sns):
        policy = self.create_write_to_sns_policy(sns.get_topic_arn(), self.get_arn())

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

    def subscribe(self, sqs_arn):
        self.subscription = self.topic.subscribe(Protocol=SQS, Endpoint=sqs_arn)
        logging.debug("Subscribe result: {}".format(self.subscription))

    def publish(self, message):
        logging.debug("Publishing message: {}".format(message))
        result = self.topic.publish(Message=message)
        logging.debug("Publish result: {}".format(result))
        return result

    def unsubscribe(self):
        self.subscription.delete()
        self.subscription = None
