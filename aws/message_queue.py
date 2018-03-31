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
        self.sqs_client = boto3.client(SQS)
        self.queue = self.sqs.create_queue(QueueName=queue_name)
        queue_attributes = MESSAGE_QUEUE_ATTRIBUTES
        queue_attributes[POLICY] = MessageQueue.create_write_from_sns_policy(self.queue.attributes[QUEUE_ARN])
        self.sqs_client.set_queue_attributes(QueueUrl=self.queue.url, Attributes=queue_attributes)
        self.queue.Attributes = queue_attributes
        self.subscription = self.topic.subscribe(Protocol=SQS, Endpoint=self.topic_arn)

    def __del__(self):
        self.subscription.delete()

    def get_arn(self):
        return self.queue.attributes[QUEUE_ARN]

    @staticmethod
    def create_write_from_sns_policy(queue_arn):
        policy_document = """{
  "Id": "Policy1522525396010",
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "Stmt1522525389948",
      "Action": [
        "sqs:SendMessage"
      ],
      "Effect": "Allow",
      "Resource": {}
      "Principal": {
        "AWS": [
          "arn:aws:sns:us-west-2:856259575263:liquid_fortress_distributed_election"
        ]
      }
    }
  ]
}""".format(queue_arn)
        print(policy_document)
        return policy_document

    def send_message(self, message):
        logging.debug("Publishing message: {}".format(message))
        result = self.topic.publish(Message=message)
        logging.debug("Publish result: {}".format(result))
        return result

    def receive_message(self):
        message_list = self.queue.receive_messages()
        if len(message_list) == 0:
            return None
        message = message_list[0]
        body = message.body
        message_id = message.message_id
        logging.debug("Received message: \'{}\' with message_id: {}".format(body, message_id))
        message.delete()
        return body
