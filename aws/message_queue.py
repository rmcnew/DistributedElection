import json
import logging
import time

import boto3

from shared.constants import *


class MessageQueue:
    """Wrapper class for easy AWS Simple Queue Service (SQS) usage for use as message queue via SNS"""

    def __init__(self, queue_name):
        self.sns = boto3.resource(SNS, region_name='us-west-2')
        self.topic = self.sns.create_topic(Name=LFDE_SNS_TOPIC)
        self.topic_arn = self.topic.arn
        self.sqs = boto3.resource(SQS, region_name='us-west-2')
        self.sqs_client = boto3.client(SQS, region_name='us-west-2')
        self.queue = self.sqs.create_queue(QueueName=queue_name)
        self.queue_arn = self.queue.attributes[QUEUE_ARN]
        queue_attributes = MESSAGE_QUEUE_ATTRIBUTES
        queue_attributes[POLICY] = MessageQueue.create_write_from_sns_policy(self.queue_arn)
        # print ("queue_attributes = {}".format(queue_attributes))
        self.sqs_client.set_queue_attributes(QueueUrl=self.queue.url, Attributes=queue_attributes)
        self.subscription = self.topic.subscribe(Protocol=SQS, Endpoint=self.queue_arn)

    def shutdown(self):
        self.subscription.delete()
        time.sleep(3)
        self.queue.delete()

    def get_arn(self):
        return self.queue.attributes[QUEUE_ARN]

    @staticmethod
    def create_write_from_sns_policy(queue_arn):
        policy_document = {
            "Version": "2012-10-17",
            "Id": "MySQSDefaultPolicy",
            "Statement": [{
                "Sid": "Sid1522530097922",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "SQS:SendMessage",
                "Resource": queue_arn
            }]
        }
        json_policy_document = json.dumps(policy_document)
        # print(json_policy_document)
        return json_policy_document

    def send_message(self, message):
        logging.debug("Publishing message: {}".format(message))
        result = self.topic.publish(Message=message)
        logging.debug("Publish result: {}".format(result))
        return result

    def receive_message(self):
        messages = self.queue.receive_messages()
        if len(messages) > 0:
            message = json.loads(messages[0].body)
            message_id = messages[0].message_id
            body = message[MESSAGE]
            body_dict = json.loads(body)
            logging.debug("Received message: \'{}\' with message_id: {}".format(body_dict, message_id))
            messages[0].delete()
            return body_dict
        else:
            return None


