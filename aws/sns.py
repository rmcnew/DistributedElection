import logging

import boto3

from shared.constants import *


class SimpleNotificationService:
    """Wrapper class for easy Simple Notification Service (SNS) usage"""

    def __init__(self):
       self.sns = boto3.resource(SNS)
       self.topic = self.sns.create_topic(Name=LFDE_SNS_TOPIC)

    def subscribe(self, sqs_arn):
       return self.topic.subscribe(Protocol=SQS, Endpoint=sqs_arn) 
