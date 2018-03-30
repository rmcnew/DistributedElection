CONTENTS = "Contents"
KEY = "Key"
LFDE_SQS_QUEUE = "liquid-fortress-distributed-election"
LFDE_S3_BUCKET = "liquid-fortress-distributed-election"
LFDE_S3_INPUT_FOLDER = "StringPairs"
LFDE_S3_INPUT_FOLDER_PREFIX = "StringPairs/StringPair-"
LFDE_S3_OUTPUT_FOLDER = "StringPairsResults"
LFDE_SNS_TOPIC = "liquid_fortress_distributed_election"
MESSAGE_ID = "MessageId"
MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod"
QUEUE_ARN = "QueueArn"
RECEIVE_MESSAGE_WAIT_TIME_SECONDS = "ReceiveMessageWaitTimeSeconds"
S3 = "s3"
SLASH = "/"
SNS = "sns"
SQS = "sqs"
VISIBILITY_TIMEOUT = "VisibilityTimeout"
WAIT_TIME_SECONDS = "WaitTimeSeconds"

MESSAGE_QUEUE_ATTRIBUTES = {MESSAGE_RETENTION_PERIOD : 60, # seconds 
                                  VISIBILITY_TIMEOUT : 45, # seconds 
                   RECEIVE_MESSAGE_WAIT_TIME_SECONDS : 20} # seconds
