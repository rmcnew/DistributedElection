CONTENTS = "Contents"
COORDINATOR_LOG = "coordinator.log"
DOT = "."
EDIT_DISTANCE_WORKER_SLEEP = 3  # seconds
EMPTY = ""
I_WIN = True
KEY = "Key"
LFDE_SQS_QUEUE = "liquid-fortress-distributed-election"
LFDE_S3_BUCKET = "liquid-fortress-distributed-election"
LFDE_S3_INPUT_FOLDER = "StringPairs"
LFDE_S3_INPUT_FOLDER_PREFIX = "StringPairs/StringPair-"
LFDE_S3_OUTPUT_FOLDER = "StringPairsResults"
LFDE_SNS_TOPIC = "liquid_fortress_distributed_election"
MESSAGE = "Message"
MESSAGE_ID = "MessageId"
MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod"
POLICY = "Policy"
QUEUE_ARN = "QueueArn"
RECEIVE_MESSAGE_WAIT_TIME_SECONDS = "ReceiveMessageWaitTimeSeconds"
S3 = "s3"
SHUTDOWN = "Shutdown"
SLASH = "/"
SNS = "sns"
SQS = "sqs"
THEY_WIN = False
VISIBILITY_TIMEOUT = "VisibilityTimeout"
WAIT_TIME_SECONDS = "WaitTimeSeconds"
WORK_IN_QUEUE_TIMEOUT = 2  # seconds

MESSAGE_QUEUE_ATTRIBUTES = {MESSAGE_RETENTION_PERIOD: "600",  # seconds
                            VISIBILITY_TIMEOUT: "120",  # seconds
                            RECEIVE_MESSAGE_WAIT_TIME_SECONDS: "20"}  # seconds
