CONTENTS = "Contents"
COORDINATOR_LOG = "coordinator.log"
DOT = "."
EDIT_DISTANCE_WORKER_SLEEP = 3  # seconds
ELECTION_BEGIN = "election_begin"
ELECTION_COMPARE = "election_compare"
ELECTION_END = "election_end"
ELECTION_WINNER_WAIT_CYCLES = 4
EMPTY = ""
ID = "id"
KEY = "Key"
LFDE_SQS_QUEUE = "liquid-fortress-distributed-election"
LFDE_S3_BUCKET = "liquid-fortress-distributed-election"
LFDE_S3_INPUT_FOLDER = "StringPairs"
LFDE_S3_INPUT_FOLDER_PREFIX = "StringPairs/StringPair-"
LFDE_S3_OUTPUT_FOLDER = "StringPairsResults"
LFDE_SNS_TOPIC = "liquid_fortress_distributed_election"
LOSER_ID = "loser_id"
MESSAGE = "Message"
MESSAGE_ID = "MessageId"
MESSAGE_RETENTION_PERIOD = "MessageRetentionPeriod"
MESSAGE_TYPE = "message_type"
POLICY = "Policy"
QUEUE_ARN = "QueueArn"
RECEIVE_MESSAGE_WAIT_TIME_SECONDS = "ReceiveMessageWaitTimeSeconds"
S3 = "s3"
SHUTDOWN = "Shutdown"
SLASH = "/"
SNS = "sns"
SQS = "sqs"
VISIBILITY_TIMEOUT = "VisibilityTimeout"
WAIT_TIME_SECONDS = "WaitTimeSeconds"
WINNER_ID = "winner_id"
WORK_IN_QUEUE_TIMEOUT = 2  # seconds

MESSAGE_QUEUE_ATTRIBUTES = {MESSAGE_RETENTION_PERIOD: "600",  # seconds
                            VISIBILITY_TIMEOUT: "120",  # seconds
                            RECEIVE_MESSAGE_WAIT_TIME_SECONDS: "2"}  # seconds
