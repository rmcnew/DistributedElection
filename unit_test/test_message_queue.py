import json
import time
import unittest

from aws.message_queue import MessageQueue
from shared.constants import *
from shared.shared import timestamp


class TestMessageQueue(unittest.TestCase):

    def test_send_receive_to_self(self):
        # create a single message queue
        mq = MessageQueue("test_self")
        time.sleep(1)
        test_message = {MESSAGE_TYPE: "Hello this is a test message",
                        TIMESTAMP: timestamp()}
        mq.send_message(json.dumps(test_message))
        time.sleep(2)
        received_message = mq.receive_message()
        print("{} =? {}".format(test_message, received_message))
        self.assertDictEqual(test_message, received_message)
        mq.shutdown()
        time.sleep(5)


if __name__ == '__main__':
    unittest.main()
